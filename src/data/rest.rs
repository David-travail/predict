//! Client REST pour récupérer l'historique de bougies sur l'API publique de Binance.

use crate::core::modeles::Bougie;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use std::error::Error;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

const URL_KLINES: &str = "https://api.binance.com/api/v3/klines";
const LIMITE_MAX_BINANCE: usize = 1000;

/// Instant de la dernière requête, partagé pour espacer les appels (rate limit).
static DERNIERE_REQUETE: Mutex<Option<Instant>> = Mutex::const_new(None);

/// Télécharge jusqu'à `limite` bougies, éventuellement bornées par `debut_ms` / `fin_ms`
/// (millisecondes Unix, bornes incluses). Sans `debut_ms`, Binance renvoie les plus récentes
/// avant `fin_ms`.
pub async fn telecharger_bougies_periode(
    symbole: &str,
    unite_temps: &str,
    debut_ms: Option<i64>,
    fin_ms: Option<i64>,
    limite: usize,
    delai_minimal: Option<Duration>,
) -> Result<Vec<Bougie>, Box<dyn Error>> {
    if limite == 0 || limite > LIMITE_MAX_BINANCE {
        return Err(format!("La limite doit être comprise entre 1 et {LIMITE_MAX_BINANCE}").into());
    }

    attendre_delai_minimal(delai_minimal).await;

    let mut parametres = vec![
        ("symbol", symbole.to_owned()),
        ("interval", unite_temps.to_owned()),
        ("limit", limite.to_string()),
    ];
    if let Some(debut) = debut_ms {
        parametres.push(("startTime", debut.to_string()));
    }
    if let Some(fin) = fin_ms {
        parametres.push(("endTime", fin.to_string()));
    }

    let reponse = super::reseau::client_http()?
        .get(URL_KLINES)
        .query(&parametres)
        .send()
        .await?
        .error_for_status()?;

    let donnees: Value = reponse.json().await?;
    convertir_bougies(&donnees, symbole, unite_temps, Utc::now())
}

/// Télécharge toutes les bougies de `[debut_ms, fin_ms]` en paginant par lots de 1000.
pub async fn telecharger_plage_bougies(
    symbole: &str,
    unite_temps: &str,
    debut_ms: i64,
    fin_ms: i64,
    delai_minimal: Option<Duration>,
) -> Result<Vec<Bougie>, Box<dyn Error>> {
    let mut bougies = Vec::new();
    let mut curseur = debut_ms;

    while curseur <= fin_ms {
        let lot = telecharger_bougies_periode(
            symbole,
            unite_temps,
            Some(curseur),
            Some(fin_ms),
            LIMITE_MAX_BINANCE,
            delai_minimal,
        )
        .await?;

        let Some(derniere) = lot.last() else { break };
        curseur = derniere.instant_ouverture.timestamp_millis() + 1;
        let complet = lot.len() == LIMITE_MAX_BINANCE;
        bougies.extend(lot);
        if !complet {
            break;
        }
    }

    Ok(bougies)
}

/// Bloque jusqu'à ce que `delai_minimal` se soit écoulé depuis la requête précédente.
/// Le verrou reste pris pendant l'attente pour sérialiser les appels concurrents.
async fn attendre_delai_minimal(delai_minimal: Option<Duration>) {
    let mut derniere = DERNIERE_REQUETE.lock().await;

    if let (Some(delai), Some(instant)) = (delai_minimal, *derniere) {
        let ecoule = instant.elapsed();
        if ecoule < delai {
            sleep(delai - ecoule).await;
        }
    }

    *derniere = Some(Instant::now());
}

/// Convertit la réponse JSON de Binance (tableau de tableaux) en bougies.
///
/// Format d'une entrée : `[ouverture_ms, "open", "high", "low", "close", "volume",
/// cloture_ms, ...]`. Une bougie est considérée close si son instant de clôture est passé.
fn convertir_bougies(
    donnees: &Value,
    symbole: &str,
    unite_temps: &str,
    maintenant: DateTime<Utc>,
) -> Result<Vec<Bougie>, Box<dyn Error>> {
    let lignes = donnees
        .as_array()
        .ok_or("Réponse inattendue de Binance : un tableau est attendu")?;

    lignes
        .iter()
        .map(|ligne| {
            let champs = ligne
                .as_array()
                .filter(|champs| champs.len() >= 7)
                .ok_or("Bougie invalide : au moins 7 champs attendus")?;

            let instant_ouverture = lire_instant(&champs[0], "heure d'ouverture")?;
            let instant_cloture = lire_instant(&champs[6], "heure de clôture")?;

            Ok(Bougie {
                symbole: symbole.to_owned(),
                unite_temps: unite_temps.to_owned(),
                instant_ouverture,
                ouverture: lire_decimal(&champs[1], "ouverture")?,
                haut: lire_decimal(&champs[2], "haut")?,
                bas: lire_decimal(&champs[3], "bas")?,
                cloture: lire_decimal(&champs[4], "clôture")?,
                volume: lire_decimal(&champs[5], "volume")?,
                cloturee: instant_cloture < maintenant,
            })
        })
        .collect()
}

/// Lit un nombre transmis sous forme de chaîne (ex. `"43250.01"`).
fn lire_decimal(valeur: &Value, nom: &str) -> Result<f64, Box<dyn Error>> {
    valeur
        .as_str()
        .ok_or_else(|| format!("Champ '{nom}' : une chaîne est attendue"))?
        .parse::<f64>()
        .map_err(|erreur| format!("Champ '{nom}' invalide : {erreur}").into())
}

/// Lit un horodatage en millisecondes depuis l'époque Unix.
fn lire_instant(valeur: &Value, nom: &str) -> Result<DateTime<Utc>, Box<dyn Error>> {
    let millisecondes = valeur
        .as_i64()
        .ok_or_else(|| format!("Champ '{nom}' : un entier est attendu"))?;

    Utc.timestamp_millis_opt(millisecondes)
        .single()
        .ok_or_else(|| format!("Champ '{nom}' : horodatage hors limites").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn maintenant() -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_100_000).unwrap()
    }

    #[test]
    fn convertit_la_reponse_binance_en_bougies() {
        let donnees = json!([
            [
                1_700_000_000_000_i64,
                "100.5",
                "110.0",
                "95.25",
                "105.75",
                "1234.5",
                1_700_000_059_999_i64,
                "0",
                10,
                "0",
                "0",
                "0"
            ],
            [
                1_700_000_060_000_i64,
                "105.75",
                "112.0",
                "104.0",
                "111.0",
                "800",
                1_700_000_119_999_i64,
                "0",
                8,
                "0",
                "0",
                "0"
            ]
        ]);

        let bougies = convertir_bougies(&donnees, "BTCUSDT", "1m", maintenant()).unwrap();

        assert_eq!(bougies.len(), 2);
        assert_eq!(bougies[0].symbole, "BTCUSDT");
        assert_eq!(bougies[0].unite_temps, "1m");
        assert_eq!(
            bougies[0].instant_ouverture.timestamp_millis(),
            1_700_000_000_000
        );
        assert_eq!(bougies[0].ouverture, 100.5);
        assert_eq!(bougies[0].haut, 110.0);
        assert_eq!(bougies[0].bas, 95.25);
        assert_eq!(bougies[0].cloture, 105.75);
        assert_eq!(bougies[0].volume, 1234.5);
        assert!(bougies[0].cloturee);
        // La seconde bougie se termine après `maintenant` : elle est encore ouverte.
        assert!(!bougies[1].cloturee);
    }

    #[test]
    fn rejette_les_reponses_invalides() {
        assert!(convertir_bougies(&json!({"code": -1121}), "X", "1m", maintenant()).is_err());
        assert!(convertir_bougies(&json!([[1, "1"]]), "X", "1m", maintenant()).is_err());
        assert!(
            convertir_bougies(
                &json!([[1, "abc", "1", "1", "1", "1", 2]]),
                "X",
                "1m",
                maintenant()
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn rejette_une_limite_invalide() {
        assert!(
            telecharger_bougies_periode("BTCUSDT", "1m", None, None, 0, None)
                .await
                .is_err()
        );
        assert!(
            telecharger_bougies_periode("BTCUSDT", "1m", None, None, 1001, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn respecte_le_delai_minimal_entre_deux_appels() {
        let delai = Duration::from_millis(100);
        attendre_delai_minimal(Some(delai)).await;

        let debut = Instant::now();
        attendre_delai_minimal(Some(delai)).await;

        assert!(debut.elapsed() >= Duration::from_millis(90));
    }
}
