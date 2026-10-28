//! Flux Coinbase : historique REST et bougies temps reel construites depuis
//! les transactions WebSocket.
//!
//! Historique : `main` appelle `telecharger_historique(symbole, unite_temps,
//! limite, delai_minimal)`. La fonction convertit le symbole et la granularite,
//! interroge l'API REST, puis transforme les lignes avec `convertir_ligne`.
//! Elle retourne `Result<Vec<Bougie>, erreur>`.
//!
//! Temps reel : `main` appelle `ecouter_flux_temps_reel(symboles, unite_temps,
//! tx)` avec un canal `Sender<BougieSourcee>`. La fonction convertit les
//! symboles en produits Coinbase, puis relance `connecter_et_lire` apres les
//! erreurs. Cette derniere s'abonne aux transactions `matches`; `analyser_match`
//! les convertit en bougies, et `fusionner_trade` met a jour les transactions
//! d'une meme periode. Les bougies sont envoyees sur `tx`; une nouvelle periode
//! clot la precedente. Sortie : mises a jour `BougieSourcee` (fournisseur
//! Coinbase).

use crate::core::modeles::Bougie;
use crate::data::sources::{BougieSourcee, Fournisseur, duree_unite};
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const URL_REST: &str = "https://api.exchange.coinbase.com";
const URL_WEBSOCKET: &str = "wss://ws-feed.exchange.coinbase.com";
const HOTE_WEBSOCKET: &str = "ws-feed.exchange.coinbase.com:443";

/// Télécharge et convertit l'historique de bougies Coinbase pour une paire.
///
/// Entrées : symbole, unité de temps, nombre maximal de bougies et délai minimal
/// à respecter après la requête.
/// Sortie : bougies triées par ouverture, ou erreur de conversion/réseau.
pub async fn telecharger_historique(
    symbole: &str,
    unite_temps: &str,
    limite: usize,
    delai_minimal: Duration,
) -> Result<Vec<Bougie>, Box<dyn Error>> {
    let secondes = granularite_secondes(unite_temps)?;
    let produit =
        produit_coinbase(symbole).ok_or_else(|| format!("Paire Coinbase invalide : {symbole}"))?;
    let maintenant = Utc::now();
    let debut = maintenant - chrono::Duration::seconds(secondes as i64 * (limite as i64 + 2));
    let client = super::reseau::client_http()?;
    let reponse: Value = client
        .get(format!("{URL_REST}/products/{produit}/candles"))
        .query(&[
            ("granularity", secondes.to_string()),
            ("start", debut.to_rfc3339()),
            ("end", maintenant.to_rfc3339()),
        ])
        .header("Accept", "application/json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let lignes = reponse
        .as_array()
        .ok_or("Réponse Coinbase sans liste de bougies")?;
    let duree = duree_unite(unite_temps).ok_or("Unité de temps invalide")?;
    let mut bougies = lignes
        .iter()
        .map(|ligne| convertir_ligne(ligne, symbole, unite_temps, duree, maintenant))
        .collect::<Result<Vec<_>, _>>()?;
    bougies.sort_by_key(|bougie| bougie.instant_ouverture);
    if bougies.len() > limite + 1 {
        bougies.drain(..bougies.len() - (limite + 1));
    }
    tokio::time::sleep(delai_minimal).await;
    Ok(bougies)
}

/// Écoute les transactions Coinbase et les agrège en bougies par période.
///
/// Entrées : symboles, unité de temps et canal recevant les `BougieSourcee`.
/// Sortie : la fonction tourne jusqu'à la fermeture du canal; les erreurs de
/// connexion entraînent une reconnexion avec délai exponentiel.
pub async fn ecouter_flux_temps_reel(
    symboles: &[String],
    unite_temps: &str,
    tx: Sender<BougieSourcee>,
) {
    if symboles.is_empty() || duree_unite(unite_temps).is_none() {
        return;
    }
    let produits = match symboles
        .iter()
        .map(|symbole| {
            produit_coinbase(symbole)
                .ok_or_else(|| format!("Paire Coinbase invalide : {symbole}"))
                .map(|produit| (produit, symbole.to_uppercase()))
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(produits) => produits,
        Err(erreur) => {
            error!("{erreur}");
            return;
        }
    };
    let mut delai = Duration::from_secs(1);
    loop {
        match connecter_et_lire(&produits, unite_temps, &tx).await {
            Ok(()) => return,
            Err(erreur) => warn!("Flux Coinbase interrompu : {erreur}"),
        }
        sleep(delai).await;
        delai = (delai * 2).min(Duration::from_secs(60));
    }
}

/// Ouvre le WebSocket Coinbase, s'abonne aux transactions et construit les bougies.
///
/// Entrées : produits associés à leurs symboles internes, unité de temps et
/// canal de sortie.
/// Sortie : `Ok(())` si le récepteur est fermé; une erreur si la connexion ou
/// la lecture échoue.
async fn connecter_et_lire(
    produits: &[(String, String)],
    unite_temps: &str,
    tx: &Sender<BougieSourcee>,
) -> Result<(), Box<dyn Error>> {
    let (mut flux, _) = super::reseau::ouvrir_websocket(URL_WEBSOCKET, HOTE_WEBSOCKET).await?;
    info!("Flux Coinbase connecté");
    let abonnement = json!({
        "type": "subscribe",
        "product_ids": produits.iter().map(|(produit, _)| produit).collect::<Vec<_>>(),
        "channels": ["matches"]
    });
    flux.send(Message::Text(abonnement.to_string().into()))
        .await?;
    let correspondance: HashMap<&str, &str> = produits
        .iter()
        .map(|(produit, symbole)| (produit.as_str(), symbole.as_str()))
        .collect();
    let mut courantes: HashMap<String, Bougie> = HashMap::new();

    loop {
        let message = timeout(Duration::from_secs(60), flux.next())
            .await
            .map_err(|_| "aucun message Coinbase reçu en 60 secondes")?
            .ok_or("connexion Coinbase fermée")??;
        match message {
            Message::Text(texte) => {
                let transaction = match analyser_match(texte.as_str(), unite_temps, &correspondance)
                {
                    Ok(transaction) => transaction,
                    Err(erreur) => {
                        warn!("Message Coinbase ignoré : {erreur}");
                        continue;
                    }
                };
                if let Some((symbole, mut bougie)) = transaction {
                    if let Some(precedente) = courantes.get_mut(&symbole) {
                        if bougie.instant_ouverture > precedente.instant_ouverture {
                            precedente.cloturee = true;
                            if tx
                                .send(BougieSourcee {
                                    fournisseur: Fournisseur::Coinbase,
                                    bougie: precedente.clone(),
                                })
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                            courantes.insert(symbole.clone(), bougie.clone());
                        } else if bougie.instant_ouverture == precedente.instant_ouverture {
                            bougie = fusionner_trade(precedente, &bougie);
                            *precedente = bougie.clone();
                        } else {
                            continue;
                        }
                    } else {
                        courantes.insert(symbole.clone(), bougie.clone());
                    }
                    if tx
                        .send(BougieSourcee {
                            fournisseur: Fournisseur::Coinbase,
                            bougie,
                        })
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Message::Ping(donnees) => flux.send(Message::Pong(donnees)).await?,
            Message::Close(_) => return Err("fermeture demandée par Coinbase".into()),
            _ => {}
        }
    }
}

/// Analyse un message Coinbase et convertit une transaction en bougie ouverte.
///
/// Entrées : message JSON, unité de temps et correspondance produit/symbole.
/// Sortie : symbole et bougie pour une transaction suivie, `None` pour les
/// autres messages/produits, ou erreur si le message est invalide.
fn analyser_match(
    texte: &str,
    unite_temps: &str,
    correspondance: &HashMap<&str, &str>,
) -> Result<Option<(String, Bougie)>, Box<dyn Error>> {
    let message: Value = serde_json::from_str(texte)?;
    if message.get("type").and_then(Value::as_str) == Some("error") {
        return Err(format!(
            "Erreur WebSocket Coinbase : {}",
            message
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("message d'erreur sans détail")
        )
        .into());
    }
    if message.get("type").and_then(Value::as_str) != Some("match") {
        return Ok(None);
    }
    let produit = message
        .get("product_id")
        .and_then(Value::as_str)
        .ok_or("Message Coinbase sans product_id")?;
    let Some(symbole) = correspondance.get(produit).copied() else {
        return Ok(None);
    };
    let instant = DateTime::parse_from_rfc3339(
        message
            .get("time")
            .and_then(Value::as_str)
            .ok_or("Message Coinbase sans horodatage")?,
    )?
    .with_timezone(&Utc);
    let prix = decimal(&message, "price")?;
    let volume = decimal(&message, "size")?;
    let duree = duree_unite(unite_temps).ok_or("Unité de temps invalide")?;
    let secondes = duree.num_seconds();
    let ouverture = instant.timestamp().div_euclid(secondes) * secondes;
    let instant_ouverture = Utc
        .timestamp_opt(ouverture, 0)
        .single()
        .ok_or("Horodatage Coinbase hors limites")?;
    Ok(Some((
        symbole.to_owned(),
        Bougie {
            symbole: symbole.to_owned(),
            unite_temps: unite_temps.to_owned(),
            instant_ouverture,
            ouverture: prix,
            haut: prix,
            bas: prix,
            cloture: prix,
            volume,
            cloturee: false,
        },
    )))
}

/// Fusionne une transaction avec la bougie de sa période.
///
/// Entrées : bougie accumulée et bougie issue de la nouvelle transaction.
/// Sortie : bougie mise à jour avec extrêmes recalculés, dernière clôture et
/// volumes additionnés.
fn fusionner_trade(precedente: &Bougie, nouvelle: &Bougie) -> Bougie {
    Bougie {
        haut: precedente.haut.max(nouvelle.haut),
        bas: precedente.bas.min(nouvelle.bas),
        cloture: nouvelle.cloture,
        volume: precedente.volume + nouvelle.volume,
        ..precedente.clone()
    }
}

/// Convertit une ligne de l'API REST Coinbase en bougie interne.
///
/// Entrées : ligne JSON, symbole, unité de temps, durée de la période et heure
/// courante utilisée pour déterminer si la bougie est clôturée.
/// Sortie : bougie convertie ou erreur si un champ est invalide.
fn convertir_ligne(
    ligne: &Value,
    symbole: &str,
    unite_temps: &str,
    duree: chrono::Duration,
    maintenant: DateTime<Utc>,
) -> Result<Bougie, Box<dyn Error>> {
    let champs = ligne
        .as_array()
        .filter(|champs| champs.len() >= 6)
        .ok_or("Bougie REST Coinbase invalide")?;
    let secondes = champs[0].as_i64().ok_or("Horodatage Coinbase invalide")?;
    let instant_ouverture = Utc
        .timestamp_opt(secondes, 0)
        .single()
        .ok_or("Horodatage Coinbase hors limites")?;
    Ok(Bougie {
        symbole: symbole.to_uppercase(),
        unite_temps: unite_temps.to_owned(),
        instant_ouverture,
        bas: decimal_valeur(&champs[1])?,
        haut: decimal_valeur(&champs[2])?,
        ouverture: decimal_valeur(&champs[3])?,
        cloture: decimal_valeur(&champs[4])?,
        volume: decimal_valeur(&champs[5])?,
        cloturee: instant_ouverture + duree <= maintenant,
    })
}

/// Convertit une unité de temps prise en charge en granularité REST, en secondes.
///
/// Entrée : unité telle que `1m` ou `1h`.
/// Sortie : nombre de secondes ou erreur si Coinbase REST ne la prend pas en charge.
fn granularite_secondes(unite: &str) -> Result<u32, Box<dyn Error>> {
    match unite {
        "1m" => Ok(60),
        "5m" => Ok(300),
        "15m" => Ok(900),
        "1h" => Ok(3600),
        "6h" => Ok(21_600),
        "1d" => Ok(86_400),
        _ => Err(format!("unité '{unite}' non prise en charge par Coinbase REST").into()),
    }
}

/// Convertit un symbole interne en identifiant de produit Coinbase (`BASE-QUOTE`).
///
/// Entrée : symbole interne, par exemple `BTCEUR`.
/// Sortie : identifiant Coinbase ou `None` si le symbole ne peut pas être découpé.
fn produit_coinbase(symbole: &str) -> Option<String> {
    let (base, quote) = super::kraken::decouper_symbole(symbole)?;
    Some(format!("{base}-{quote}"))
}

/// Lit un champ décimal textuel dans un message Coinbase.
///
/// Entrées : objet JSON et nom du champ.
/// Sortie : valeur `f64` ou erreur si le champ est absent ou invalide.
fn decimal(message: &Value, cle: &str) -> Result<f64, Box<dyn Error>> {
    message
        .get(cle)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Champ Coinbase '{cle}' absent ou invalide"))?
        .parse()
        .map_err(|erreur| format!("Champ Coinbase '{cle}' invalide : {erreur}").into())
}

/// Convertit une valeur REST Coinbase numérique ou textuelle en `f64`.
///
/// Entrée : valeur JSON à convertir.
/// Sortie : valeur `f64` ou erreur si son format n'est pas numérique.
fn decimal_valeur(valeur: &Value) -> Result<f64, Box<dyn Error>> {
    valeur
        .as_f64()
        .or_else(|| valeur.as_str().and_then(|s| s.parse().ok()))
        .ok_or_else(|| "Valeur Coinbase invalide".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn convertit_une_transaction_en_bougie_ouverte() {
        let message = json!({
            "type": "match",
            "product_id": "BTC-EUR",
            "time": "2026-10-28T10:14:30.000Z",
            "price": "120",
            "size": "0.5"
        });
        let correspondance = HashMap::from([("BTC-EUR", "BTCEUR")]);
        let (_, bougie) = analyser_match(&message.to_string(), "1h", &correspondance)
            .unwrap()
            .unwrap();
        assert_eq!(bougie.symbole, "BTCEUR");
        assert_eq!(bougie.volume, 0.5);
        assert_eq!(bougie.instant_ouverture.timestamp(), 1_793_181_600);
        assert!(!bougie.cloturee);
    }

    #[test]
    fn convertit_les_produits_et_granularites_coinbase() {
        assert_eq!(produit_coinbase("BTCEUR").as_deref(), Some("BTC-EUR"));
        assert_eq!(granularite_secondes("1d").unwrap(), 86_400);
        assert!(granularite_secondes("2h").is_err());
    }
}
