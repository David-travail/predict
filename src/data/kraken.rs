use crate::core::modeles::Bougie;
use crate::data::sources::{BougieSourcee, Fournisseur, duree_unite};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const URL_REST: &str = "https://api.kraken.com/0/public/OHLC";
const URL_WEBSOCKET: &str = "wss://ws.kraken.com/v2";
const HOTE_WEBSOCKET: &str = "ws.kraken.com:443";

pub async fn telecharger_historique(
    symbole: &str,
    unite_temps: &str,
    limite: usize,
    delai_minimal: Duration,
) -> Result<Vec<Bougie>, Box<dyn Error>> {
    let intervalle = intervalle_minutes(unite_temps)?;
    let paire =
        paire_kraken(symbole).ok_or_else(|| format!("Paire Kraken invalide : {symbole}"))?;
    let client = super::reseau::client_http()?;
    let reponse: Value = client
        .get(URL_REST)
        .query(&[("pair", paire), ("interval", intervalle.to_string())])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if let Some(erreurs) = reponse.get("error").and_then(Value::as_array)
        && !erreurs.is_empty()
    {
        return Err(format!("Erreur REST Kraken : {erreurs:?}").into());
    }

    let resultat = reponse
        .get("result")
        .and_then(Value::as_object)
        .ok_or("Réponse Kraken sans objet 'result'")?;
    let lignes = resultat
        .iter()
        .find(|(cle, _)| cle.as_str() != "last")
        .and_then(|(_, valeur)| valeur.as_array())
        .ok_or("Réponse Kraken sans liste de bougies")?;
    let maintenant = Utc::now();
    let mut bougies = lignes
        .iter()
        .map(|ligne| convertir_ligne_rest(ligne, symbole, unite_temps, maintenant))
        .collect::<Result<Vec<_>, _>>()?;
    bougies.sort_by_key(|bougie| bougie.instant_ouverture);
    if bougies.len() > limite + 1 {
        bougies.drain(..bougies.len() - (limite + 1));
    }
    tokio::time::sleep(delai_minimal).await;
    Ok(bougies)
}

pub async fn ecouter_flux_temps_reel(
    symboles: &[String],
    unite_temps: &str,
    tx: Sender<BougieSourcee>,
) {
    if symboles.is_empty() {
        return;
    }
    let intervalle = match intervalle_minutes(unite_temps) {
        Ok(intervalle) => intervalle,
        Err(erreur) => {
            error!("Unité de temps non prise en charge par Kraken : {erreur}");
            return;
        }
    };
    let paires = match symboles
        .iter()
        .map(|symbole| {
            paire_websocket(symbole).ok_or_else(|| format!("Paire Kraken invalide : {symbole}"))
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(paires) => paires,
        Err(erreur) => {
            error!("{erreur}");
            return;
        }
    };

    let mut delai = Duration::from_secs(1);
    loop {
        match connecter_et_lire(&paires, intervalle, unite_temps, &tx).await {
            Ok(()) => return,
            Err(erreur) => warn!("Flux Kraken interrompu : {erreur}"),
        }
        sleep(delai).await;
        delai = (delai * 2).min(Duration::from_secs(60));
    }
}

async fn connecter_et_lire(
    paires: &[String],
    intervalle: u32,
    unite_temps: &str,
    tx: &Sender<BougieSourcee>,
) -> Result<(), Box<dyn Error>> {
    let (mut flux, _) = super::reseau::ouvrir_websocket(URL_WEBSOCKET, HOTE_WEBSOCKET).await?;
    info!("Flux Kraken connecté");
    let abonnement = json!({
        "method": "subscribe",
        "params": {
            "channel": "ohlc",
            "symbol": paires,
            "interval": intervalle,
            "snapshot": true
        }
    });
    flux.send(Message::Text(abonnement.to_string().into()))
        .await?;
    let mut courantes: HashMap<String, Bougie> = HashMap::new();

    loop {
        let message = timeout(Duration::from_secs(60), flux.next())
            .await
            .map_err(|_| "aucun message Kraken reçu en 60 secondes")?
            .ok_or("connexion Kraken fermée")??;
        match message {
            Message::Text(texte) => {
                let bougies = match analyser_messages(texte.as_str(), unite_temps) {
                    Ok(bougies) => bougies,
                    Err(erreur) => {
                        warn!("Message Kraken ignoré : {erreur}");
                        continue;
                    }
                };
                for (symbole, bougie) in bougies {
                    if let Some(precedente) = courantes.get_mut(&symbole) {
                        if bougie.instant_ouverture > precedente.instant_ouverture {
                            precedente.cloturee = true;
                            if tx
                                .send(BougieSourcee {
                                    fournisseur: Fournisseur::Kraken,
                                    bougie: precedente.clone(),
                                })
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                            courantes.insert(symbole.clone(), bougie.clone());
                        } else if bougie.instant_ouverture == precedente.instant_ouverture {
                            *precedente = bougie.clone();
                        }
                    } else {
                        courantes.insert(symbole.clone(), bougie.clone());
                    }
                    if tx
                        .send(BougieSourcee {
                            fournisseur: Fournisseur::Kraken,
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
            Message::Close(_) => return Err("fermeture demandée par Kraken".into()),
            _ => {}
        }
    }
}

fn analyser_messages(
    texte: &str,
    unite_temps: &str,
) -> Result<Vec<(String, Bougie)>, Box<dyn Error>> {
    let message: Value = serde_json::from_str(texte)?;
    if message.get("success").and_then(Value::as_bool) == Some(false) {
        return Err(format!(
            "Abonnement WebSocket Kraken refusé : {}",
            message
                .get("error")
                .and_then(Value::as_str)
                .unwrap_or("erreur sans détail")
        )
        .into());
    }
    if message.get("channel").and_then(Value::as_str) != Some("ohlc") {
        return Ok(Vec::new());
    }
    let donnees = message
        .get("data")
        .and_then(Value::as_array)
        .ok_or("Message OHLC Kraken sans tableau 'data'")?;
    donnees
        .iter()
        .map(|donnee| {
            let paire_fournisseur = lire_texte(donnee, "symbol")?;
            let symbole = paire_interne(paire_fournisseur)
                .ok_or_else(|| format!("Symbole Kraken non reconnu : {paire_fournisseur}"))?;
            let instant = DateTime::parse_from_rfc3339(lire_texte(donnee, "interval_begin")?)?
                .with_timezone(&Utc);
            let bougie = Bougie {
                symbole: symbole.clone(),
                unite_temps: unite_temps.to_owned(),
                instant_ouverture: instant,
                ouverture: decimal(donnee, "open")?,
                haut: decimal(donnee, "high")?,
                bas: decimal(donnee, "low")?,
                cloture: decimal(donnee, "close")?,
                volume: decimal(donnee, "volume")?,
                cloturee: false,
            };
            Ok((symbole, bougie))
        })
        .collect()
}

fn convertir_ligne_rest(
    ligne: &Value,
    symbole: &str,
    unite_temps: &str,
    maintenant: DateTime<Utc>,
) -> Result<Bougie, Box<dyn Error>> {
    let champs = ligne
        .as_array()
        .filter(|champs| champs.len() >= 8)
        .ok_or("Bougie REST Kraken invalide")?;
    let secondes = champs[0].as_f64().ok_or("Horodatage Kraken invalide")?;
    let ouverture_ms = (secondes * 1000.0) as i64;
    let instant_ouverture = chrono::TimeZone::timestamp_millis_opt(&Utc, ouverture_ms)
        .single()
        .ok_or("Horodatage Kraken hors limites")?;
    let duree = duree_unite(unite_temps).ok_or("Unité de temps invalide")?;
    let cloturee = instant_ouverture + duree <= maintenant;
    Ok(Bougie {
        symbole: symbole.to_uppercase(),
        unite_temps: unite_temps.to_owned(),
        instant_ouverture,
        ouverture: decimal_valeur(&champs[1])?,
        haut: decimal_valeur(&champs[2])?,
        bas: decimal_valeur(&champs[3])?,
        cloture: decimal_valeur(&champs[4])?,
        volume: decimal_valeur(&champs[6])?,
        cloturee,
    })
}

fn intervalle_minutes(unite: &str) -> Result<u32, Box<dyn Error>> {
    let minutes = match unite {
        "1m" => 1,
        "5m" => 5,
        "15m" => 15,
        "30m" => 30,
        "1h" => 60,
        "4h" => 240,
        "1d" => 1440,
        "1w" => 10_080,
        _ => return Err(format!("unité '{unite}' non prise en charge").into()),
    };
    Ok(minutes)
}

fn paire_kraken(symbole: &str) -> Option<String> {
    let (base, quote) = decouper_symbole(symbole)?;
    let base = if base == "BTC" {
        "XBT".to_owned()
    } else {
        base
    };
    Some(format!("{base}{quote}"))
}

fn paire_websocket(symbole: &str) -> Option<String> {
    let (base, quote) = decouper_symbole(symbole)?;
    Some(format!("{base}/{quote}"))
}

fn paire_interne(paire: &str) -> Option<String> {
    let (base, quote) = paire.split_once('/')?;
    let base = if base == "XBT" { "BTC" } else { base };
    Some(format!("{base}{quote}"))
}

pub fn decouper_symbole(symbole: &str) -> Option<(String, String)> {
    let symbole = symbole.trim().to_uppercase();
    for quote in ["USDT", "USDC", "EUR", "USD", "GBP", "BTC"] {
        if let Some(base) = symbole.strip_suffix(quote)
            && !base.is_empty()
        {
            return Some((base.to_owned(), quote.to_owned()));
        }
    }
    None
}

fn lire_texte<'a>(valeur: &'a Value, cle: &str) -> Result<&'a str, Box<dyn Error>> {
    valeur
        .get(cle)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("Champ Kraken '{cle}' absent ou invalide").into())
}

fn decimal(valeur: &Value, cle: &str) -> Result<f64, Box<dyn Error>> {
    lire_texte(valeur, cle)?
        .parse()
        .map_err(|erreur| format!("Champ Kraken '{cle}' invalide : {erreur}").into())
}

fn decimal_valeur(valeur: &Value) -> Result<f64, Box<dyn Error>> {
    valeur
        .as_str()
        .ok_or("Valeur Kraken non textuelle")?
        .parse()
        .map_err(|erreur| format!("Valeur Kraken invalide : {erreur}").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn convertit_le_message_ohlc_kraken() {
        let message = json!({
            "channel": "ohlc",
            "type": "update",
            "data": [{
                "symbol": "BTC/EUR",
                "interval_begin": "2026-10-28T09:00:00.000000Z",
                "open": "100",
                "high": "110",
                "low": "95",
                "close": "105",
                "volume": "12.5"
            }]
        });
        let resultat = analyser_messages(&message.to_string(), "1h").unwrap();
        assert_eq!(resultat[0].0, "BTCEUR");
        assert_eq!(resultat[0].1.volume, 12.5);
    }

    #[test]
    fn convertit_les_symboles_kraken_et_les_intervalles() {
        assert_eq!(paire_kraken("BTCEUR").as_deref(), Some("XBTEUR"));
        assert_eq!(paire_websocket("BTCEUR").as_deref(), Some("BTC/EUR"));
        assert_eq!(intervalle_minutes("1d").unwrap(), 1440);
        assert!(intervalle_minutes("2h").is_err());
    }
}
