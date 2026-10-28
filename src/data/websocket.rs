//! Flux temps réel Binance (WebSocket) avec reconnexion automatique.

use crate::core::modeles::Bougie;
use chrono::{DateTime, TimeZone, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::error::Error;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

const URL_BASE: &str = "wss://stream.binance.com:443/stream?streams=";
const HOTE_FLUX: &str = "stream.binance.com:443";
const DELAI_RECONNEXION_INITIAL: Duration = Duration::from_secs(1);
const DELAI_RECONNEXION_MAX: Duration = Duration::from_secs(60);
/// Binance envoie un ping toutes les 20 s : sans aucun message pendant ce laps de temps, la connexion est considérée morte.
const DELAI_INACTIVITE_MAX: Duration = Duration::from_secs(60);

/// Écoute en continu les bougies fermées des `symboles` sur l'unité de temps `ut_execution`
/// et les transmet à `tx`.
///
/// La fonction ne rend la main que si la liste de symboles est vide ou si le récepteur du
/// canal est fermé. Toute déconnexion ou erreur réseau déclenche une reconnexion avec un
/// délai exponentiel (1 s, 2 s, 4 s... plafonné à 60 s), remis à zéro après une connexion réussie.
pub async fn ecouter_flux_temps_reel(
    symboles: &[String],
    ut_execution: &str,
    tx: tokio::sync::mpsc::Sender<Bougie>,
) {
    if symboles.is_empty() {
        error!("Aucun symbole à écouter : flux WebSocket non démarré");
        return;
    }

    let url = construire_url(symboles, ut_execution);
    let mut delai = DELAI_RECONNEXION_INITIAL;

    loop {
        info!(url = %url, "Connexion au flux WebSocket");

        match connecter_et_lire(&url, &tx, &mut delai).await {
            Ok(()) => {
                info!("Récepteur fermé : arrêt de l'écoute du flux");
                return;
            }
            Err(erreur) => warn!(
                "Flux WebSocket interrompu : {erreur}. Reconnexion dans {} s",
                delai.as_secs()
            ),
        }

        sleep(delai).await;
        delai = (delai * 2).min(DELAI_RECONNEXION_MAX);
    }
}

/// Construit l'URL du flux combiné, ex. `.../stream?streams=btceur@kline_1h/ethusdt@kline_1h`.
fn construire_url(symboles: &[String], ut_execution: &str) -> String {
    let flux: Vec<String> = symboles
        .iter()
        .map(|symbole| format!("{}@kline_{ut_execution}", symbole.to_lowercase()))
        .collect();

    format!("{URL_BASE}{}", flux.join("/"))
}

/// Maintient une connexion jusqu'à erreur. `Ok(())` signifie que `tx` est fermé.
/// Remet `delai` à sa valeur initiale dès que la connexion est établie.
async fn connecter_et_lire(
    url: &str,
    tx: &Sender<Bougie>,
    delai: &mut Duration,
) -> Result<(), Box<dyn Error>> {
    let (flux, _) = match super::reseau::ouvrir_websocket(url, HOTE_FLUX).await {
        Ok(connexion) => connexion,
        Err(e) => {
            error!(url = %url, "Échec de connexion WebSocket : {}", e.to_string());
            return Err(e);
        }
    };
    info!("Flux WebSocket connecté");
    *delai = DELAI_RECONNEXION_INITIAL;

    let (mut ecriture, mut lecture) = flux.split();

    loop {
        let message = timeout(DELAI_INACTIVITE_MAX, lecture.next())
            .await
            .map_err(|_| "aucun message reçu avant le délai d'inactivité")?
            .ok_or("connexion fermée par le serveur")??;

        match message {
            Message::Text(texte) => {
                // L'erreur (non `Send`) doit être consommée avant tout `.await`.
                let bougie = analyser_message(texte.as_str()).unwrap_or_else(|erreur| {
                    warn!("Message ignoré : {erreur}");
                    None
                });
                if let Some(bougie) = bougie
                    && tx.send(bougie).await.is_err()
                {
                    return Ok(());
                }
            }
            Message::Ping(donnees) => ecriture.send(Message::Pong(donnees)).await?,
            Message::Close(_) => return Err("fermeture demandée par le serveur".into()),
            _ => {}
        }
    }
}

/// Analyse un message du flux combiné. Retourne `Ok(Some(bougie))` pour une bougie fermée
/// (`k.x == true`), `Ok(None)` pour tout message à ignorer (bougie en cours, autre événement).
fn analyser_message(texte: &str) -> Result<Option<Bougie>, Box<dyn Error>> {
    let message: Value = serde_json::from_str(texte)?;
    // Le flux combiné enveloppe l'événement dans `data` ; un flux simple le renvoie directement.
    let evenement = message.get("data").unwrap_or(&message);

    if evenement.get("e").and_then(Value::as_str) != Some("kline") {
        return Ok(None);
    }

    let k = evenement.get("k").ok_or("événement kline sans champ 'k'")?;

    let cloturee = k.get("x").and_then(Value::as_bool).unwrap_or(false);

    Ok(Some(Bougie {
        symbole: lire_texte(k, "s")?.to_owned(),
        unite_temps: lire_texte(k, "i")?.to_owned(),
        instant_ouverture: lire_instant(k, "t")?,
        ouverture: lire_decimal(k, "o")?,
        haut: lire_decimal(k, "h")?,
        bas: lire_decimal(k, "l")?,
        cloture: lire_decimal(k, "c")?,
        volume: lire_decimal(k, "v")?,
        cloturee,
    }))
}

fn lire_texte<'a>(k: &'a Value, cle: &str) -> Result<&'a str, Box<dyn Error>> {
    k.get(cle)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("champ '{cle}' absent ou invalide").into())
}

fn lire_decimal(k: &Value, cle: &str) -> Result<f64, Box<dyn Error>> {
    lire_texte(k, cle)?
        .parse::<f64>()
        .map_err(|erreur| format!("champ '{cle}' invalide : {erreur}").into())
}

fn lire_instant(k: &Value, cle: &str) -> Result<DateTime<Utc>, Box<dyn Error>> {
    let millisecondes = k
        .get(cle)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("champ '{cle}' absent ou invalide"))?;

    Utc.timestamp_millis_opt(millisecondes)
        .single()
        .ok_or_else(|| format!("champ '{cle}' : horodatage hors limites").into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message_kline(fermee: bool) -> String {
        format!(
            r#"{{"stream":"btceur@kline_1h","data":{{"e":"kline","E":1700003600000,"s":"BTCEUR",
            "k":{{"t":1700000000000,"T":1700003599999,"s":"BTCEUR","i":"1h","o":"100.5","c":"105.75",
            "h":"110.0","l":"95.25","v":"1234.5","x":{fermee}}}}}}}"#
        )
    }

    #[test]
    fn construit_l_url_du_flux_combine() {
        let symboles = vec!["BTCEUR".to_string(), "ETHUSDT".to_string()];

        assert_eq!(
            construire_url(&symboles, "1h"),
            "wss://stream.binance.com:443/stream?streams=btceur@kline_1h/ethusdt@kline_1h"
        );
    }

    #[test]
    fn convertit_une_bougie_fermee() {
        let bougie = analyser_message(&message_kline(true)).unwrap().unwrap();

        assert_eq!(bougie.symbole, "BTCEUR");
        assert_eq!(bougie.unite_temps, "1h");
        assert_eq!(
            bougie.instant_ouverture.timestamp_millis(),
            1_700_000_000_000
        );
        assert_eq!(bougie.ouverture, 100.5);
        assert_eq!(bougie.haut, 110.0);
        assert_eq!(bougie.bas, 95.25);
        assert_eq!(bougie.cloture, 105.75);
        assert_eq!(bougie.volume, 1234.5);
        assert!(bougie.cloturee);
    }

    #[test]
    fn transmet_les_bougies_en_cours_et_ignore_les_autres_messages() {
        let bougie = analyser_message(&message_kline(false)).unwrap().unwrap();
        assert!(!bougie.cloturee);
        assert!(
            analyser_message(r#"{"result":null,"id":1}"#)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejette_les_messages_invalides() {
        assert!(analyser_message("pas du json").is_err());
        assert!(analyser_message(r#"{"e":"kline","k":{"x":true,"s":"X"}}"#).is_err());
    }

    #[tokio::test]
    async fn rend_la_main_sans_symbole() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);

        ecouter_flux_temps_reel(&[], "1h", tx).await;
    }
}
