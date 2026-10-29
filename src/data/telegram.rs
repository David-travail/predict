use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::error;

const CAPACITE_FILE: usize = 32;
const LONGUEUR_MAX_EXTRAIT: usize = 500;

#[derive(Clone)]
pub struct ConfigurationTelegram {
    jeton_bot: String,
    identifiant_chat: String,
}

impl ConfigurationTelegram {
    pub fn depuis_identifiants(jeton_bot: String, identifiant_chat: String) -> Self {
        Self {
            jeton_bot,
            identifiant_chat,
        }
    }

    /// Démarre la file d'envoi Telegram, traitée indépendamment du moteur.
    ///
    /// Sortie : expéditeur borné des notifications du moteur.
    pub fn demarrer(self) -> NotificateurTelegram {
        let (tx, mut rx) = mpsc::channel::<String>(CAPACITE_FILE);
        tokio::spawn(async move {
            let client = match crate::data::reseau::client_http() {
                Ok(client) => client,
                Err(_) => {
                    error!("Impossible de créer le client HTTP pour Telegram.");
                    return;
                }
            };

            while let Some(message) = rx.recv().await {
                if let Err(erreur) = envoyer_message(&client, &self, &message).await {
                    error!("Échec d'envoi de notification Telegram : {erreur}");
                }
            }
        });
        NotificateurTelegram { tx }
    }
}

#[derive(Clone)]
pub struct NotificateurTelegram {
    tx: mpsc::Sender<String>,
}

impl NotificateurTelegram {
    /// Ajoute un message à la file sans attendre la réponse du service Telegram.
    ///
    /// Entrée : texte à transmettre.
    /// Sortie : erreur si la file est pleine ou si le service d'envoi est arrêté.
    pub fn notifier(&self, message: String) -> Result<(), String> {
        self.tx.try_send(message).map_err(|erreur| match erreur {
            mpsc::error::TrySendError::Full(_) => {
                "La file des notifications Telegram est pleine.".into()
            }
            mpsc::error::TrySendError::Closed(_) => {
                "Le service d'envoi Telegram est arrêté.".into()
            }
        })
    }
}

#[derive(Serialize)]
struct RequeteMessage<'a> {
    chat_id: &'a str,
    text: &'a str,
}

#[derive(Deserialize)]
struct ReponseTelegram {
    ok: bool,
    description: Option<String>,
}

async fn envoyer_message(
    client: &Client,
    configuration: &ConfigurationTelegram,
    message: &str,
) -> Result<(), String> {
    let url = format!(
        "https://api.telegram.org/bot{}/sendMessage",
        configuration.jeton_bot
    );
    let reponse = client
        .post(url)
        .json(&RequeteMessage {
            chat_id: &configuration.identifiant_chat,
            text: message,
        })
        .send()
        .await
        .map_err(|_| "Impossible de joindre l'API Telegram.".to_string())?;
    let statut = reponse.status();
    let corps = reponse
        .text()
        .await
        .map_err(|_| "Impossible de lire la réponse de l'API Telegram.".to_string())?;
    let resultat: ReponseTelegram = serde_json::from_str(&corps).map_err(|_| {
        let extrait = extrait_reponse(&corps, &configuration.jeton_bot);
        format!("Réponse HTTP {statut} non conforme à l'API Telegram; contenu reçu : {extrait}")
    })?;

    if !statut.is_success() || !resultat.ok {
        let description = resultat
            .description
            .unwrap_or_else(|| "réponse Telegram avec ok=false".to_string());
        let description = extrait_reponse(&description, &configuration.jeton_bot);
        return Err(format!("Réponse Telegram HTTP {statut} : {description}"));
    }
    Ok(())
}

fn extrait_reponse(corps: &str, jeton_bot: &str) -> String {
    let corps = if jeton_bot.is_empty() {
        corps.to_owned()
    } else {
        corps.replace(jeton_bot, "[jeton masqué]")
    };
    let mut nettoye = String::new();
    let mut caracteres = corps.chars().peekable();
    while let Some(caractere) = caracteres.next() {
        if caractere == '\u{1b}' {
            if caracteres.peek() == Some(&'[') {
                caracteres.next();
                for caractere in caracteres.by_ref() {
                    if ('@'..='~').contains(&caractere) {
                        break;
                    }
                }
            } else {
                caracteres.next();
            }
        } else if caractere.is_control() {
            nettoye.push(' ');
        } else {
            nettoye.push(caractere);
        }
    }

    let compact = nettoye.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut extrait: String = compact.chars().take(LONGUEUR_MAX_EXTRAIT).collect();
    if compact.chars().count() > LONGUEUR_MAX_EXTRAIT {
        extrait.push_str("...");
    }
    if extrait.is_empty() {
        "[réponse vide]".into()
    } else {
        extrait
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nettoie_et_masque_le_corps_de_reponse() {
        let corps = "\u{1b}[31mproxy refuse le jeton-secret\u{1b}[0m\nHTML";
        assert_eq!(
            extrait_reponse(corps, "jeton-secret"),
            "proxy refuse le [jeton masqué] HTML"
        );
    }

    #[test]
    fn limite_la_taille_de_l_extrait() {
        let extrait = extrait_reponse(&"x".repeat(LONGUEUR_MAX_EXTRAIT + 10), "token");
        assert_eq!(extrait.chars().count(), LONGUEUR_MAX_EXTRAIT + 3);
        assert!(extrait.ends_with("..."));
    }
}
