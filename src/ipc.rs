use crate::config::Parametrages;
use crate::data::sources::Fournisseur;
use crate::ui::{CommandeMoteur, ModeExecution, NotificationMoteur, SignalVisuel};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{RwLock, Semaphore, broadcast, mpsc};
use tokio::time::{Duration, timeout};
use tracing::{info, warn};

const CAPACITE_DIFFUSION: usize = 1024;
const DELAI_AUTHENTIFICATION: Duration = Duration::from_secs(10);
const TAILLE_MAX_MESSAGE: usize = 1024 * 1024;
const CLIENTS_MAX: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtatServeur {
    pub symboles: Vec<String>,
    pub parametrages: Parametrages,
    pub fournisseurs: Vec<Fournisseur>,
    pub generation: u64,
    pub mode: ModeExecution,
    pub statut: Option<(u64, NotificationMoteur)>,
    pub signaux: Vec<SignalVisuel>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "contenu", rename_all = "snake_case")]
enum RequeteClient {
    Authentifier { jeton: String },
    Commande(CommandeMoteur),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "contenu", rename_all = "snake_case")]
enum MessageServeur {
    Connecte(EtatServeur),
    Evenement(EvenementServeur),
    Erreur(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "contenu", rename_all = "snake_case")]
enum EvenementServeur {
    Signal(SignalVisuel),
    Statut {
        generation: u64,
        notification: NotificationMoteur,
    },
}

pub struct ConnexionUi {
    pub etat: EtatServeur,
    pub rx_signaux: mpsc::Receiver<SignalVisuel>,
    pub rx_statut: mpsc::UnboundedReceiver<(u64, NotificationMoteur)>,
    pub tx_commandes: mpsc::UnboundedSender<CommandeMoteur>,
}

/// Connecte l'IHM au moteur, l'authentifie et crée les canaux d'échange.
///
/// Entrées : adresse TCP du moteur et jeton d'authentification.
/// Sortie : état initial et canaux de signaux, statuts et commandes, ou erreur réseau/protocole.
pub async fn connecter(
    adresse: &str,
    jeton: String,
) -> Result<ConnexionUi, Box<dyn Error + Send + Sync>> {
    let stream = TcpStream::connect(adresse).await?;
    let (lecture, ecriture) = stream.into_split();
    let mut lecteur = BufReader::new(lecture);
    let mut ecrivain = BufWriter::new(ecriture);
    ecrire_message(&mut ecrivain, &RequeteClient::Authentifier { jeton }).await?;

    let mut ligne = String::new();
    timeout(
        DELAI_AUTHENTIFICATION,
        lire_ligne_limitee(&mut lecteur, &mut ligne),
    )
    .await??;
    let reponse: MessageServeur = serde_json::from_str(&ligne)?;
    let MessageServeur::Connecte(etat) = reponse else {
        return match reponse {
            MessageServeur::Erreur(message) => Err(message.into()),
            _ => Err("Réponse inattendue du moteur".into()),
        };
    };

    let (tx_signaux, rx_signaux) = mpsc::channel(256);
    let (tx_statut, rx_statut) = mpsc::unbounded_channel();
    let (tx_commandes, mut rx_commandes) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut ligne = String::new();
        loop {
            tokio::select! {
                resultat = lire_ligne_limitee(&mut lecteur, &mut ligne) => {
                    match resultat {
                        Ok(0) => {
                            let _ = tx_statut.send((
                                etat.generation,
                                NotificationMoteur::Echec("Connexion au moteur perdue.".to_string()),
                            ));
                            break;
                        }
                        Ok(_) => {
                            let message = match serde_json::from_str::<MessageServeur>(&ligne) {
                                Ok(message) => message,
                                Err(erreur) => {
                                    let _ = tx_statut.send((
                                        etat.generation,
                                        NotificationMoteur::Echec(format!(
                                            "Message reçu du moteur invalide : {erreur}"
                                        )),
                                    ));
                                    break;
                                }
                            };
                            match message {
                                MessageServeur::Evenement(EvenementServeur::Signal(signal)) => {
                                    if tx_signaux.send(signal).await.is_err() {
                                        break;
                                    }
                                }
                                MessageServeur::Evenement(EvenementServeur::Statut {
                                    generation,
                                    notification,
                                }) => {
                                    if tx_statut.send((generation, notification)).is_err() {
                                        break;
                                    }
                                }
                                MessageServeur::Erreur(message) => {
                                    let _ = tx_statut.send((
                                        etat.generation,
                                        NotificationMoteur::Echec(message),
                                    ));
                                    break;
                                }
                                MessageServeur::Connecte(_) => {
                                    let _ = tx_statut.send((
                                        etat.generation,
                                        NotificationMoteur::Echec(
                                            "Message de connexion inattendu reçu du moteur"
                                                .to_string(),
                                        ),
                                    ));
                                    break;
                                }
                            }
                            ligne.clear();
                        }
                        Err(erreur) => {
                            let _ = tx_statut.send((
                                etat.generation,
                                NotificationMoteur::Echec(format!(
                                    "Erreur de lecture du moteur : {erreur}"
                                )),
                            ));
                            break;
                        }
                    }
                }
                commande = rx_commandes.recv() => {
                    let Some(commande) = commande else { break };
                    if let Err(erreur) = ecrire_message(
                        &mut ecrivain,
                        &RequeteClient::Commande(commande),
                    ).await {
                        let _ = tx_statut.send((
                            etat.generation,
                            NotificationMoteur::Echec(format!(
                                "Erreur d'envoi au moteur : {erreur}"
                            )),
                        ));
                        break;
                    }
                }
            }
        }
    });

    Ok(ConnexionUi {
        etat,
        rx_signaux,
        rx_statut,
        tx_commandes,
    })
}

/// Sert de relais TCP entre les clients IHM et le moteur.
///
/// Entrées : listener, jeton, état initial, canal des commandes moteur, chemin de sauvegarde
/// et récepteurs des signaux/statuts.
/// Sortie : tourne jusqu'à Ctrl+C ou une erreur de listener/canal.
pub async fn servir(
    listener: TcpListener,
    jeton: String,
    etat_initial: EtatServeur,
    tx_moteur: mpsc::UnboundedSender<CommandeMoteur>,
    chemin_parametrages: PathBuf,
    mut rx_signaux: mpsc::Receiver<SignalVisuel>,
    mut rx_statut: mpsc::UnboundedReceiver<(u64, NotificationMoteur)>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let etat = Arc::new(RwLock::new(etat_initial));
    let (tx_evenements, _) = broadcast::channel(CAPACITE_DIFFUSION);
    let (tx_commandes, mut rx_commandes) = mpsc::unbounded_channel();
    let places_clients = Arc::new(Semaphore::new(CLIENTS_MAX));
    let jeton = Arc::<str>::from(jeton);

    info!("Serveur prêt : {}", listener.local_addr()?);
    loop {
        tokio::select! {
            connexion = listener.accept() => {
                let (stream, adresse) = connexion?;
                let Ok(permit) = Arc::clone(&places_clients).try_acquire_owned() else {
                    warn!("Connexion refusée : limite de clients atteinte ({CLIENTS_MAX})");
                    drop(stream);
                    continue;
                };
                info!("Connexion entrante : {adresse}");
                let jeton_client = Arc::clone(&jeton);
                let evenements_client = tx_evenements.clone();
                let etat_client = Arc::clone(&etat);
                let commandes_client = tx_commandes.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    gerer_client(
                        stream,
                        jeton_client,
                        evenements_client,
                        etat_client,
                        commandes_client,
                    )
                    .await;
                });
            }
            signal = rx_signaux.recv() => {
                let Some(signal) = signal else {
                    return Err("Le canal de signaux du moteur est fermé".into());
                };
                let mode_courant = {
                    let mut etat = etat.write().await;
                    if signal.generation == etat.generation {
                        etat.signaux.retain(|ancien| ancien.symbole != signal.symbole);
                        etat.signaux.push(signal.clone());
                        Some(etat.mode)
                    } else {
                        None
                    }
                };
                if signal.est_alerte
                    && matches!(
                        mode_courant,
                        Some(ModeExecution::BacktestPeriode | ModeExecution::BacktestFichier)
                    )
                {
                    println!(
                        "[{}] ALERTE {} {} | Prix: {:.2} | ADX: {:.1} | Vol Ratio: {:.2}x | SCORE: {}/100",
                        signal.horodatage,
                        signal.symbole,
                        signal.tendance.libelle(),
                        signal.prix,
                        signal.adx,
                        signal.ratio_volume,
                        signal.score
                    );
                }
                let _ = tx_evenements.send(EvenementServeur::Signal(signal));
            }
            notification = rx_statut.recv() => {
                let Some((generation, notification)) = notification else {
                    return Err("Le canal de statuts du moteur est fermé".into());
                };
                {
                    let mut etat = etat.write().await;
                    if generation == etat.generation {
                        etat.statut = Some((generation, notification.clone()));
                    }
                }
                afficher_notification(&notification);
                let _ = tx_evenements.send(EvenementServeur::Statut {
                    generation,
                    notification,
                });
            }
            commande = rx_commandes.recv() => {
                let Some(commande) = commande else { continue };
                if let CommandeMoteur::SauvegarderParametrages { parametrages } = &commande {
                    let notification = match parametrages.sauvegarder(&chemin_parametrages) {
                        Ok(()) => NotificationMoteur::Statut(format!(
                            "Paramétrages sauvegardés : {}",
                            chemin_parametrages.display()
                        )),
                        Err(erreur) => {
                            let message = format!("Sauvegarde impossible sur le moteur : {erreur}");
                            tracing::error!("{message}");
                            NotificationMoteur::Erreur(message)
                        }
                    };
                    let generation = {
                        let mut etat = etat.write().await;
                        etat.parametrages = parametrages.clone();
                        etat.statut = Some((etat.generation, notification.clone()));
                        etat.generation
                    };
                    let _ = tx_evenements.send(EvenementServeur::Statut {
                        generation,
                        notification,
                    });
                    continue;
                }
                {
                    let mut etat = etat.write().await;
                    match &commande {
                        CommandeMoteur::Arreter { generation, mode } => {
                            etat.generation = *generation;
                            etat.mode = *mode;
                            etat.signaux.clear();
                            etat.statut = Some((
                                *generation,
                                NotificationMoteur::Statut("Arrêt demandé".to_string()),
                            ));
                        }
                        CommandeMoteur::SauvegarderParametrages { .. } => {}
                        CommandeMoteur::Demarrer {
                            generation,
                            mode,
                            parametrages,
                            fournisseurs,
                            ..
                        } => {
                            etat.generation = *generation;
                            etat.mode = *mode;
                            etat.parametrages = parametrages.clone();
                            etat.fournisseurs = fournisseurs.clone();
                            etat.signaux.clear();
                            etat.statut = None;
                        }
                    }
                }
                if !matches!(
                    &commande,
                    CommandeMoteur::SauvegarderParametrages { .. }
                ) && tx_moteur.send(commande).is_err()
                {
                    return Err("Le moteur ne reçoit plus de commandes".into());
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("Arrêt du serveur demandé par Ctrl+C");
                return Ok(());
            }
        }
    }
}

/// Authentifie un client, puis relaie ses commandes et les événements du moteur.
///
/// Entrées : connexion TCP, jeton attendu, diffuseur d'événements, état partagé et canal moteur.
/// Sortie : ferme la connexion après déconnexion, erreur de protocole ou arrêt du moteur.
async fn gerer_client(
    stream: TcpStream,
    jeton_attendu: Arc<str>,
    tx_evenements: broadcast::Sender<EvenementServeur>,
    etat: Arc<RwLock<EtatServeur>>,
    tx_commandes: mpsc::UnboundedSender<CommandeMoteur>,
) {
    let (lecture, ecriture) = stream.into_split();
    let mut lecteur = BufReader::new(lecture);
    let mut ecrivain = BufWriter::new(ecriture);
    let mut abonnement = tx_evenements.subscribe();
    let mut ligne = String::new();

    let authentification = timeout(
        DELAI_AUTHENTIFICATION,
        lire_ligne_limitee(&mut lecteur, &mut ligne),
    )
    .await;
    let valide = matches!(authentification, Ok(Ok(_)))
        && matches!(
            serde_json::from_str::<RequeteClient>(&ligne),
            Ok(RequeteClient::Authentifier { ref jeton }) if jeton.as_str() == jeton_attendu.as_ref()
        );
    if !valide {
        let _ = ecrire_message(
            &mut ecrivain,
            &MessageServeur::Erreur("Authentification refusée ou délai dépassé".to_string()),
        )
        .await;
        warn!("Authentification refusée");
        return;
    }

    let etat_initial = etat.read().await.clone();
    if let Err(erreur) =
        ecrire_message(&mut ecrivain, &MessageServeur::Connecte(etat_initial)).await
    {
        warn!("Impossible d'envoyer l'état initial au client : {erreur}");
        return;
    }
    ligne.clear();

    loop {
        tokio::select! {
            resultat = lire_ligne_limitee(&mut lecteur, &mut ligne) => {
                match resultat {
                    Ok(0) => return,
                    Ok(_) => {
                        match serde_json::from_str::<RequeteClient>(&ligne) {
                            Ok(RequeteClient::Commande(commande)) => {
                                if tx_commandes.send(commande).is_err() {
                                    let _ = ecrire_message(
                                        &mut ecrivain,
                                        &MessageServeur::Erreur("Le moteur est arrêté".to_string()),
                                    ).await;
                                    return;
                                }
                            }
                            Ok(RequeteClient::Authentifier { .. }) => {
                                warn!("Nouvelle authentification ignorée sur une connexion active");
                                return;
                            }
                            Err(erreur) => {
                                warn!("Commande reçue invalide : {erreur}");
                                return;
                            }
                        }
                        ligne.clear();
                    }
                    Err(erreur) => {
                        warn!("Erreur de lecture client : {erreur}");
                        return;
                    }
                }
            }
            evenement = abonnement.recv() => {
                let message = match evenement {
                    Ok(evenement) => MessageServeur::Evenement(evenement),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = ecrire_message(
                            &mut ecrivain,
                            &MessageServeur::Erreur("Le client ne suit plus le flux du moteur ; reconnectez l'IHM".to_string()),
                        ).await;
                        return;
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                };
                if let Err(erreur) = ecrire_message(&mut ecrivain, &message).await {
                    warn!("Erreur d'envoi au client : {erreur}");
                    return;
                }
            }
        }
    }
}

/// Lit une ligne UTF-8 sans dépasser la taille maximale des messages réseau.
///
/// Entrées : lecteur asynchrone et chaîne à remplir.
/// Sortie : nombre d'octets lus, `0` à la fin du flux, ou erreur d'I/O/limite/encodage.
async fn lire_ligne_limitee<R>(lecteur: &mut R, ligne: &mut String) -> std::io::Result<usize>
where
    R: AsyncBufRead + Unpin,
{
    let mut octets = Vec::new();
    loop {
        let (taille, fin_ligne) = {
            let tampon = lecteur.fill_buf().await?;
            if tampon.is_empty() {
                if octets.is_empty() {
                    ligne.clear();
                    return Ok(0);
                }
                break;
            }
            let taille = tampon
                .iter()
                .position(|octet| *octet == b'\n')
                .map_or(tampon.len(), |index| index + 1);
            if octets.len() + taille > TAILLE_MAX_MESSAGE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Message réseau trop volumineux",
                ));
            }
            octets.extend_from_slice(&tampon[..taille]);
            (taille, tampon[taille - 1] == b'\n')
        };
        lecteur.consume(taille);
        if fin_ligne {
            break;
        }
    }
    *ligne = String::from_utf8(octets)
        .map_err(|erreur| std::io::Error::new(std::io::ErrorKind::InvalidData, erreur))?;
    Ok(ligne.len())
}

/// Affiche une notification du moteur au niveau de journal approprié.
///
/// Entrée : notification à afficher.
/// Sortie : écrit le statut sur stdout et les erreurs sur stderr.
fn afficher_notification(notification: &NotificationMoteur) {
    match notification {
        NotificationMoteur::Statut(texte) => println!("{texte}"),
        NotificationMoteur::Erreur(texte) => eprintln!("Erreur de source : {texte}"),
        NotificationMoteur::Echec(texte) => eprintln!("Erreur : {texte}"),
    }
}

/// Sérialise un message JSON et l'écrit sur une ligne du flux réseau.
///
/// Entrées : writer asynchrone et message sérialisable.
/// Sortie : `Ok(())` après écriture et vidage, ou erreur de sérialisation/I/O.
async fn ecrire_message<W, T>(
    ecrivain: &mut W,
    message: &T,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let json = serde_json::to_vec(message)?;
    ecrivain.write_all(&json).await?;
    ecrivain.write_all(b"\n").await?;
    ecrivain.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn etat_test() -> EtatServeur {
        let config =
            crate::config::ConfigurationApplication::depuis_toml(include_str!("../config.toml"))
                .unwrap();
        EtatServeur {
            symboles: config.paires.symboles.clone(),
            parametrages: config.parametrages(),
            fournisseurs: config.sources_donnees.fournisseurs,
            generation: 0,
            mode: ModeExecution::TempsReel,
            statut: None,
            signaux: Vec::new(),
        }
    }

    #[tokio::test]
    async fn authentifie_le_client_et_echange_commandes_et_evenements() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let adresse = listener.local_addr().unwrap();
        let (tx_evenements, _) = broadcast::channel(8);
        let (tx_commandes, mut rx_commandes) = mpsc::unbounded_channel();
        let etat = Arc::new(RwLock::new(etat_test()));
        let jeton = Arc::<str>::from("jeton-test-avec-assez-de-caracteres");
        let jeton_client = jeton.to_string();
        let tx_evenements_serveur = tx_evenements.clone();

        let serveur = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            gerer_client(
                stream,
                Arc::clone(&jeton),
                tx_evenements_serveur,
                etat,
                tx_commandes,
            )
            .await;
        });

        let mut connexion = connecter(&adresse.to_string(), jeton_client).await.unwrap();
        assert_eq!(connexion.etat.generation, 0);

        tx_evenements
            .send(EvenementServeur::Statut {
                generation: 0,
                notification: NotificationMoteur::Statut("Flux actif".to_string()),
            })
            .unwrap();
        let (_, notification) =
            tokio::time::timeout(Duration::from_secs(1), connexion.rx_statut.recv())
                .await
                .unwrap()
                .unwrap();
        assert!(matches!(
            notification,
            NotificationMoteur::Statut(texte) if texte == "Flux actif"
        ));

        connexion
            .tx_commandes
            .send(CommandeMoteur::Arreter {
                generation: 1,
                mode: ModeExecution::BacktestFichier,
            })
            .unwrap();
        let commande = tokio::time::timeout(Duration::from_secs(1), rx_commandes.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            commande,
            CommandeMoteur::Arreter {
                generation: 1,
                mode: ModeExecution::BacktestFichier
            }
        ));
        drop(connexion);
        serveur.abort();
    }
}
