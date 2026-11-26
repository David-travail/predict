//! Côté IHM : échanges avec le moteur dans un thread réseau séparé.
//!
//! Le thread graphique envoie ses requêtes par un canal `tokio::mpsc` (envoi non bloquant)
//! et vide sans bloquer un canal `std::mpsc` de retours. Le thread réseau lit l'état
//! périodiquement et transmet les requêtes de l'IHM, une à la fois.

use crate::protocole::{EtatMoteur, ReponseMoteur, RequeteIhm, ecrire_message, lire_ligne_limitee};
use std::time::Duration;
use tokio::io::{BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio::time::{MissedTickBehavior, timeout};

const DELAI_CONNEXION: Duration = Duration::from_secs(5);
const DELAI_REPONSE: Duration = Duration::from_secs(10);

/// Retour du thread réseau vers le thread graphique.
pub enum RetourReseau {
    /// Résultat d'une lecture périodique de l'état.
    Etat(Result<EtatMoteur, String>),
    /// Réponse à une requête envoyée par l'IHM.
    Reponse(Result<ReponseMoteur, String>),
}

/// Extrémités, côté thread graphique, des canaux vers le thread réseau.
pub struct ClientMoteur {
    pub requetes: UnboundedSender<RequeteIhm>,
    pub retours: std::sync::mpsc::Receiver<RetourReseau>,
}

/// Lance le thread réseau de l'IHM.
///
/// Entrées : adresse du moteur, intervalle de lecture et fonction appelée après chaque retour
/// (pour redessiner l'IHM).
/// Sortie : canaux du thread graphique ; le thread s'arrête quand l'un d'eux est fermé.
pub fn demarrer(
    adresse: String,
    intervalle: Duration,
    reveiller: impl Fn() + Send + 'static,
) -> ClientMoteur {
    let (tx_requetes, mut rx_requetes) = unbounded_channel::<RequeteIhm>();
    let (tx_retours, rx_retours) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("reseau-ihm".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(erreur) => {
                    let _ = tx_retours.send(RetourReseau::Etat(Err(format!(
                        "Thread réseau indisponible : {erreur}"
                    ))));
                    reveiller();
                    return;
                }
            };
            runtime.block_on(async move {
                let mut minuterie = tokio::time::interval(intervalle);
                minuterie.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut suivi = SuiviLecture::default();
                let mut relire = false;
                loop {
                    let requete = if relire {
                        relire = false;
                        None
                    } else {
                        tokio::select! {
                            _ = minuterie.tick() => None,
                            requete = rx_requetes.recv() => match requete {
                                Some(requete) => Some(requete),
                                None => break, // IHM fermée
                            },
                        }
                    };
                    let retour = match requete {
                        None => {
                            let lecture = RequeteIhm::LireEtat {
                                curseur: suivi.curseur,
                            };
                            let resultat = match echanger(&adresse, &lecture).await {
                                Ok(ReponseMoteur::Etat(etat)) => Ok(etat),
                                Ok(ReponseMoteur::Resultat { message, .. }) => Err(message),
                                Ok(_) => Err("Réponse inattendue du moteur".to_string()),
                                Err(erreur) => Err(erreur),
                            };
                            match resultat {
                                Ok(etat) => match suivi.integrer(&etat) {
                                    Integration::Transmettre { suite } => {
                                        relire = suite;
                                        RetourReseau::Etat(Ok(etat))
                                    }
                                    Integration::Relire => {
                                        relire = true;
                                        continue;
                                    }
                                },
                                Err(erreur) => RetourReseau::Etat(Err(erreur)),
                            }
                        }
                        Some(requete) => RetourReseau::Reponse(echanger(&adresse, &requete).await),
                    };
                    if tx_retours.send(retour).is_err() {
                        break; // IHM fermée
                    }
                    reveiller();
                }
            });
        })
        .expect("création du thread réseau de l'IHM");
    ClientMoteur {
        requetes: tx_requetes,
        retours: rx_retours,
    }
}

/// Suivi, par le thread réseau, du journal déjà reçu.
#[derive(Default)]
struct SuiviLecture {
    instance: Option<u64>,
    curseur: u64,
}

enum Integration {
    /// État à transmettre ; `suite` si d'autres événements attendent.
    Transmettre { suite: bool },
    /// Le moteur a redémarré : relire depuis le début avant de transmettre.
    Relire,
}

impl SuiviLecture {
    fn integrer(&mut self, etat: &EtatMoteur) -> Integration {
        if self.instance.is_some_and(|instance| instance != etat.instance) && self.curseur != 0 {
            // Nouveau moteur : ses numéros de séquence repartent de 1.
            self.instance = Some(etat.instance);
            self.curseur = 0;
            return Integration::Relire;
        }
        self.instance = Some(etat.instance);
        if let Some(dernier) = etat.evenements.last() {
            self.curseur = self.curseur.max(dernier.seq);
        }
        Integration::Transmettre { suite: etat.suite }
    }
}

/// Un échange complet : connexion, une ligne envoyée, une ligne reçue.
///
/// Entrées : adresse du moteur et requête.
/// Sortie : réponse décodée, ou message d'erreur lisible.
pub async fn echanger(adresse: &str, requete: &RequeteIhm) -> Result<ReponseMoteur, String> {
    let flux = timeout(DELAI_CONNEXION, TcpStream::connect(adresse))
        .await
        .map_err(|_| format!("Moteur injoignable ({adresse}) : délai de connexion dépassé"))?
        .map_err(|erreur| format!("Moteur injoignable ({adresse}) : {erreur}"))?;
    let (lecture, ecriture) = flux.into_split();
    let mut ecrivain = BufWriter::new(ecriture);
    ecrire_message(&mut ecrivain, requete)
        .await
        .map_err(|erreur| format!("Envoi au moteur impossible : {erreur}"))?;

    let mut lecteur = BufReader::new(lecture);
    let mut ligne = String::new();
    let lue = timeout(DELAI_REPONSE, lire_ligne_limitee(&mut lecteur, &mut ligne))
        .await
        .map_err(|_| "Le moteur n'a pas répondu à temps".to_string())?
        .map_err(|erreur| format!("Lecture de la réponse du moteur impossible : {erreur}"))?;
    if lue == 0 {
        return Err("Le moteur a fermé la connexion sans répondre".to_string());
    }
    serde_json::from_str(&ligne).map_err(|erreur| format!("Réponse du moteur invalide : {erreur}"))
}

/// Annonce au moteur la fermeture de l'IHM ; un échec est sans conséquence (délai d'absence).
///
/// Entrées : runtime pour l'envoi et adresse du moteur.
pub fn annoncer_fermeture(runtime: &tokio::runtime::Runtime, adresse: &str) {
    let _ = runtime.block_on(timeout(
        Duration::from_secs(2),
        echanger(adresse, &RequeteIhm::FermetureIhm),
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocole::{ContenuEvenement, Evenement};
    use crate::ui::ModeExecution;

    fn etat(instance: u64, seqs: &[u64], suite: bool) -> EtatMoteur {
        let config =
            crate::config::ConfigurationApplication::depuis_toml(include_str!("../config.toml"))
                .unwrap();
        EtatMoteur {
            instance,
            generation: 0,
            mode: ModeExecution::TempsReel,
            statut: None,
            symboles: Vec::new(),
            parametrages: config.parametrages(),
            fournisseurs: Vec::new(),
            paires: Vec::new(),
            nb_messages: 0,
            dernier_message_il_y_a_ms: None,
            evenements: seqs
                .iter()
                .map(|seq| Evenement {
                    seq: *seq,
                    contenu: ContenuEvenement::Info(String::new()),
                })
                .collect(),
            suite,
            ressources: None,
            notifications: Default::default(),
            telegram_configure: false,
        }
    }

    #[test]
    fn suit_le_curseur_et_detecte_le_redemarrage() {
        let mut suivi = SuiviLecture::default();
        assert!(matches!(
            suivi.integrer(&etat(1, &[1, 2], true)),
            Integration::Transmettre { suite: true }
        ));
        assert_eq!(suivi.curseur, 2);
        assert!(matches!(
            suivi.integrer(&etat(1, &[], false)),
            Integration::Transmettre { suite: false }
        ));
        assert_eq!(suivi.curseur, 2);
        // Le moteur a redémarré : on relit depuis le début.
        assert!(matches!(suivi.integrer(&etat(2, &[7], false)), Integration::Relire));
        assert_eq!(suivi.curseur, 0);
        assert!(matches!(
            suivi.integrer(&etat(2, &[1], false)),
            Integration::Transmettre { .. }
        ));
        assert_eq!(suivi.curseur, 1);
    }

    #[tokio::test]
    async fn signale_un_moteur_injoignable() {
        // Port réservé puis libéré : personne n'écoute.
        let adresse = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let erreur = echanger(&adresse, &RequeteIhm::LireEtat { curseur: 0 })
            .await
            .unwrap_err();
        assert!(erreur.contains("injoignable"));
    }
}
