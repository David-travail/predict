//! Contrat d'échange entre l'IHM et le moteur (voir `COMMUNICATION_IHM_MOTEUR.md`).
//!
//! Une connexion TCP par requête : l'IHM écrit une ligne JSON (`RequeteIhm`), le moteur répond
//! par une ligne JSON (`ReponseMoteur`) puis ferme. Le moteur ne pousse rien : l'IHM interroge
//! périodiquement avec `LireEtat`.

use crate::config::Parametrages;
use crate::data::sources::Fournisseur;
use crate::ui::{ModeExecution, SignalVisuel};
use serde::{Deserialize, Serialize};
use std::error::Error;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt};

/// Taille maximale d'une ligne JSON échangée.
pub const TAILLE_MAX_MESSAGE: usize = 1024 * 1024;
/// Nombre maximal d'événements renvoyés par une réponse `Etat`.
pub const EVENEMENTS_PAR_REPONSE: usize = 500;
/// Nombre maximal d'entrées renvoyées par `ContenuDossier`.
pub const ENTREES_DOSSIER_MAX: usize = 500;

/// IHM → moteur : une ligne JSON, une connexion par requête.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequeteIhm {
    /// Lecture périodique. `curseur` = dernier numéro de séquence déjà reçu (0 au départ).
    LireEtat { curseur: u64 },
    /// Démarre une session ; le moteur attribue une nouvelle génération.
    Demarrer {
        mode: ModeExecution,
        date_debut: String,
        date_fin: String,
        /// Chemin sur la machine du moteur.
        chemin_fichier: String,
        symboles_test: Vec<String>,
        parametrages: Parametrages,
        fournisseurs: Vec<Fournisseur>,
    },
    /// Arrête la session et sélectionne un mode sans le démarrer (modes de test).
    Arreter { mode: ModeExecution },
    SauvegarderParametrages { parametrages: Parametrages },
    /// Liste un dossier de la machine du moteur ; vide = son dossier de travail.
    ListerDossier { chemin: String },
    /// L'IHM annonce sa fermeture.
    FermetureIhm,
}

/// Moteur → IHM : une ligne JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReponseMoteur {
    Etat(EtatMoteur),
    /// Réponse générique ; `message` est rédigé pour l'utilisateur.
    Resultat {
        succes: bool,
        message: String,
        /// Génération attribuée par `Demarrer` / `Arreter`.
        #[serde(default)]
        generation: Option<u64>,
    },
    ContenuDossier(ContenuDossier),
}

/// Contenu d'un dossier de la machine du moteur.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContenuDossier {
    /// Chemin absolu listé (ou demandé, en cas d'erreur).
    pub chemin: String,
    pub parent: Option<String>,
    /// Noms des sous-dossiers, triés.
    pub dossiers: Vec<String>,
    /// Noms des fichiers `.json`, triés.
    pub fichiers_json: Vec<String>,
    /// Vrai si la liste a été coupée à `ENTREES_DOSSIER_MAX`.
    pub tronque: bool,
    pub erreur: Option<String>,
}

/// Instantané de tout ce que l'IHM affiche.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EtatMoteur {
    /// Identifiant tiré au démarrage du serveur : s'il change, le moteur a redémarré.
    pub instance: u64,
    /// Génération attribuée par le serveur à chaque `Demarrer` / `Arreter`.
    pub generation: u64,
    pub mode: ModeExecution,
    /// Dernier statut de la session en cours.
    pub statut: Option<String>,
    pub symboles: Vec<String>,
    pub parametrages: Parametrages,
    pub fournisseurs: Vec<Fournisseur>,
    /// Dernier signal reçu pour chaque paire.
    pub paires: Vec<SignalVisuel>,
    /// Messages reçus du flux en direct depuis le début de la session.
    pub nb_messages: u64,
    /// Âge du dernier message du flux (durée relative : pas d'horloge partagée).
    pub dernier_message_il_y_a_ms: Option<u64>,
    /// Événements de numéro supérieur au curseur demandé, au plus `EVENEMENTS_PAR_REPONSE`.
    pub evenements: Vec<Evenement>,
    /// Vrai si d'autres événements attendent : l'IHM doit relire aussitôt.
    pub suite: bool,
    #[serde(default)]
    pub ressources: Option<RessourcesServeur>,
}

/// Événement du journal du serveur, à ne pas manquer entre deux lectures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evenement {
    pub seq: u64,
    pub contenu: ContenuEvenement,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContenuEvenement {
    Alerte(SignalVisuel),
    /// Message ponctuel (ex. sauvegarde).
    Info(String),
    /// Erreur de source de données.
    Erreur(String),
    /// Échec de la session.
    Echec(String),
}

/// Occupation CPU/mémoire du processus serveur.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RessourcesServeur {
    /// Part de la capacité CPU totale de la machine utilisée par le serveur (0–100 %).
    pub cpu_pourcent: f32,
    /// Mémoire résidente du processus serveur, en octets.
    pub memoire_octets: u64,
    /// Mémoire physique totale de la machine, en octets.
    pub memoire_totale_octets: u64,
}

/// Lit une ligne UTF-8 sans dépasser `TAILLE_MAX_MESSAGE`.
///
/// Entrées : lecteur asynchrone et chaîne à remplir.
/// Sortie : nombre d'octets lus, `0` à la fin du flux, ou erreur d'I/O/limite/encodage.
pub async fn lire_ligne_limitee<R>(lecteur: &mut R, ligne: &mut String) -> std::io::Result<usize>
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

/// Sérialise un message JSON et l'écrit sur une ligne du flux réseau.
///
/// Entrées : writer asynchrone et message sérialisable.
/// Sortie : `Ok(())` après écriture et vidage, ou erreur de sérialisation/I/O.
pub async fn ecrire_message<W, T>(
    ecrivain: &mut W,
    message: &T,
) -> Result<(), Box<dyn Error + Send + Sync>>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut json = serde_json::to_vec(message)?;
    json.push(b'\n');
    ecrivain.write_all(&json).await?;
    ecrivain.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::modeles::Tendance;

    fn signal() -> SignalVisuel {
        SignalVisuel {
            generation: 3,
            symbole: "BTCEUR".to_string(),
            prix: 50_000.0,
            prix_precedent: 49_000.0,
            score: 90,
            adx: 30.0,
            adx_valide: true,
            ratio_volume: 1.5,
            volume_valide: true,
            tendance: Tendance::Haussiere,
            est_alerte: true,
            unite: "1h".to_string(),
            horodatage: "2026-01-01 10:00".to_string(),
        }
    }

    fn aller_retour<T: Serialize + for<'de> Deserialize<'de>>(valeur: &T) -> T {
        let json = serde_json::to_string(valeur).unwrap();
        assert!(!json.contains('\n'), "un message doit tenir sur une ligne");
        serde_json::from_str(&json).unwrap()
    }

    #[test]
    fn messages_aller_retour_json() {
        let config =
            crate::config::ConfigurationApplication::depuis_toml(include_str!("../config.toml"))
                .unwrap();

        let requetes = [
            RequeteIhm::LireEtat { curseur: 12 },
            RequeteIhm::Demarrer {
                mode: ModeExecution::BacktestFichier,
                date_debut: String::new(),
                date_fin: String::new(),
                chemin_fichier: "/tmp/h.json".to_string(),
                symboles_test: vec!["BTCEUR".to_string()],
                parametrages: config.parametrages(),
                fournisseurs: config.sources_donnees.fournisseurs.clone(),
            },
            RequeteIhm::Arreter {
                mode: ModeExecution::BacktestPeriode,
            },
            RequeteIhm::SauvegarderParametrages {
                parametrages: config.parametrages(),
            },
            RequeteIhm::ListerDossier {
                chemin: String::new(),
            },
            RequeteIhm::FermetureIhm,
        ];
        for requete in &requetes {
            let relue = aller_retour(requete);
            assert_eq!(
                serde_json::to_string(&relue).unwrap(),
                serde_json::to_string(requete).unwrap()
            );
        }
        assert_eq!(
            serde_json::to_string(&RequeteIhm::FermetureIhm).unwrap(),
            "\"FermetureIhm\""
        );

        let etat = ReponseMoteur::Etat(EtatMoteur {
            instance: 7,
            generation: 3,
            mode: ModeExecution::TempsReel,
            statut: Some("🔴 Flux actif".to_string()),
            symboles: config.paires.symboles.clone(),
            parametrages: config.parametrages(),
            fournisseurs: config.sources_donnees.fournisseurs.clone(),
            paires: vec![signal()],
            nb_messages: 4,
            dernier_message_il_y_a_ms: Some(1500),
            evenements: vec![
                Evenement {
                    seq: 1,
                    contenu: ContenuEvenement::Alerte(signal()),
                },
                Evenement {
                    seq: 2,
                    contenu: ContenuEvenement::Erreur("source muette".to_string()),
                },
            ],
            suite: false,
            ressources: Some(RessourcesServeur {
                cpu_pourcent: 1.5,
                memoire_octets: 10,
                memoire_totale_octets: 100,
            }),
        });
        let ReponseMoteur::Etat(relu) = aller_retour(&etat) else {
            panic!("variante perdue");
        };
        assert_eq!(relu.generation, 3);
        assert_eq!(relu.paires[0].symbole, "BTCEUR");
        assert_eq!(relu.evenements.len(), 2);
        assert_eq!(relu.ressources.unwrap().memoire_octets, 10);

        let resultat = ReponseMoteur::Resultat {
            succes: true,
            message: "ok".to_string(),
            generation: Some(4),
        };
        assert!(matches!(
            aller_retour(&resultat),
            ReponseMoteur::Resultat { succes: true, generation: Some(4), .. }
        ));
        // Champ ajouté après coup : absent = valeur par défaut.
        let ancien: ReponseMoteur =
            serde_json::from_str(r#"{"Resultat":{"succes":false,"message":"x"}}"#).unwrap();
        assert!(matches!(ancien, ReponseMoteur::Resultat { generation: None, .. }));

        let dossier = ReponseMoteur::ContenuDossier(ContenuDossier {
            chemin: "/home".to_string(),
            parent: Some("/".to_string()),
            dossiers: vec!["pi".to_string()],
            fichiers_json: vec!["h.json".to_string()],
            tronque: false,
            erreur: None,
        });
        assert!(matches!(aller_retour(&dossier), ReponseMoteur::ContenuDossier(c) if c.dossiers == ["pi"]));
    }
}
