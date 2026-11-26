//! Serveur du moteur : tient l'instantané à jour et répond aux requêtes de l'IHM.
//!
//! Une connexion TCP par requête (voir `protocole`). Le serveur ne pousse rien.

use crate::config::{ChoixNotifications, Parametrages};
use crate::data::sources::Fournisseur;
use crate::data::telegram::{NotificateurTelegram, TypeNotification};
use crate::protocole::{
    ContenuDossier, ContenuEvenement, ENTREES_DOSSIER_MAX, EVENEMENTS_PAR_REPONSE, EtatMoteur,
    Evenement, ReponseMoteur, RequeteIhm, RessourcesServeur, ecrire_message, lire_ligne_limitee,
};
use crate::ui::{CommandeMoteur, ModeExecution, NotificationMoteur, SignalVisuel};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::error::Error;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::io::{BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Duration, timeout};
use tracing::{error, info, warn};

/// Délai maximal pour recevoir la requête d'un client.
const DELAI_REQUETE: Duration = Duration::from_secs(10);
/// Connexions traitées simultanément au plus.
const CLIENTS_MAX: usize = 32;
const PERIODE_RESSOURCES: Duration = Duration::from_secs(2);
const PERIODE_PRESENCE: Duration = Duration::from_secs(5);
/// Événements (alertes, erreurs, infos) conservés par le serveur.
const JOURNAL_MAX: usize = 5000;

/// Mesure l'occupation CPU/mémoire du processus courant.
struct MesureRessources {
    systeme: sysinfo::System,
    pid: sysinfo::Pid,
    nb_coeurs: f32,
}

impl MesureRessources {
    /// Prépare la mesure ; `None` si la plateforme ne permet pas de lire le processus.
    fn new() -> Option<Self> {
        let pid = sysinfo::get_current_pid().ok()?;
        let mut systeme = sysinfo::System::new().ok()?;
        systeme.refresh_memory();
        let nb_coeurs = std::thread::available_parallelism()
            .map(|n| n.get() as f32)
            .unwrap_or(1.0);
        let mut mesure = Self {
            systeme,
            pid,
            nb_coeurs,
        };
        // Première lecture : sert de référence au calcul du CPU de la mesure suivante.
        mesure.rafraichir();
        Some(mesure)
    }

    fn rafraichir(&mut self) {
        self.systeme.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[self.pid]),
            false,
            sysinfo::ProcessRefreshKind::nothing()
                .with_cpu()
                .with_memory(),
        );
    }

    /// Relit le processus et renvoie l'occupation depuis la mesure précédente.
    fn mesurer(&mut self) -> Option<RessourcesServeur> {
        self.rafraichir();
        let processus = self.systeme.process(self.pid)?;
        Some(RessourcesServeur {
            cpu_pourcent: (processus.cpu_usage() / self.nb_coeurs).clamp(0.0, 100.0),
            memoire_octets: processus.memory(),
            memoire_totale_octets: self.systeme.total_memory(),
        })
    }
}

/// Réglages de départ du serveur.
pub struct EtatInitial {
    pub symboles: Vec<String>,
    pub parametrages: Parametrages,
    pub fournisseurs: Vec<Fournisseur>,
    pub mode: ModeExecution,
    pub notifications: ChoixNotifications,
    pub telegram_configure: bool,
}

/// Instantané tenu par le serveur, lu par `LireEtat`.
struct Instantane {
    instance: u64,
    generation: u64,
    mode: ModeExecution,
    statut: Option<String>,
    symboles: Vec<String>,
    parametrages: Parametrages,
    fournisseurs: Vec<Fournisseur>,
    paires: BTreeMap<String, SignalVisuel>,
    nb_messages: u64,
    dernier_message: Option<Instant>,
    journal: VecDeque<Evenement>,
    prochaine_seq: u64,
    ressources: Option<RessourcesServeur>,
    notifications: ChoixNotifications,
    telegram_configure: bool,
}

impl Instantane {
    fn new(initial: EtatInitial) -> Self {
        let instance = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64)
            .max(1);
        Self {
            instance,
            generation: 0,
            mode: initial.mode,
            statut: None,
            symboles: initial.symboles,
            parametrages: initial.parametrages,
            fournisseurs: initial.fournisseurs,
            paires: BTreeMap::new(),
            nb_messages: 0,
            dernier_message: None,
            journal: VecDeque::new(),
            prochaine_seq: 1,
            ressources: None,
            notifications: initial.notifications,
            telegram_configure: initial.telegram_configure,
        }
    }

    /// Ajoute un événement au journal plafonné.
    fn journaliser(&mut self, contenu: ContenuEvenement) {
        if self.journal.len() >= JOURNAL_MAX {
            self.journal.pop_front();
        }
        self.journal.push_back(Evenement {
            seq: self.prochaine_seq,
            contenu,
        });
        self.prochaine_seq += 1;
    }

    /// Ouvre une nouvelle génération : vide l'affichage, le journal et les compteurs.
    fn nouvelle_generation(&mut self, mode: ModeExecution) -> u64 {
        self.generation += 1;
        self.mode = mode;
        self.statut = None;
        self.paires.clear();
        self.journal.clear();
        self.nb_messages = 0;
        self.dernier_message = None;
        self.generation
    }

    /// Intègre un signal du moteur s'il appartient à la génération courante.
    ///
    /// Sortie : `true` si le signal a été retenu.
    fn recevoir_signal(&mut self, signal: SignalVisuel) -> bool {
        if signal.generation != self.generation {
            return false;
        }
        // Les signaux d'amorçage (avant l'ouverture du flux) ne comptent pas.
        if self
            .statut
            .as_deref()
            .is_some_and(|s| s.starts_with('🔴') || s.starts_with('🟡'))
        {
            self.nb_messages += 1;
            self.dernier_message = Some(Instant::now());
        }
        if signal.est_alerte {
            self.journaliser(ContenuEvenement::Alerte(signal.clone()));
        }
        self.paires.insert(signal.symbole.clone(), signal);
        true
    }

    /// Intègre une notification du moteur si elle appartient à la génération courante.
    fn recevoir_notification(&mut self, generation: u64, notification: NotificationMoteur) {
        if generation != self.generation {
            return;
        }
        match notification {
            NotificationMoteur::Statut(texte) => self.statut = Some(texte),
            NotificationMoteur::Info(texte) => self.journaliser(ContenuEvenement::Info(texte)),
            NotificationMoteur::Erreur(texte) => self.journaliser(ContenuEvenement::Erreur(texte)),
            NotificationMoteur::Echec(texte) => self.journaliser(ContenuEvenement::Echec(texte)),
        }
    }

    /// Construit la réponse à `LireEtat` pour un curseur donné.
    fn etat(&self, curseur: u64) -> EtatMoteur {
        let mut nouveaux = self.journal.iter().filter(|e| e.seq > curseur);
        let evenements: Vec<Evenement> =
            nouveaux.by_ref().take(EVENEMENTS_PAR_REPONSE).cloned().collect();
        let suite = nouveaux.next().is_some();
        EtatMoteur {
            instance: self.instance,
            generation: self.generation,
            mode: self.mode,
            statut: self.statut.clone(),
            symboles: self.symboles.clone(),
            parametrages: self.parametrages.clone(),
            fournisseurs: self.fournisseurs.clone(),
            paires: self.paires.values().cloned().collect(),
            nb_messages: self.nb_messages,
            dernier_message_il_y_a_ms: self
                .dernier_message
                .map(|t| t.elapsed().as_millis() as u64),
            evenements,
            suite,
            ressources: self.ressources,
            notifications: self.notifications,
            telegram_configure: self.telegram_configure,
        }
    }
}

/// État partagé entre la boucle du serveur et le traitement de chaque requête.
struct Partage {
    instantane: Mutex<Instantane>,
    tx_moteur: mpsc::UnboundedSender<CommandeMoteur>,
    /// IHM vues récemment (identifiées par IP) et date de leur dernière lecture.
    ihm: Mutex<HashMap<IpAddr, Instant>>,
    /// Absence au-delà de laquelle une IHM muette est déclarée déconnectée.
    delai_absence: Duration,
    /// Notificateur Telegram du moteur (absent : Telegram non configuré).
    notificateur: Option<NotificateurTelegram>,
    /// Vrai si l'arrivée et le départ des IHM sont à notifier.
    presence_ihm: bool,
    /// `config.toml` où enregistrer le choix des notifications (`None` : configuration intégrée).
    chemin_config: Option<PathBuf>,
    /// Sérialise les écritures de `config.toml` demandées par des IHM concurrentes.
    ecriture_config: tokio::sync::Mutex<()>,
}

impl Partage {
    /// Ouvre une génération et transmet la commande correspondante au moteur.
    ///
    /// Entrées : mode, réglages de session à retenir (pour `Demarrer`) et fabrique de la
    /// commande à partir de la génération attribuée.
    /// Sortie : génération attribuée, ou erreur si le moteur ne reçoit plus de commandes.
    fn commander(
        &self,
        mode: ModeExecution,
        reglages: Option<(Parametrages, Vec<Fournisseur>)>,
        commande: impl FnOnce(u64) -> CommandeMoteur,
    ) -> Result<u64, String> {
        let mut instantane = self.instantane.lock().expect("instantané verrouillé");
        let generation = instantane.nouvelle_generation(mode);
        if let Some((parametrages, fournisseurs)) = reglages {
            instantane.parametrages = parametrages;
            instantane.fournisseurs = fournisseurs;
        }
        // Envoi sous verrou : deux IHM concurrentes ne peuvent pas inverser l'ordre des générations.
        self.tx_moteur
            .send(commande(generation))
            .map_err(|_| "Le moteur est arrêté.".to_string())?;
        Ok(generation)
    }

    /// Note la lecture d'une IHM ; notifie sa première apparition.
    fn enregistrer_lecture(&self, ip: IpAddr) {
        let nouvelle = self
            .ihm
            .lock()
            .expect("liste des IHM verrouillée")
            .insert(ip, Instant::now())
            .is_none();
        if nouvelle {
            info!("IHM connectée : {ip}");
            self.notifier(format!("🖥 IHM connectée ({ip})"));
        }
    }

    /// Retire une IHM qui annonce sa fermeture.
    fn retirer(&self, ip: IpAddr) {
        let presente = self
            .ihm
            .lock()
            .expect("liste des IHM verrouillée")
            .remove(&ip)
            .is_some();
        if presente {
            info!("IHM déconnectée : {ip}");
            self.notifier(format!("🔌 IHM déconnectée ({ip})"));
        }
    }

    /// Déclare déconnectées les IHM muettes depuis plus de `delai_absence`.
    fn verifier_absences(&self) {
        let absentes: Vec<IpAddr> = {
            let mut ihm = self.ihm.lock().expect("liste des IHM verrouillée");
            let absentes: Vec<IpAddr> = ihm
                .iter()
                .filter(|(_, vue)| vue.elapsed() > self.delai_absence)
                .map(|(ip, _)| *ip)
                .collect();
            for ip in &absentes {
                ihm.remove(ip);
            }
            absentes
        };
        for ip in absentes {
            info!("IHM sans nouvelles, considérée déconnectée : {ip}");
            self.notifier(format!("🔌 IHM déconnectée ({ip}, sans nouvelles)"));
        }
    }

    /// Notifie la présence d'une IHM, si demandé.
    fn notifier(&self, message: String) {
        if !self.presence_ihm {
            return;
        }
        if let Some(notificateur) = self.notificateur.clone() {
            tokio::spawn(async move {
                if let Err(erreur) = notificateur
                    .notifier(TypeNotification::PresenceIhm, message)
                    .await
                {
                    error!("Impossible d'envoyer la notification Telegram : {erreur}");
                }
            });
        }
    }
}

/// Options du serveur.
pub struct OptionsServeur {
    /// Intervalle de lecture des IHM : sert à détecter leur disparition.
    pub intervalle_ihm: Duration,
    /// Notificateur Telegram du moteur, dont le serveur règle le choix des notifications.
    pub notificateur: Option<NotificateurTelegram>,
    /// Notifie l'arrivée et le départ des IHM.
    pub presence_ihm: bool,
    /// `config.toml` où enregistrer le choix des notifications.
    pub chemin_config: Option<PathBuf>,
    /// Affiche statuts et alertes de test dans la console.
    pub console: bool,
}

/// Fait tourner le serveur : agrège ce que produit le moteur et répond aux IHM.
///
/// Entrées : listener, état de départ, canal des commandes moteur, récepteurs des signaux et
/// statuts du moteur, options et futur dont la fin arrête le serveur.
/// Sortie : `Ok(())` à l'arrêt demandé, erreur si le listener ou un canal du moteur échoue.
pub async fn servir(
    listener: TcpListener,
    initial: EtatInitial,
    tx_moteur: mpsc::UnboundedSender<CommandeMoteur>,
    mut rx_signaux: mpsc::Receiver<SignalVisuel>,
    mut rx_statut: mpsc::UnboundedReceiver<(u64, NotificationMoteur)>,
    options: OptionsServeur,
    arret: impl Future<Output = ()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let partage = Arc::new(Partage {
        instantane: Mutex::new(Instantane::new(initial)),
        tx_moteur,
        ihm: Mutex::new(HashMap::new()),
        delai_absence: options.intervalle_ihm * 3 + Duration::from_secs(10),
        notificateur: options.notificateur,
        presence_ihm: options.presence_ihm,
        chemin_config: options.chemin_config,
        ecriture_config: tokio::sync::Mutex::new(()),
    });
    let places = Arc::new(Semaphore::new(CLIENTS_MAX));
    let mut mesure_ressources = MesureRessources::new();
    if mesure_ressources.is_none() {
        warn!("Mesure de l'occupation CPU/mémoire du serveur indisponible");
    }
    let mut minuterie_ressources = tokio::time::interval(PERIODE_RESSOURCES);
    minuterie_ressources.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut minuterie_presence = tokio::time::interval(PERIODE_PRESENCE);
    minuterie_presence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tokio::pin!(arret);

    info!("Serveur prêt : {}", listener.local_addr()?);
    loop {
        tokio::select! {
            connexion = listener.accept() => {
                let (stream, adresse) = match connexion {
                    Ok(connexion) => connexion,
                    Err(erreur) => {
                        warn!("Connexion entrante refusée : {erreur}");
                        continue;
                    }
                };
                let Ok(place) = Arc::clone(&places).try_acquire_owned() else {
                    warn!("Connexion refusée : {CLIENTS_MAX} requêtes déjà en cours");
                    continue;
                };
                let partage = Arc::clone(&partage);
                tokio::spawn(async move {
                    let _place = place;
                    if let Err(erreur) = gerer_client(stream, adresse, &partage).await {
                        warn!("Requête de {adresse} : {erreur}");
                    }
                });
            }
            signal = rx_signaux.recv() => {
                let Some(signal) = signal else {
                    return Err("Le canal de signaux du moteur est fermé".into());
                };
                let (retenu, mode) = {
                    let mut instantane = partage.instantane.lock().expect("instantané verrouillé");
                    (instantane.recevoir_signal(signal.clone()), instantane.mode)
                };
                if options.console
                    && retenu
                    && signal.est_alerte
                    && matches!(mode, ModeExecution::BacktestPeriode | ModeExecution::BacktestFichier)
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
            }
            notification = rx_statut.recv() => {
                let Some((generation, notification)) = notification else {
                    return Err("Le canal de statuts du moteur est fermé".into());
                };
                if options.console {
                    afficher_notification(&notification);
                }
                partage
                    .instantane
                    .lock()
                    .expect("instantané verrouillé")
                    .recevoir_notification(generation, notification);
            }
            _ = minuterie_ressources.tick(), if mesure_ressources.is_some() => {
                if let Some(ressources) = mesure_ressources.as_mut().and_then(MesureRessources::mesurer) {
                    partage.instantane.lock().expect("instantané verrouillé").ressources = Some(ressources);
                }
            }
            _ = minuterie_presence.tick() => partage.verifier_absences(),
            _ = &mut arret => {
                info!("Arrêt du serveur demandé");
                return Ok(());
            }
        }
    }
}

/// Traite une requête : lit une ligne, répond par une ligne, ferme.
///
/// Entrées : connexion, adresse du client et état partagé.
/// Sortie : `Ok(())` après réponse, ou erreur de lecture/écriture.
async fn gerer_client(
    stream: TcpStream,
    adresse: SocketAddr,
    partage: &Partage,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (lecture, ecriture) = stream.into_split();
    let mut lecteur = BufReader::new(lecture);
    let mut ecrivain = BufWriter::new(ecriture);
    let mut ligne = String::new();

    let lue = match timeout(DELAI_REQUETE, lire_ligne_limitee(&mut lecteur, &mut ligne)).await {
        Ok(lue) => lue,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "délai de lecture de la requête dépassé",
        )),
    };
    let reponse = match lue {
        Ok(0) => return Ok(()),
        Ok(_) => match serde_json::from_str::<RequeteIhm>(&ligne) {
            Ok(requete) => traiter(requete, adresse.ip(), partage).await,
            Err(erreur) => echec(format!("Requête invalide : {erreur}")),
        },
        Err(erreur) => echec(format!("Requête illisible : {erreur}")),
    };
    ecrire_message(&mut ecrivain, &reponse).await
}

/// Exécute une requête de l'IHM et construit la réponse.
async fn traiter(requete: RequeteIhm, ip: IpAddr, partage: &Partage) -> ReponseMoteur {
    match requete {
        RequeteIhm::LireEtat { curseur } => {
            partage.enregistrer_lecture(ip);
            let etat = partage
                .instantane
                .lock()
                .expect("instantané verrouillé")
                .etat(curseur);
            ReponseMoteur::Etat(etat)
        }
        RequeteIhm::Demarrer {
            mode,
            date_debut,
            date_fin,
            chemin_fichier,
            symboles_test,
            parametrages,
            fournisseurs,
        } => {
            if let Err(erreur) = parametrages.valider() {
                return echec(format!("Réglages invalides : {erreur}"));
            }
            if fournisseurs.is_empty() {
                return echec("Sélectionnez au moins un flux de données.".to_string());
            }
            let reglages = Some((parametrages.clone(), fournisseurs.clone()));
            let resultat = partage.commander(mode, reglages, |generation| {
                CommandeMoteur::Demarrer {
                    generation,
                    mode,
                    date_debut,
                    date_fin,
                    chemin_fichier,
                    symboles_test,
                    parametrages,
                    fournisseurs,
                }
            });
            resultat_commande(resultat, "Démarrage demandé.")
        }
        RequeteIhm::Arreter { mode } => {
            let resultat =
                partage.commander(mode, None, |generation| CommandeMoteur::Arreter { generation, mode });
            resultat_commande(resultat, "Arrêt demandé.")
        }
        RequeteIhm::SauvegarderParametrages { parametrages } => {
            if let Err(erreur) = parametrages.valider() {
                return echec(format!("Réglages invalides : {erreur}"));
            }
            // Les IHM qui se connectent ensuite reçoivent les réglages sauvegardés.
            partage
                .instantane
                .lock()
                .expect("instantané verrouillé")
                .parametrages = parametrages.clone();
            // Le résultat de l'écriture revient par le journal (Info ou Erreur).
            let envoi = partage
                .tx_moteur
                .send(CommandeMoteur::SauvegarderParametrages { parametrages });
            match envoi {
                Ok(()) => ReponseMoteur::Resultat {
                    succes: true,
                    message: "Sauvegarde demandée.".to_string(),
                    generation: None,
                },
                Err(_) => echec("Le moteur est arrêté.".to_string()),
            }
        }
        RequeteIhm::ChoisirNotifications { notifications } => {
            choisir_notifications(notifications, partage).await
        }
        RequeteIhm::ListerDossier { chemin } => {
            let contenu = tokio::task::spawn_blocking(move || lister_dossier(&chemin))
                .await
                .unwrap_or_else(|erreur| ContenuDossier {
                    erreur: Some(format!("Lecture du dossier interrompue : {erreur}")),
                    ..ContenuDossier::default()
                });
            ReponseMoteur::ContenuDossier(contenu)
        }
        RequeteIhm::FermetureIhm => {
            partage.retirer(ip);
            ReponseMoteur::Resultat {
                succes: true,
                message: "Au revoir.".to_string(),
                generation: None,
            }
        }
    }
}

/// Applique aussitôt le choix des notifications Telegram puis l'enregistre dans `config.toml`.
///
/// Entrées : choix reçu de l'IHM et état partagé.
/// Sortie : succès, ou échec si l'enregistrement est impossible (le choix reste appliqué
/// jusqu'à l'arrêt du moteur).
async fn choisir_notifications(
    notifications: ChoixNotifications,
    partage: &Partage,
) -> ReponseMoteur {
    // Tenu jusqu'à la fin de l'écriture : la dernière demande appliquée est la dernière écrite.
    let _ecriture = partage.ecriture_config.lock().await;
    partage
        .instantane
        .lock()
        .expect("instantané verrouillé")
        .notifications = notifications;
    if let Some(notificateur) = &partage.notificateur {
        notificateur.definir_choix(notifications);
    }
    info!("Notifications Telegram choisies : {notifications:?}");

    let Some(chemin) = partage.chemin_config.clone() else {
        return echec(
            "Choix appliqué mais non enregistré : le moteur utilise sa configuration intégrée \
             (config.toml introuvable)."
                .to_string(),
        );
    };
    let ecriture = tokio::task::spawn_blocking(move || {
        notifications
            .enregistrer(&chemin)
            .map_err(|erreur| erreur.to_string())
    })
    .await
    .unwrap_or_else(|erreur| Err(format!("écriture interrompue : {erreur}")));
    match ecriture {
        Ok(()) => ReponseMoteur::Resultat {
            succes: true,
            message: "Notifications Telegram enregistrées.".to_string(),
            generation: None,
        },
        Err(erreur) => {
            error!("Enregistrement des notifications Telegram impossible : {erreur}");
            echec(format!("Choix appliqué mais non enregistré : {erreur}"))
        }
    }
}

fn echec(message: String) -> ReponseMoteur {
    ReponseMoteur::Resultat {
        succes: false,
        message,
        generation: None,
    }
}

fn resultat_commande(resultat: Result<u64, String>, message: &str) -> ReponseMoteur {
    match resultat {
        Ok(generation) => ReponseMoteur::Resultat {
            succes: true,
            message: message.to_string(),
            generation: Some(generation),
        },
        Err(erreur) => echec(erreur),
    }
}

/// Liste les sous-dossiers et fichiers `.json` d'un dossier de la machine du moteur.
///
/// Entrée : chemin absolu ou relatif au dossier de travail ; vide = dossier de travail.
/// Sortie : contenu trié et plafonné, ou contenu vide avec `erreur` renseignée.
fn lister_dossier(chemin: &str) -> ContenuDossier {
    let demande = if chemin.trim().is_empty() {
        PathBuf::from(".")
    } else {
        PathBuf::from(chemin.trim())
    };
    let absolu = match std::fs::canonicalize(&demande) {
        Ok(absolu) => absolu,
        Err(erreur) => {
            return ContenuDossier {
                chemin: demande.display().to_string(),
                erreur: Some(format!("Dossier inaccessible : {erreur}")),
                ..ContenuDossier::default()
            };
        }
    };
    let mut contenu = ContenuDossier {
        chemin: absolu.display().to_string(),
        parent: absolu.parent().map(|p| p.display().to_string()),
        ..ContenuDossier::default()
    };
    let entrees = match std::fs::read_dir(&absolu) {
        Ok(entrees) => entrees,
        Err(erreur) => {
            contenu.erreur = Some(format!("Lecture impossible : {erreur}"));
            return contenu;
        }
    };
    for entree in entrees.flatten() {
        let nom = entree.file_name().to_string_lossy().into_owned();
        if nom.starts_with('.') {
            continue;
        }
        // `metadata` suit les liens symboliques.
        let Ok(meta) = std::fs::metadata(entree.path()) else {
            continue;
        };
        if meta.is_dir() {
            contenu.dossiers.push(nom);
        } else if est_json(&entree.path()) {
            contenu.fichiers_json.push(nom);
        }
    }
    contenu.dossiers.sort_unstable_by_key(|n| n.to_lowercase());
    contenu.fichiers_json.sort_unstable_by_key(|n| n.to_lowercase());
    let total = contenu.dossiers.len() + contenu.fichiers_json.len();
    if total > ENTREES_DOSSIER_MAX {
        contenu.tronque = true;
        contenu.dossiers.truncate(ENTREES_DOSSIER_MAX);
        let reste = ENTREES_DOSSIER_MAX - contenu.dossiers.len();
        contenu.fichiers_json.truncate(reste);
    }
    contenu
}

fn est_json(chemin: &Path) -> bool {
    chemin
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"))
}

/// Affiche une notification du moteur dans la console.
fn afficher_notification(notification: &NotificationMoteur) {
    match notification {
        NotificationMoteur::Statut(texte) | NotificationMoteur::Info(texte) => println!("{texte}"),
        NotificationMoteur::Erreur(texte) => eprintln!("Erreur de source : {texte}"),
        NotificationMoteur::Echec(texte) => eprintln!("Erreur : {texte}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::echanger;
    use crate::core::modeles::Tendance;

    fn initial() -> EtatInitial {
        let config =
            crate::config::ConfigurationApplication::depuis_toml(include_str!("../config.toml"))
                .unwrap();
        EtatInitial {
            symboles: config.paires.symboles.clone(),
            parametrages: config.parametrages(),
            fournisseurs: config.sources_donnees.fournisseurs,
            mode: ModeExecution::TempsReel,
            notifications: ChoixNotifications::default(),
            telegram_configure: false,
        }
    }

    fn signal(generation: u64, symbole: &str, alerte: bool) -> SignalVisuel {
        SignalVisuel {
            generation,
            symbole: symbole.to_string(),
            prix: 1.0,
            prix_precedent: 1.0,
            score: 90,
            adx: 30.0,
            adx_valide: true,
            ratio_volume: 1.5,
            volume_valide: true,
            tendance: Tendance::Haussiere,
            est_alerte: alerte,
            unite: "1h".to_string(),
            horodatage: "2026-01-01 10:00".to_string(),
        }
    }

    #[test]
    fn journal_pagine_et_vide_a_chaque_generation() {
        let mut instantane = Instantane::new(initial());
        for _ in 0..(EVENEMENTS_PAR_REPONSE + 3) {
            instantane.recevoir_signal(signal(0, "BTCEUR", true));
        }
        // Un signal d'une autre génération est ignoré.
        assert!(!instantane.recevoir_signal(signal(9, "ETHEUR", true)));

        let premier = instantane.etat(0);
        assert_eq!(premier.evenements.len(), EVENEMENTS_PAR_REPONSE);
        assert!(premier.suite);
        let curseur = premier.evenements.last().unwrap().seq;
        let second = instantane.etat(curseur);
        assert_eq!(second.evenements.len(), 3);
        assert!(!second.suite);
        assert_eq!(second.paires.len(), 1);

        let seq_avant = instantane.prochaine_seq;
        assert_eq!(instantane.nouvelle_generation(ModeExecution::Simulation), 1);
        let apres = instantane.etat(0);
        assert!(apres.evenements.is_empty());
        assert!(apres.paires.is_empty());
        assert_eq!(apres.mode, ModeExecution::Simulation);
        // Les numéros de séquence continuent de croître.
        instantane.recevoir_notification(1, NotificationMoteur::Erreur("x".to_string()));
        assert_eq!(instantane.etat(0).evenements[0].seq, seq_avant);
    }

    #[test]
    fn journal_plafonne() {
        let mut instantane = Instantane::new(initial());
        for _ in 0..(JOURNAL_MAX + 10) {
            instantane.journaliser(ContenuEvenement::Info("i".to_string()));
        }
        assert_eq!(instantane.journal.len(), JOURNAL_MAX);
        assert_eq!(instantane.journal.front().unwrap().seq, 11);
    }

    #[test]
    fn compte_les_messages_seulement_flux_ouvert() {
        let mut instantane = Instantane::new(initial());
        instantane.recevoir_signal(signal(0, "BTCEUR", false));
        assert_eq!(instantane.nb_messages, 0);
        instantane.recevoir_notification(0, NotificationMoteur::Statut("🔴 Flux".to_string()));
        instantane.recevoir_signal(signal(0, "BTCEUR", false));
        assert_eq!(instantane.nb_messages, 1);
        assert!(instantane.etat(0).dernier_message_il_y_a_ms.is_some());
    }

    #[test]
    fn liste_les_dossiers_et_fichiers_json() {
        let contenu = lister_dossier(env!("CARGO_MANIFEST_DIR"));
        assert!(contenu.erreur.is_none());
        assert!(contenu.dossiers.iter().any(|d| d == "src"));
        assert!(contenu.fichiers_json.iter().all(|f| f.ends_with(".json")));
        assert!(contenu.parent.is_some());
        let absent = lister_dossier("/dossier/qui/n/existe/pas");
        assert!(absent.erreur.is_some());
    }

    #[tokio::test]
    async fn repond_aux_requetes_de_l_ihm() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let adresse = listener.local_addr().unwrap().to_string();
        let (tx_moteur, mut rx_moteur) = mpsc::unbounded_channel();
        let (tx_signaux, rx_signaux) = mpsc::channel(8);
        let (tx_statut, rx_statut) = mpsc::unbounded_channel();
        let (tx_arret, rx_arret) = tokio::sync::oneshot::channel::<()>();
        let chemin_config = std::env::temp_dir().join(format!(
            "predict-test-config-{}.toml",
            std::process::id()
        ));
        std::fs::write(&chemin_config, include_str!("../config.toml")).unwrap();
        let serveur = tokio::spawn(servir(
            listener,
            initial(),
            tx_moteur,
            rx_signaux,
            rx_statut,
            OptionsServeur {
                intervalle_ihm: Duration::from_secs(1),
                notificateur: None,
                presence_ihm: false,
                chemin_config: Some(chemin_config.clone()),
                console: false,
            },
            async {
                let _ = rx_arret.await;
            },
        ));

        let ReponseMoteur::Etat(etat) = echanger(&adresse, &RequeteIhm::LireEtat { curseur: 0 })
            .await
            .unwrap()
        else {
            panic!("état attendu");
        };
        assert_eq!(etat.generation, 0);
        assert!(etat.notifications.retournements);

        let choix = ChoixNotifications {
            retournements: false,
            presence_ihm: false,
            ..ChoixNotifications::default()
        };
        let reponse = echanger(
            &adresse,
            &RequeteIhm::ChoisirNotifications {
                notifications: choix,
            },
        )
        .await
        .unwrap();
        assert!(matches!(reponse, ReponseMoteur::Resultat { succes: true, .. }));
        let ReponseMoteur::Etat(etat) = echanger(&adresse, &RequeteIhm::LireEtat { curseur: 0 })
            .await
            .unwrap()
        else {
            panic!("état attendu");
        };
        assert_eq!(etat.notifications, choix);
        let relue = crate::config::ConfigurationApplication::charger_config(&chemin_config).unwrap();
        assert_eq!(relue.telegram.notifications, choix);
        std::fs::remove_file(&chemin_config).unwrap();

        let reponse = echanger(
            &adresse,
            &RequeteIhm::Arreter {
                mode: ModeExecution::BacktestFichier,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            reponse,
            ReponseMoteur::Resultat { succes: true, generation: Some(1), .. }
        ));
        assert!(matches!(
            rx_moteur.recv().await.unwrap(),
            CommandeMoteur::Arreter { generation: 1, mode: ModeExecution::BacktestFichier }
        ));

        tx_signaux.send(signal(1, "BTCEUR", true)).await.unwrap();
        tx_statut
            .send((1, NotificationMoteur::Statut("Analyse".to_string())))
            .unwrap();
        // Laisse la boucle du serveur intégrer le signal et le statut.
        let mut etat = None;
        for _ in 0..50 {
            let ReponseMoteur::Etat(lu) = echanger(&adresse, &RequeteIhm::LireEtat { curseur: 0 })
                .await
                .unwrap()
            else {
                panic!("état attendu");
            };
            if lu.statut.is_some() && !lu.evenements.is_empty() {
                etat = Some(lu);
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let etat = etat.expect("signal et statut intégrés");
        assert_eq!(etat.generation, 1);
        assert_eq!(etat.mode, ModeExecution::BacktestFichier);
        assert_eq!(etat.paires.len(), 1);
        assert!(matches!(etat.evenements[0].contenu, ContenuEvenement::Alerte(_)));

        let invalide = {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut flux = TcpStream::connect(&adresse).await.unwrap();
            flux.write_all(b"pas du json\n").await.unwrap();
            let mut texte = String::new();
            flux.read_to_string(&mut texte).await.unwrap();
            serde_json::from_str::<ReponseMoteur>(texte.trim()).unwrap()
        };
        assert!(matches!(invalide, ReponseMoteur::Resultat { succes: false, .. }));

        let _ = tx_arret.send(());
        serveur.await.unwrap().unwrap();
    }
}
