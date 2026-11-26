use crate::client::{ClientMoteur, RetourReseau};
use crate::config::{ChoixNotifications, ConfigurationStrategie, Parametrages, Portee};
use crate::core::modeles::Tendance;
use crate::data::sources::Fournisseur;
use crate::protocole::{
    ContenuDossier, ContenuEvenement, EtatMoteur, ReponseMoteur, RequeteIhm, RessourcesServeur,
};
use chrono::{Datelike, Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

/// Alertes conservées dans le journal affiché.
const JOURNAL_ALERTES_MAX: usize = 5000;

#[derive(Debug, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum ModeExecution {
    TempsReel,
    Simulation,
    BacktestPeriode,
    BacktestFichier,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NotificationMoteur {
    Statut(String),
    /// Message ponctuel (ex. sauvegarde) qui ne remplace pas le statut du mode en cours.
    Info(String),
    Erreur(String),
    Echec(String),
}

/// Ordre envoyé par l'IHM au moteur Tokio.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CommandeMoteur {
    Arreter {
        generation: u64,
        mode: ModeExecution,
    },
    SauvegarderParametrages {
        parametrages: Parametrages,
    },
    Demarrer {
        generation: u64,
        mode: ModeExecution,
        date_debut: String,
        date_fin: String,
        chemin_fichier: String,
        symboles_test: Vec<String>,
        parametrages: Parametrages,
        fournisseurs: Vec<Fournisseur>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignalVisuel {
    /// Numéro de session : les signaux d'un ancien mode sont ignorés par l'IHM.
    pub generation: u64,
    pub symbole: String,
    pub prix: f64,
    /// Clôture de la bougie précédente, référence de la couleur du prix.
    pub prix_precedent: f64,
    pub score: u8,
    pub adx: f64,
    pub adx_valide: bool,
    pub ratio_volume: f64,
    pub volume_valide: bool,
    pub tendance: Tendance,
    pub est_alerte: bool,
    pub unite: String,
    pub horodatage: String,
}

/// Intervalles de bougies acceptés par Binance.
const UNITES_TEMPS: &[&str] = &[
    "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
];

/// Prix à 2 décimales avec les milliers séparés par une espace (ex. `12 345.67`).
///
/// Entrée : prix numérique.
/// Sortie : prix formaté avec deux décimales.
fn formater_prix(prix: f64) -> String {
    let texte = format!("{prix:.2}");
    let (signe, reste) = texte
        .strip_prefix('-')
        .map_or(("", texte.as_str()), |r| ("-", r));
    let (entier, decimales) = reste.split_once('.').unwrap_or((reste, ""));
    let mut groupes = String::new();
    for (i, c) in entier.chars().enumerate() {
        if i > 0 && (entier.len() - i) % 3 == 0 {
            groupes.push(' ');
        }
        groupes.push(c);
    }
    format!("{signe}{groupes}.{decimales}")
}

/// Ajoute un séparateur lisible entre une paire et sa devise de cotation connue.
///
/// Entrée : symbole sans séparateur.
/// Sortie : paire formatée, ou symbole inchangé si aucune devise fiat ne correspond.
fn formater_paire(symbole: &str) -> String {
    const DEVISES_FIAT: &[&str] = &[
        "AED", "ARS", "AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "CZK", "DKK", "EUR", "GBP", "HKD",
        "HUF", "IDR", "ILS", "INR", "JPY", "KRW", "MXN", "MYR", "NOK", "NZD", "PHP", "PLN", "RON",
        "RUB", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "UAH", "USD", "VND", "ZAR",
    ];

    DEVISES_FIAT
        .iter()
        .find_map(|devise| {
            symbole
                .strip_suffix(devise)
                .filter(|crypto| !crypto.is_empty())
                .map(|crypto| format!("{crypto} / {devise}"))
        })
        .unwrap_or_else(|| symbole.to_string())
}

/// Affiche un calendrier de sélection et met à jour la date choisie.
///
/// Entrées : contexte egui, titre/identifiant, état d'ouverture, mois affiché et valeur de date.
/// Sortie : met à jour la date au format `AAAA-MM-JJ` et ferme le calendrier après sélection.
fn afficher_calendrier(
    ctx: &egui::Context,
    titre: &str,
    id: &str,
    ouvert: &mut bool,
    mois: &mut NaiveDate,
    valeur: &mut String,
) {
    if !*ouvert {
        return;
    }

    let mut date_choisie = None;
    egui::Window::new(titre)
        .id(egui::Id::new(id))
        .open(ouvert)
        .resizable(false)
        .show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("‹").clicked() {
                    *mois = if mois.month() == 1 {
                        NaiveDate::from_ymd_opt(mois.year() - 1, 12, 1).unwrap()
                    } else {
                        NaiveDate::from_ymd_opt(mois.year(), mois.month() - 1, 1).unwrap()
                    };
                }
                let noms_mois = [
                    "janvier",
                    "février",
                    "mars",
                    "avril",
                    "mai",
                    "juin",
                    "juillet",
                    "août",
                    "septembre",
                    "octobre",
                    "novembre",
                    "décembre",
                ];
                ui.label(format!(
                    "{} {}",
                    noms_mois[mois.month0() as usize],
                    mois.year()
                ));
                if ui.button("›").clicked() {
                    *mois = if mois.month() == 12 {
                        NaiveDate::from_ymd_opt(mois.year() + 1, 1, 1).unwrap()
                    } else {
                        NaiveDate::from_ymd_opt(mois.year(), mois.month() + 1, 1).unwrap()
                    };
                }
            });

            let premier_jour = NaiveDate::from_ymd_opt(mois.year(), mois.month(), 1).unwrap();
            let premier_jour_mois_suivant = if mois.month() == 12 {
                NaiveDate::from_ymd_opt(mois.year() + 1, 1, 1).unwrap()
            } else {
                NaiveDate::from_ymd_opt(mois.year(), mois.month() + 1, 1).unwrap()
            };
            let dernier_jour = premier_jour_mois_suivant - Duration::days(1);
            let selection_actuelle = NaiveDate::parse_from_str(valeur, "%Y-%m-%d").ok();
            egui::Grid::new(id).num_columns(7).show(ui, |ui| {
                for jour in ["Lu", "Ma", "Me", "Je", "Ve", "Sa", "Di"] {
                    ui.strong(jour);
                }
                ui.end_row();

                let decalage = premier_jour.weekday().num_days_from_monday();
                for _ in 0..decalage {
                    ui.label("");
                }
                for jour in 1..=dernier_jour.day() {
                    let date = NaiveDate::from_ymd_opt(mois.year(), mois.month(), jour).unwrap();
                    let bouton = egui::Button::new(jour.to_string())
                        .selected(selection_actuelle == Some(date));
                    if ui.add(bouton).clicked() {
                        date_choisie = Some(date);
                    }
                    if date.weekday().num_days_from_monday() == 6 {
                        ui.end_row();
                    }
                }
            });
        });

    if let Some(date) = date_choisie {
        *valeur = date.format("%Y-%m-%d").to_string();
        *ouvert = false;
    }
}

/// Fenêtre de parcours des dossiers de la machine du moteur.
struct Navigateur {
    contenu: Option<ContenuDossier>,
    en_cours: bool,
}

pub struct ApplicationCryptobot {
    /// Canaux vers le thread réseau.
    client: ClientMoteur,
    /// Dernier signal de chaque paire, triées par symbole.
    pub paires: BTreeMap<String, SignalVisuel>,
    pub journal_alertes: Vec<SignalVisuel>,

    // Nouveaux champs pour le choix du mode et le calibrage
    pub mode_actif: ModeExecution,
    pub date_debut: String,
    pub date_fin: String,
    date_debut_mois: NaiveDate,
    date_fin_mois: NaiveDate,
    calendrier_debut_ouvert: bool,
    calendrier_fin_ouvert: bool,
    pub chemin_fichier_test: String,
    /// Paramètres de calcul modifiables ; envoyés au moteur à chaque (re)lancement.
    pub parametrages: Parametrages,
    fournisseurs: Vec<Fournisseur>,
    /// Paramétrages tels qu'ils ont été sauvegardés pour la dernière fois.
    parametrages_sauvegardes: Parametrages,
    /// Portée (défaut ou paire) actuellement éditée dans la barre de paramètres.
    portee: Portee,
    /// Réglage copié, prêt à être collé sur une paire (libellé de la source, réglage).
    presse_papiers: Option<(String, ConfigurationStrategie)>,
    /// Fenêtre « Copier vers… » ouverte, avec les paires cochées.
    copie_vers: Option<HashSet<String>>,
    /// Dernier message concernant les réglages (copie, sauvegarde…).
    message_reglages: String,
    symboles_test_disponibles: Vec<String>,
    symboles_test: HashSet<String>,
    /// Instance du moteur connue (`None` tant qu'aucun état n'a été reçu).
    instance: Option<u64>,
    /// Génération affichée, attribuée par le moteur.
    generation: Option<u64>,
    /// Les états de génération inférieure sont ignorés (session remplacée par l'IHM).
    generation_min: u64,
    /// Vrai si la dernière lecture de l'état a réussi.
    connecte: bool,
    erreur_reseau: Option<String>,
    /// Dernier statut reçu du moteur, pour ne reprendre que ses changements.
    dernier_statut_moteur: Option<String>,
    pub statut: String,
    /// Alerte cliquée dans le journal : sa paire affiche les données de cette alerte.
    pub selection: Option<SignalVisuel>,
    /// Messages reçus du flux en direct depuis le lancement de la session.
    nb_messages: u64,
    dernier_message: Option<Instant>,
    /// Dernière occupation CPU/mémoire publiée par le serveur.
    ressources: Option<RessourcesServeur>,
    navigateur: Option<Navigateur>,
    erreur_dialogue: Option<String>,
    /// Notifications Telegram cochées dans l'IHM.
    notifications: ChoixNotifications,
    /// Dernier choix publié par le moteur, pour ne reprendre que ses changements (une
    /// lecture partie avant l'envoi d'un nouveau choix ne doit pas le décocher).
    notifications_moteur: Option<ChoixNotifications>,
    /// Vrai si le moteur a un jeton et un chat Telegram.
    telegram_configure: bool,
}

/// Liste triée des paires proposées : celles suivies et celles qui ont un réglage propre.
fn symboles_disponibles(symboles: &[String], parametrages: &Parametrages) -> Vec<String> {
    let mut liste: Vec<String> = symboles
        .iter()
        .chain(parametrages.paires.keys())
        .map(|s| s.to_uppercase())
        .collect();
    liste.sort_unstable();
    liste.dedup();
    liste
}

/// Dossier parent d'un chemin saisi, quel que soit le séparateur ; vide s'il n'y en a pas.
fn dossier_de(chemin: &str) -> String {
    match chemin.rfind(['/', '\\']) {
        Some(0) => chemin[..1].to_string(),
        Some(index) => chemin[..index].to_string(),
        None => String::new(),
    }
}

/// Joint un nom à un dossier du moteur avec le séparateur que ce dossier utilise.
fn joindre(dossier: &str, nom: &str) -> String {
    let separateur = if dossier.contains('\\') && !dossier.contains('/') {
        '\\'
    } else {
        '/'
    };
    if dossier.ends_with(separateur) {
        format!("{dossier}{nom}")
    } else {
        format!("{dossier}{separateur}{nom}")
    }
}

impl ApplicationCryptobot {
    /// Construit l'IHM ; les réglages fournis servent jusqu'au premier état reçu du moteur.
    ///
    /// Entrées : paramétrages, fournisseurs et symboles par défaut, canaux du thread réseau.
    /// Sortie : application initialisée, en attente du premier état du moteur.
    pub fn new(
        parametrages: Parametrages,
        fournisseurs: Vec<Fournisseur>,
        symboles: Vec<String>,
        client: ClientMoteur,
    ) -> Self {
        Self {
            parametrages_sauvegardes: parametrages.clone(),
            symboles_test: symboles.iter().map(|s| s.to_uppercase()).collect(),
            symboles_test_disponibles: symboles_disponibles(&symboles, &parametrages),
            parametrages,
            fournisseurs,
            portee: Portee::Generique,
            presse_papiers: None,
            copie_vers: None,
            message_reglages: String::new(),
            client,
            paires: BTreeMap::new(),
            journal_alertes: Vec::new(),
            mode_actif: ModeExecution::TempsReel,
            date_debut: "2026-01-01".to_string(),
            date_fin: "2026-02-01".to_string(),
            date_debut_mois: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            date_fin_mois: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
            calendrier_debut_ouvert: false,
            calendrier_fin_ouvert: false,
            chemin_fichier_test: "historique.json".to_string(),
            instance: None,
            generation: None,
            generation_min: 0,
            connecte: false,
            erreur_reseau: None,
            dernier_statut_moteur: None,
            statut: String::new(),
            selection: None,
            nb_messages: 0,
            dernier_message: None,
            ressources: None,
            navigateur: None,
            erreur_dialogue: None,
            notifications: ChoixNotifications::default(),
            notifications_moteur: None,
            telegram_configure: false,
        }
    }

    /// Vide le canal des retours du thread réseau et met l'état à jour.
    fn recevoir(&mut self) {
        while let Ok(retour) = self.client.retours.try_recv() {
            match retour {
                RetourReseau::Etat(Ok(etat)) => self.appliquer_etat(etat),
                RetourReseau::Etat(Err(erreur)) => {
                    self.connecte = false;
                    self.erreur_reseau = Some(erreur);
                }
                RetourReseau::Reponse(Ok(reponse)) => self.appliquer_reponse(reponse),
                RetourReseau::Reponse(Err(erreur)) => {
                    // La commande n'a pas abouti : la session affichée reste valable.
                    self.generation_min = 0;
                    if let Some(navigateur) = &mut self.navigateur {
                        navigateur.en_cours = false;
                    }
                    self.ajouter_erreur(erreur);
                }
            }
        }
    }

    /// Remplace l'affichage par l'instantané reçu du moteur et intègre ses nouveaux événements.
    fn appliquer_etat(&mut self, etat: EtatMoteur) {
        self.connecte = true;
        self.erreur_reseau = None;
        self.ressources = etat.ressources;
        self.telegram_configure = etat.telegram_configure;
        if self.notifications_moteur != Some(etat.notifications) {
            self.notifications = etat.notifications;
            self.notifications_moteur = Some(etat.notifications);
        }

        if self.instance != Some(etat.instance) {
            if self.instance.is_none() {
                // Premier contact : les réglages du moteur remplacent ceux par défaut.
                self.symboles_test_disponibles =
                    symboles_disponibles(&etat.symboles, &etat.parametrages);
                self.symboles_test = etat.symboles.iter().map(|s| s.to_uppercase()).collect();
                self.parametrages_sauvegardes = etat.parametrages.clone();
                self.parametrages = etat.parametrages.clone();
            }
            self.instance = Some(etat.instance);
            self.generation = None;
            self.generation_min = 0;
        }
        if etat.generation < self.generation_min {
            return; // ancienne session, remplacée par une commande de cette IHM
        }
        if self.generation != Some(etat.generation) {
            // Nouvelle session (lancée par cette IHM ou par une autre).
            self.generation = Some(etat.generation);
            self.mode_actif = etat.mode;
            self.fournisseurs = etat.fournisseurs;
            self.journal_alertes.clear();
            self.selection = None;
            self.dernier_statut_moteur = None;
        }

        self.paires = etat
            .paires
            .into_iter()
            .map(|signal| (signal.symbole.clone(), signal))
            .collect();
        self.nb_messages = etat.nb_messages;
        self.dernier_message = etat.dernier_message_il_y_a_ms.and_then(|ms| {
            Instant::now().checked_sub(std::time::Duration::from_millis(ms))
        });
        if etat.statut != self.dernier_statut_moteur {
            if let Some(statut) = &etat.statut {
                self.statut = statut.clone();
            }
            self.dernier_statut_moteur = etat.statut;
        }
        for evenement in etat.evenements {
            match evenement.contenu {
                ContenuEvenement::Alerte(signal) => {
                    self.journal_alertes.insert(0, signal);
                    self.journal_alertes.truncate(JOURNAL_ALERTES_MAX);
                }
                ContenuEvenement::Info(texte) => self.message_reglages = texte,
                ContenuEvenement::Erreur(texte) | ContenuEvenement::Echec(texte) => {
                    self.ajouter_erreur(texte);
                }
            }
        }
    }

    /// Traite la réponse du moteur à une requête de l'IHM.
    fn appliquer_reponse(&mut self, reponse: ReponseMoteur) {
        match reponse {
            ReponseMoteur::Resultat {
                succes: true,
                generation: Some(generation),
                ..
            } => self.generation_min = generation,
            ReponseMoteur::Resultat { succes: true, .. } => {}
            ReponseMoteur::Resultat {
                succes: false,
                message,
                ..
            } => {
                self.generation_min = 0;
                self.ajouter_erreur(message);
            }
            ReponseMoteur::ContenuDossier(contenu) => {
                if let Some(navigateur) = &mut self.navigateur {
                    navigateur.contenu = Some(contenu);
                    navigateur.en_cours = false;
                }
            }
            ReponseMoteur::Etat(_) => {}
        }
    }

    /// Ajoute un message à la fenêtre d'erreur (ouverte si besoin).
    fn ajouter_erreur(&mut self, texte: String) {
        if let Some(erreur) = &mut self.erreur_dialogue {
            erreur.push('\n');
            erreur.push_str(&texte);
        } else {
            self.erreur_dialogue = Some(texte);
        }
    }

    /// Efface toutes les valeurs affichées et invalide les signaux encore en transit.
    ///
    /// Entrée : état courant de l'application.
    /// Sortie : incrémente la génération et vide les données d'affichage.
    fn reinitialiser(&mut self) {
        // Les états de la session courante qui arrivent encore sont ignorés.
        self.generation_min = self.generation.map_or(0, |g| g + 1);
        self.paires.clear();
        self.journal_alertes.clear();
        self.selection = None;
        self.statut.clear();
        self.nb_messages = 0;
        self.dernier_message = None;
    }

    /// Transmet une requête au thread réseau.
    ///
    /// Entrée : requête pour le moteur.
    /// Sortie : `true` si elle part ; sinon le statut signale que le moteur est injoignable.
    fn envoyer(&mut self, requete: RequeteIhm) -> bool {
        if !self.connecte {
            self.statut = "Moteur injoignable : commande non envoyée.".to_string();
            return false;
        }
        if self.client.requetes.send(requete).is_err() {
            self.statut = "Thread réseau arrêté : redémarrez l'IHM.".to_string();
            return false;
        }
        true
    }

    /// Menu de choix des notifications Telegram ; chaque changement est envoyé au moteur, qui
    /// l'applique et l'enregistre dans son `config.toml`.
    fn menu_notifications(&mut self, ui: &mut egui::Ui) {
        ui.menu_button("🔔 Telegram", |ui| {
            if !self.telegram_configure {
                ui.label(
                    egui::RichText::new(
                        "Telegram non configuré sur le moteur :\nle choix est enregistré, rien n'est envoyé.",
                    )
                    .weak(),
                );
                ui.separator();
            }
            let mut choix = self.notifications;
            let mut modifie = false;
            for (coche, libelle) in [
                (&mut choix.demarrage, "🚀 Démarrage du moteur"),
                (&mut choix.arret, "🛑 Arrêt du moteur"),
                (&mut choix.presence_ihm, "🖥 Connexion / déconnexion des IHM"),
                (&mut choix.retournements, "🔄 Retournements de tendance"),
            ] {
                modifie |= ui.checkbox(coche, libelle).changed();
            }
            if modifie
                && self.envoyer(RequeteIhm::ChoisirNotifications {
                    notifications: choix,
                })
            {
                self.notifications = choix;
            }
        });
    }

    /// Ouvre (ou met à jour) le navigateur sur un dossier de la machine du moteur.
    ///
    /// Entrée : chemin du dossier ; vide = dossier de travail du moteur.
    fn parcourir(&mut self, chemin: String) {
        let navigateur = self.navigateur.get_or_insert(Navigateur {
            contenu: None,
            en_cours: false,
        });
        navigateur.en_cours = true;
        if !self.envoyer(RequeteIhm::ListerDossier { chemin })
            && let Some(navigateur) = &mut self.navigateur
        {
            navigateur.en_cours = false;
        }
    }

    /// Affiche la fenêtre de choix d'un fichier `.json` sur la machine du moteur.
    ///
    /// Entrée : contexte egui.
    /// Sortie : met à jour le chemin du fichier de test ou demande un autre dossier.
    fn afficher_navigateur(&mut self, ctx: &egui::Context) {
        let Some(navigateur) = &self.navigateur else {
            return;
        };
        let mut ouverte = true;
        let mut aller_vers: Option<String> = None;
        let mut fichier_choisi: Option<String> = None;
        egui::Window::new("Fichiers du moteur")
            .id(egui::Id::new("navigateur_moteur"))
            .open(&mut ouverte)
            .collapsible(false)
            .default_size([520.0, 400.0])
            .show(ctx, |ui| {
                let contenu = navigateur.contenu.as_ref();
                ui.horizontal(|ui| {
                    let parent = contenu.and_then(|c| c.parent.clone());
                    if ui
                        .add_enabled(parent.is_some(), egui::Button::new("⬆ Parent"))
                        .clicked()
                    {
                        aller_vers = parent;
                    }
                    if ui
                        .button("🏠 Dossier du moteur")
                        .on_hover_text("Dossier de travail du moteur")
                        .clicked()
                    {
                        aller_vers = Some(String::new());
                    }
                    if navigateur.en_cours {
                        ui.spinner();
                    }
                });
                let Some(contenu) = contenu else {
                    ui.label("Lecture du dossier…");
                    return;
                };
                ui.label(egui::RichText::new(&contenu.chemin).monospace());
                if let Some(erreur) = &contenu.erreur {
                    ui.label(egui::RichText::new(erreur).color(egui::Color32::LIGHT_RED));
                }
                ui.separator();
                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        for dossier in &contenu.dossiers {
                            if ui.selectable_label(false, format!("📁 {dossier}")).clicked() {
                                aller_vers = Some(joindre(&contenu.chemin, dossier));
                            }
                        }
                        for fichier in &contenu.fichiers_json {
                            if ui.selectable_label(false, format!("📄 {fichier}")).clicked() {
                                fichier_choisi = Some(joindre(&contenu.chemin, fichier));
                            }
                        }
                        if contenu.erreur.is_none()
                            && contenu.dossiers.is_empty()
                            && contenu.fichiers_json.is_empty()
                        {
                            ui.label("Aucun sous-dossier ni fichier .json.");
                        }
                        if contenu.tronque {
                            ui.label(
                                egui::RichText::new("Liste tronquée.").color(egui::Color32::GRAY),
                            );
                        }
                    });
            });
        if let Some(chemin) = fichier_choisi {
            self.chemin_fichier_test = chemin;
            self.navigateur = None;
        } else if !ouverte {
            self.navigateur = None;
        } else if let Some(chemin) = aller_vers {
            self.parcourir(chemin);
        }
    }

    /// Change de mode : tout est réinitialisé ; temps réel et simulation démarrent aussitôt,
    /// les modes de test attendent le clic sur « Charger & Lancer ».
    ///
    /// Entrée : mode sélectionné dans l'IHM.
    /// Sortie : réinitialise les valeurs et démarre le mode ou demande l'arrêt du moteur.
    fn changer_de_mode(&mut self) {
        self.reinitialiser();
        match self.mode_actif {
            ModeExecution::TempsReel | ModeExecution::Simulation => self.lancer(),
            ModeExecution::BacktestPeriode | ModeExecution::BacktestFichier => {
                let _ = self.envoyer(RequeteIhm::Arreter {
                    mode: self.mode_actif,
                });
                self.statut =
                    "Renseignez les paramètres puis cliquez sur « Charger & Lancer ».".to_string();
            }
        }
    }

    /// Envoie au moteur une commande de démarrage avec la sélection et les réglages actuels.
    ///
    /// Entrée : état courant de l'IHM.
    /// Sortie : transmet une commande de démarrage via le canal configuré.
    fn lancer(&mut self) {
        let _ = self.envoyer(RequeteIhm::Demarrer {
            mode: self.mode_actif,
            date_debut: self.date_debut.clone(),
            date_fin: self.date_fin.clone(),
            chemin_fichier: self.chemin_fichier_test.clone(),
            symboles_test: self
                .symboles_test_disponibles
                .iter()
                .filter(|s| self.symboles_test.contains(&s.to_uppercase()))
                .cloned()
                .collect(),
            parametrages: self.parametrages.clone(),
            fournisseurs: self.fournisseurs.clone(),
        });
    }

    /// Exécute une action sur les réglages (copie, collage, retour au défaut, sauvegarde).
    ///
    /// Entrée : action demandée depuis la barre de paramètres ou une carte de paire.
    /// Sortie : met à jour les paramétrages, le presse-papiers et le message affiché.
    fn executer_action_reglages(&mut self, action: ActionReglages) {
        match action {
            ActionReglages::Copier(portee) => {
                let libelle = libelle_portee(&portee);
                self.presse_papiers = Some((libelle.clone(), self.parametrages.lire(&portee)));
                self.message_reglages = format!("📋 Réglages « {libelle} » copiés.");
            }
            ActionReglages::Coller(portee) => {
                if let Some((source, reglage)) = self.presse_papiers.clone() {
                    self.parametrages.ecrire(&portee, reglage);
                    self.message_reglages = format!(
                        "📥 Réglages « {source} » collés sur « {} ».",
                        libelle_portee(&portee)
                    );
                }
            }
            ActionReglages::CopierVers(source, cibles) => {
                self.parametrages.copier(&source, &cibles);
                self.message_reglages = format!(
                    "⇉ Réglages « {} » copiés vers {} paire(s).",
                    libelle_portee(&source),
                    cibles.len()
                );
            }
            ActionReglages::RevenirAuDefaut(portee) => {
                self.parametrages.reinitialiser(&portee);
                self.message_reglages = format!(
                    "↺ « {} » suit de nouveau le défaut.",
                    libelle_portee(&portee)
                );
            }
            ActionReglages::Sauvegarder => {
                if let Err(erreur) = self.parametrages.valider() {
                    self.erreur_dialogue = Some(format!("Réglages invalides : {erreur}"));
                    return;
                }
                let envoyee = self.envoyer(RequeteIhm::SauvegarderParametrages {
                    parametrages: self.parametrages.clone(),
                });
                if envoyee {
                    self.message_reglages = "Sauvegarde en cours...".to_string();
                    self.parametrages_sauvegardes = self.parametrages.clone();
                }
            }
        }
    }
}

/// Action demandée sur les réglages des indicateurs.
enum ActionReglages {
    Copier(Portee),
    Coller(Portee),
    CopierVers(Portee, Vec<String>),
    RevenirAuDefaut(Portee),
    Sauvegarder,
}

/// Libellé affiché pour une portée de réglage.
///
/// Entrée : portée.
/// Sortie : « Défaut » ou le symbole de la paire.
fn libelle_portee(portee: &Portee) -> String {
    match portee {
        Portee::Generique => "Défaut".to_string(),
        Portee::Paire(symbole) => symbole.clone(),
    }
}

/// Champs d'édition des indicateurs d'un paramétrage (utilisés par la barre et par chaque carte).
///
/// Entrées : interface egui, stratégie modifiable et choix de disposition.
/// Sortie : affiche les contrôles et met à jour directement la stratégie.
fn editeur_indicateurs(
    ui: &mut egui::Ui,
    s: &mut ConfigurationStrategie,
    disposition_verticale: bool,
) {
    if disposition_verticale {
        ui.label("Période");
        egui::ComboBox::from_id_salt(ui.id().with("unite_temps"))
            .selected_text(s.unite_temps.as_str())
            .width(60.0)
            .show_ui(ui, |ui| {
                for u in UNITES_TEMPS {
                    ui.selectable_value(&mut s.unite_temps, u.to_string(), *u);
                }
            });
        ui.label("EMA période");
        ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_ema).range(2..=1000));
        ui.checkbox(&mut s.filtre_macro.utiliser_supertrend, "Supertrend");
        if s.filtre_macro.utiliser_supertrend {
            ui.label("ATR période");
            ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_supertrend).range(1..=100));
            ui.label("Supertrend multiplicateur");
            ui.add(
                egui::DragValue::new(&mut s.filtre_macro.multiplicateur_supertrend)
                    .range(0.1..=10.0)
                    .speed(0.05),
            );
        }
        ui.label("ADX période");
        ui.add(egui::DragValue::new(&mut s.adx.periode).range(2..=100));
        ui.label("ADX seuil");
        ui.add(
            egui::DragValue::new(&mut s.adx.seuil_minimal)
                .range(1.0..=100.0)
                .speed(0.5),
        );
        ui.label("Volume SMA (bougies)");
        ui.add(egui::DragValue::new(&mut s.volume.periode_moyenne_mobile).range(2..=500));
        ui.label("Multiplicateur volume");
        ui.add(
            egui::DragValue::new(&mut s.volume.multiplicateur)
                .range(0.1..=10.0)
                .speed(0.05)
                .suffix("×"),
        );
        return;
    }

    if ui.available_width() < 500.0 {
        ui.horizontal(|ui| {
            ui.label("Période");
            egui::ComboBox::from_id_salt(ui.id().with("unite_temps"))
                .selected_text(s.unite_temps.as_str())
                .width(60.0)
                .show_ui(ui, |ui| {
                    for u in UNITES_TEMPS {
                        ui.selectable_value(&mut s.unite_temps, u.to_string(), *u);
                    }
                });
        });
        ui.horizontal(|ui| {
            ui.label("EMA période");
            ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_ema).range(2..=1000));
        });
        ui.checkbox(&mut s.filtre_macro.utiliser_supertrend, "Supertrend");
        if s.filtre_macro.utiliser_supertrend {
            ui.horizontal(|ui| {
                ui.label("ATR période");
                ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_supertrend).range(1..=100));
                ui.label("×");
                ui.add(
                    egui::DragValue::new(&mut s.filtre_macro.multiplicateur_supertrend)
                        .range(0.1..=10.0)
                        .speed(0.05),
                );
            });
        }
        ui.horizontal(|ui| {
            ui.label("ADX période");
            ui.add(egui::DragValue::new(&mut s.adx.periode).range(2..=100));
        });
        ui.horizontal(|ui| {
            ui.label("ADX seuil");
            ui.add(
                egui::DragValue::new(&mut s.adx.seuil_minimal)
                    .range(1.0..=100.0)
                    .speed(0.5),
            );
        });
        ui.horizontal(|ui| {
            ui.label("Volume SMA (bougies)");
            ui.add(egui::DragValue::new(&mut s.volume.periode_moyenne_mobile).range(2..=500));
        });
        ui.horizontal(|ui| {
            ui.label("Multiplicateur volume");
            ui.add(
                egui::DragValue::new(&mut s.volume.multiplicateur)
                    .range(0.1..=10.0)
                    .speed(0.05)
                    .suffix("×"),
            );
        });
        return;
    }

    ui.label("Période");
    egui::ComboBox::from_id_salt(ui.id().with("unite_temps"))
        .selected_text(s.unite_temps.as_str())
        .width(60.0)
        .show_ui(ui, |ui| {
            for u in UNITES_TEMPS {
                ui.selectable_value(&mut s.unite_temps, u.to_string(), *u);
            }
        });
    ui.separator();
    ui.label("EMA période");
    ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_ema).range(2..=1000));
    ui.checkbox(&mut s.filtre_macro.utiliser_supertrend, "Supertrend");
    ui.add_enabled_ui(s.filtre_macro.utiliser_supertrend, |ui| {
        ui.label("ATR période");
        ui.add(egui::DragValue::new(&mut s.filtre_macro.periode_supertrend).range(1..=100));
        ui.label("×");
        ui.add(
            egui::DragValue::new(&mut s.filtre_macro.multiplicateur_supertrend)
                .range(0.1..=10.0)
                .speed(0.05),
        );
    });
    ui.separator();
    ui.label("ADX période");
    ui.add(egui::DragValue::new(&mut s.adx.periode).range(2..=100));
    ui.label("seuil");
    ui.add(
        egui::DragValue::new(&mut s.adx.seuil_minimal)
            .range(1.0..=100.0)
            .speed(0.5),
    );
    ui.separator();
    ui.label("Volume SMA");
    ui.add(egui::DragValue::new(&mut s.volume.periode_moyenne_mobile).range(2..=500));
    ui.label("Multiplicateur");
    ui.add(
        egui::DragValue::new(&mut s.volume.multiplicateur)
            .range(0.1..=10.0)
            .speed(0.05)
            .suffix("×"),
    );
}

/// Formate l'occupation du serveur pour l'en-tête (ex. « 🖥 Serveur : CPU 1.2 % · RAM 48 Mo (0.3 %) »).
fn texte_ressources(r: &RessourcesServeur) -> String {
    let memoire_mo = r.memoire_octets as f64 / (1024.0 * 1024.0);
    let memoire = if memoire_mo >= 1024.0 {
        format!("{:.2} Go", memoire_mo / 1024.0)
    } else {
        format!("{memoire_mo:.0} Mo")
    };
    let part = if r.memoire_totale_octets > 0 {
        format!(
            " ({:.1} %)",
            r.memoire_octets as f64 * 100.0 / r.memoire_totale_octets as f64
        )
    } else {
        String::new()
    };
    format!("🖥 Serveur : CPU {:.1} % · RAM {memoire}{part}", r.cpu_pourcent)
}

impl eframe::App for ApplicationCryptobot {
    /// Dessine l'interface et traite les signaux, statuts et interactions utilisateur.
    ///
    /// Entrées : interface egui et frame courant.
    /// Sortie : met à jour l'état de l'application et dessine la frame; ne retourne pas de valeur.
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        // 1. Réception des états et réponses du moteur
        self.recevoir();

        if let Some(erreur) = self.erreur_dialogue.clone() {
            let mut fermer = false;
            egui::Window::new("Erreur de source de données")
                .collapsible(false)
                .resizable(true)
                .default_width(520.0)
                .show(&ctx, |ui| {
                    ui.label(erreur);
                    if ui.button("Fermer").clicked() {
                        fermer = true;
                    }
                });
            if fermer {
                self.erreur_dialogue = None;
            }
        }

        // Le thread réseau réveille l'IHM à chaque retour ; ce délai fait avancer l'âge affiché.
        ctx.request_repaint_after(std::time::Duration::from_secs(1));

        let mode_avant = self.mode_actif;
        let mut parcourir: Option<String> = None;
        let mut lancer_test = false;
        let mut fournisseurs_modifies = false;

        // 2. BARRE SUPÉRIEURE : Sélection du mode & Seuil
        egui::Panel::top("en_tete").show(ui, |ui| {
            // Commandes grisées tant que le moteur ne répond pas.
            if !self.connecte {
                ui.disable();
            }
            ui.horizontal(|ui| {
                ui.heading("⚡ Détecteur de Tendances Crypto H24");
                ui.separator();

                if matches!(
                    self.mode_actif,
                    ModeExecution::TempsReel | ModeExecution::Simulation
                ) {
                    let (couleur, texte) = match self.dernier_message {
                        None => (egui::Color32::GRAY, "En attente de messages".to_string()),
                        Some(t) => {
                            let age = t.elapsed().as_secs();
                            let couleur = match age {
                                0..=9 => egui::Color32::GREEN,
                                10..=59 => egui::Color32::GOLD,
                                _ => egui::Color32::RED,
                            };
                            (
                                couleur,
                                format!("{} msg | dernier il y a {} s", self.nb_messages, age),
                            )
                        }
                    };
                    ui.label(egui::RichText::new("●").color(couleur));
                    ui.label(texte);
                    ui.separator();
                }

                // Menu déroulant pour le choix du mode
                egui::ComboBox::from_label("Mode")
                    .selected_text(match self.mode_actif {
                        ModeExecution::TempsReel => "🔴 Temps Réel (multi-sources)",
                        ModeExecution::Simulation => "🟡 Simulation Locale",
                        ModeExecution::BacktestPeriode => "🔵 Test Historique (REST)",
                        ModeExecution::BacktestFichier => "📁 Fichier Local (JSON)",
                    })
                    .show_ui(ui, |ui| {
                        ui.selectable_value(
                            &mut self.mode_actif,
                            ModeExecution::TempsReel,
                            "🔴 Temps Réel (multi-sources)",
                        );
                        ui.selectable_value(
                            &mut self.mode_actif,
                            ModeExecution::Simulation,
                            "🟡 Simulation Locale",
                        );
                        ui.selectable_value(
                            &mut self.mode_actif,
                            ModeExecution::BacktestPeriode,
                            "🔵 Test Historique (REST)",
                        );
                        ui.selectable_value(
                            &mut self.mode_actif,
                            ModeExecution::BacktestFichier,
                            "📁 Fichier Local (JSON)",
                        );
                    });

                ui.separator();
                self.menu_notifications(ui);
            });

            if self.mode_actif == ModeExecution::TempsReel {
                ui.horizontal_wrapped(|ui| {
                    ui.label("Flux de données :");
                    for (fournisseur, libelle) in [
                        (Fournisseur::Binance, "Binance"),
                        (Fournisseur::Kraken, "Kraken"),
                        (Fournisseur::Coinbase, "Coinbase"),
                    ] {
                        let mut actif = self.fournisseurs.contains(&fournisseur);
                        if ui
                            .add_enabled(
                                !actif || self.fournisseurs.len() > 1,
                                egui::Checkbox::new(&mut actif, libelle),
                            )
                            .changed()
                        {
                            if actif {
                                if !self.fournisseurs.contains(&fournisseur) {
                                    self.fournisseurs.push(fournisseur);
                                }
                            } else {
                                self.fournisseurs.retain(|f| *f != fournisseur);
                            }
                            fournisseurs_modifies = true;
                        }
                    }
                });
            }

            // Barre de sous-menu dynamique pour le Backtest / Test
            if self.mode_actif == ModeExecution::BacktestPeriode
                || self.mode_actif == ModeExecution::BacktestFichier
            {
                ui.separator();
                ui.vertical(|ui| {
                    if self.mode_actif == ModeExecution::BacktestPeriode {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Début (AAAA-MM-JJ) :");
                            ui.text_edit_singleline(&mut self.date_debut);
                            if ui
                                .button("📅")
                                .on_hover_text("Choisir la date de début")
                                .clicked()
                            {
                                if let Ok(date) =
                                    NaiveDate::parse_from_str(&self.date_debut, "%Y-%m-%d")
                                {
                                    self.date_debut_mois =
                                        NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                                            .unwrap();
                                }
                                self.calendrier_debut_ouvert = true;
                            }
                            ui.label("Fin (AAAA-MM-JJ) :");
                            ui.text_edit_singleline(&mut self.date_fin);
                            if ui
                                .button("📅")
                                .on_hover_text("Choisir la date de fin")
                                .clicked()
                            {
                                if let Ok(date) =
                                    NaiveDate::parse_from_str(&self.date_fin, "%Y-%m-%d")
                                {
                                    self.date_fin_mois =
                                        NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                                            .unwrap();
                                }
                                self.calendrier_fin_ouvert = true;
                            }
                        });
                    } else {
                        ui.horizontal_wrapped(|ui| {
                            ui.label("Chemin fichier test :");
                            ui.text_edit_singleline(&mut self.chemin_fichier_test);
                            if ui
                                .button("📂 Parcourir...")
                                .on_hover_text("Fichiers de la machine du moteur")
                                .clicked()
                            {
                                parcourir = Some(dossier_de(&self.chemin_fichier_test));
                            }
                        });
                    }

                    ui.label("Paires à jouer :");
                    let mut selection = self.symboles_test.clone();
                    ui.horizontal_wrapped(|ui| {
                        for symbole in &self.symboles_test_disponibles {
                            let mut coche = selection.contains(&symbole.to_uppercase());
                            if ui.checkbox(&mut coche, formater_paire(symbole)).changed() {
                                if coche {
                                    selection.insert(symbole.to_uppercase());
                                } else {
                                    selection.remove(&symbole.to_uppercase());
                                }
                            }
                        }
                    });
                    self.symboles_test = selection;

                    if ui.button("▶ Charger & Lancer l'Analyse").clicked() {
                        lancer_test = true;
                    }
                });
            }

            // Paramètres de calcul : appliqués par « Appliquer » (relance le mode courant)
            ui.separator();
            let mut actions: Vec<ActionReglages> = Vec::new();
            ui.horizontal_wrapped(|ui| {
                ui.label("Réglages :");
                egui::ComboBox::from_id_salt("portee")
                    .selected_text(libelle_portee(&self.portee))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.portee, Portee::Generique, "Défaut")
                            .on_hover_text("S'applique aux paires sans réglage propre");
                        for symbole in &self.symboles_test_disponibles {
                            let propre = self.parametrages.paires.contains_key(symbole);
                            let texte = if propre {
                                format!("{symbole} ★")
                            } else {
                                symbole.clone()
                            };
                            ui.selectable_value(
                                &mut self.portee,
                                Portee::Paire(symbole.clone()),
                                texte,
                            );
                        }
                    });
                if matches!(self.portee, Portee::Paire(_)) {
                    let propre = self.parametrages.a_un_reglage_propre(&self.portee);
                    ui.label(if propre {
                        "(réglage propre ★)"
                    } else {
                        "(suit le défaut)"
                    });
                    if ui
                        .add_enabled(propre, egui::Button::new("↺ Revenir au défaut"))
                        .clicked()
                    {
                        actions.push(ActionReglages::RevenirAuDefaut(self.portee.clone()));
                    }
                }
                ui.separator();
                if ui
                    .button("📋 Copier")
                    .on_hover_text("Copie ces réglages pour les coller sur une autre paire")
                    .clicked()
                {
                    actions.push(ActionReglages::Copier(self.portee.clone()));
                }
                let collage_possible = self.presse_papiers.is_some();
                let texte_coller = match &self.presse_papiers {
                    Some((source, _)) => format!("📥 Coller « {source} »"),
                    None => "📥 Coller".to_string(),
                };
                if ui
                    .add_enabled(collage_possible, egui::Button::new(texte_coller))
                    .clicked()
                {
                    actions.push(ActionReglages::Coller(self.portee.clone()));
                }
                if ui
                    .button("⇉ Copier vers…")
                    .on_hover_text("Copie ces réglages vers plusieurs paires")
                    .clicked()
                {
                    self.copie_vers = Some(HashSet::new());
                }
                ui.separator();
                let non_sauvegarde = self.parametrages != self.parametrages_sauvegardes;
                if ui
                    .button(if non_sauvegarde {
                        "💾 Sauvegarder *"
                    } else {
                        "💾 Sauvegarder"
                    })
                    .on_hover_text(
                        "Écrit les réglages dans parametres.toml (rechargés au démarrage)",
                    )
                    .clicked()
                {
                    actions.push(ActionReglages::Sauvegarder);
                }
                if non_sauvegarde {
                    ui.label(
                        egui::RichText::new("● modifications non sauvegardées")
                            .color(egui::Color32::from_rgb(200, 120, 0)),
                    );
                } else if !self.message_reglages.is_empty() {
                    ui.label(
                        egui::RichText::new(&self.message_reglages).color(egui::Color32::GRAY),
                    );
                }
            });

            let mut s = self.parametrages.lire(&self.portee);
            let avant = s.clone();
            ui.horizontal_wrapped(|ui| {
                ui.add(egui::Slider::new(&mut s.score_min_alerte, 0..=100).text("Seuil d'alerte"));
                ui.separator();
                editeur_indicateurs(ui, &mut s, false);
                ui.separator();
                if ui
                    .button("✔ Appliquer")
                    .on_hover_text("Relance l'analyse avec ces réglages")
                    .clicked()
                {
                    lancer_test = true;
                }
            });
            if s != avant {
                self.parametrages.ecrire(&self.portee, s);
            }
            for action in actions {
                self.executer_action_reglages(action);
            }
        });

        // Fenêtre « Copier vers… » : choix des paires qui reçoivent les réglages de la portée éditée.
        if let Some(mut cibles) = self.copie_vers.take() {
            let mut ouverte = true;
            let mut valider = false;
            let source = self.portee.clone();
            egui::Window::new(format!("Copier « {} » vers…", libelle_portee(&source)))
                .id(egui::Id::new("copie_vers"))
                .open(&mut ouverte)
                .collapsible(false)
                .resizable(false)
                .show(&ctx, |ui| {
                    ui.horizontal(|ui| {
                        if ui.small_button("Tout cocher").clicked() {
                            cibles.extend(self.symboles_test_disponibles.iter().cloned());
                        }
                        if ui.small_button("Aucune").clicked() {
                            cibles.clear();
                        }
                    });
                    for symbole in &self.symboles_test_disponibles {
                        if source == Portee::Paire(symbole.clone()) {
                            continue;
                        }
                        let mut coche = cibles.contains(symbole);
                        if ui.checkbox(&mut coche, formater_paire(symbole)).changed() {
                            if coche {
                                cibles.insert(symbole.clone());
                            } else {
                                cibles.remove(symbole);
                            }
                        }
                    }
                    ui.separator();
                    if ui
                        .add_enabled(!cibles.is_empty(), egui::Button::new("⇉ Copier"))
                        .clicked()
                    {
                        valider = true;
                    }
                });
            if valider {
                let mut liste: Vec<String> = cibles.into_iter().collect();
                liste.sort_unstable();
                self.executer_action_reglages(ActionReglages::CopierVers(source, liste));
            } else if ouverte {
                self.copie_vers = Some(cibles);
            }
        }

        if let Some(chemin) = parcourir {
            self.parcourir(chemin);
        }
        self.afficher_navigateur(&ctx);

        afficher_calendrier(
            &ctx,
            "Choisir la date de début",
            "calendrier_debut",
            &mut self.calendrier_debut_ouvert,
            &mut self.date_debut_mois,
            &mut self.date_debut,
        );
        afficher_calendrier(
            &ctx,
            "Choisir la date de fin",
            "calendrier_fin",
            &mut self.calendrier_fin_ouvert,
            &mut self.date_fin_mois,
            &mut self.date_fin,
        );

        if self.mode_actif != mode_avant {
            self.changer_de_mode();
        } else if fournisseurs_modifies {
            self.reinitialiser();
            self.statut = "Redémarrage avec les flux sélectionnés...".to_string();
            self.lancer();
        } else if lancer_test {
            if self.symboles_test.is_empty() {
                self.statut = "Sélectionnez au moins une paire à jouer.".to_string();
            } else {
                self.reinitialiser();
                self.statut = "Chargement en cours...".to_string();
                self.lancer();
            }
        }

        // 3. PANNEAU INFÉRIEUR : Journal des alertes
        let mut alerte_cliquee: Option<SignalVisuel> = None;
        egui::Panel::bottom("journal_alertes")
            .resizable(true)
            .default_size(140.0)
            .show(ui, |ui| {
                ui.heading("🚨 Dernières Alertes Détectées");
                ui.separator();

                egui::ScrollArea::vertical()
                    .auto_shrink([false, false])
                    .show(ui, |ui| {
                        if self.journal_alertes.is_empty() {
                            ui.label("Aucune alerte franchie pour le moment.");
                        } else {
                            for (index, alerte) in self.journal_alertes.iter().enumerate() {
                                let selectionnee = self.selection.as_ref().is_some_and(|s| {
                                    s.symbole == alerte.symbole && s.horodatage == alerte.horodatage
                                });
                                let ligne = ui.horizontal(|ui| {
                                    if selectionnee {
                                        ui.label("▶");
                                    }
                                    ui.label(format!("[{}]", alerte.horodatage));
                                    ui.label(
                                        egui::RichText::new(&alerte.symbole)
                                            .strong()
                                            .color(egui::Color32::GOLD),
                                    );
                                    ui.label(format!("Prix: {}", formater_prix(alerte.prix)));
                                    ui.label(egui::RichText::new(alerte.tendance.libelle()).color(
                                        if alerte.tendance == Tendance::Haussiere {
                                            egui::Color32::GREEN
                                        } else {
                                            egui::Color32::LIGHT_RED
                                        },
                                    ));
                                    ui.label(format!("ADX: {:.1}", alerte.adx));
                                    ui.label(format!("Vol Ratio: {:.2}x", alerte.ratio_volume));
                                    ui.label(
                                        egui::RichText::new(format!("SCORE: {}/100", alerte.score))
                                            .strong()
                                            .color(egui::Color32::GREEN),
                                    );
                                });
                                let reponse = ui
                                    .interact(
                                        ligne.response.rect,
                                        ui.id().with(("alerte", index)),
                                        egui::Sense::click(),
                                    )
                                    .on_hover_cursor(egui::CursorIcon::PointingHand);
                                if reponse.clicked() {
                                    alerte_cliquee = Some(alerte.clone());
                                }
                            }
                        }
                    });
            });
        if let Some(alerte) = alerte_cliquee {
            self.selection = Some(alerte);
        }

        // 4. PANNEAU CENTRAL : Grille des cartes par paire
        egui::CentralPanel::default().show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.heading("📊 Paires Surveillées en Temps Réel");
                if let Some(r) = self.ressources {
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        ui.label(texte_ressources(&r))
                            .on_hover_text("Occupation du processus serveur (mise à jour toutes les 2 s)");
                    });
                }
            });
            if let Some(erreur) = &self.erreur_reseau {
                ui.label(
                    egui::RichText::new(format!("⚠ {erreur} — nouvelle tentative…"))
                        .strong()
                        .color(egui::Color32::LIGHT_RED),
                );
            } else if self.instance.is_none() {
                ui.label(egui::RichText::new("Connexion au moteur…").color(egui::Color32::GRAY));
            }
            if !self.statut.is_empty() {
                ui.label(
                    egui::RichText::new(&self.statut).color(egui::Color32::from_rgb(90, 60, 0)),
                );
            }
            ui.separator();

            let mut retour_direct = false;
            let mut appliquer = false;
            let mut modifications: Vec<(String, ConfigurationStrategie)> = Vec::new();
            let mut actions: Vec<ActionReglages> = Vec::new();
            let presse_papiers = self.presse_papiers.as_ref().map(|(source, _)| source.clone());
            egui::ScrollArea::vertical().show(ui, |ui| {
                ui.horizontal_wrapped(|ui| {
                    for (symbole, signal_courant) in &self.paires {
                        let alerte_affichee =
                            self.selection.as_ref().filter(|s| s.symbole == *symbole);
                        let signal = alerte_affichee.unwrap_or(signal_courant);
                        let mut strat = self.parametrages.resoudre(symbole).clone();
                        let strat_initiale = strat.clone();
                        ui.group(|ui| {
                            ui.set_width(280.0);
                            ui.vertical(|ui| {
                                // Titre & Prix actuel
                                ui.horizontal(|ui| {
                                    ui.heading(formater_paire(symbole));
                                    ui.label(
                                        egui::RichText::new(format!("({})", signal.unite))
                                            .color(egui::Color32::GRAY),
                                    );
                                    ui.with_layout(
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            let couleur_prix =
                                                if signal.prix > signal.prix_precedent {
                                                    egui::Color32::GREEN
                                                } else if signal.prix < signal.prix_precedent {
                                                    egui::Color32::LIGHT_RED
                                                } else {
                                                    egui::Color32::GRAY
                                                };
                                            ui.label(
                                                egui::RichText::new(formater_prix(signal.prix))
                                                    .strong()
                                                    .size(16.0)
                                                    .color(couleur_prix),
                                            );
                                        },
                                    );
                                });

                                ui.separator();

                                if alerte_affichee.is_some() {
                                    ui.horizontal(|ui| {
                                        ui.label(
                                            egui::RichText::new(format!(
                                                "📌 Alerte du {}",
                                                signal.horodatage
                                            ))
                                            .strong()
                                            .color(egui::Color32::from_rgb(150, 90, 0)),
                                        );
                                        if ui.small_button("↩ Direct").clicked() {
                                            retour_direct = true;
                                        }
                                    });
                                }

                                // Tendance Macro EMA
                                let couleur_tendance = match signal.tendance {
                                    Tendance::Haussiere => egui::Color32::GREEN,
                                    Tendance::Baissiere => egui::Color32::LIGHT_RED,
                                    Tendance::Neutre => egui::Color32::GRAY,
                                };
                                ui.label(
                                    egui::RichText::new(format!(
                                        "Tendance EMA : {}",
                                        signal.tendance.libelle()
                                    ))
                                    .strong()
                                    .color(couleur_tendance),
                                )
                                .on_hover_text(format!(
                                    "Tendance de fond, dans les deux sens.\n\
                                     Hausse si la clôture est au-dessus de l'EMA {0}, baisse si elle est en dessous{1}.\n\
                                     Neutre si les critères ne s'accordent pas.\n\
                                     Utilité : ne retenir que les mouvements alignés sur la tendance. \
                                     Neutre = score 0.\n\
                                     Poids dans le score : 40 pts.",
                                    strat.filtre_macro.periode_ema,
                                    if strat.filtre_macro.utiliser_supertrend {
                                        format!(
                                            ", confirmée par le Supertrend (ATR {} × {}) de même sens",
                                            strat.filtre_macro.periode_supertrend,
                                            strat.filtre_macro.multiplicateur_supertrend
                                        )
                                    } else {
                                        String::new()
                                    }
                                ));

                                // ADX avec indicateur de validation
                                ui.horizontal(|ui| {
                                    ui.label(format!("Force ADX : {:.1}", signal.adx))
                                        .on_hover_text(format!(
                                            "ADX (Average Directional Index), période {}.\n\
                                             Mesure la force de la tendance (0 à 100), pas son sens.\n\
                                             Valide si ADX ≥ {:.0}.\n\
                                             Utilité : écarter les marchés sans direction (range) où les signaux sont peu fiables.\n\
                                             Poids dans le score : 30 pts.",
                                            strat.adx.periode,
                                            strat.adx.seuil_minimal
                                        ));
                                    if signal.adx_valide {
                                        ui.label(
                                            egui::RichText::new("✔ OK").color(egui::Color32::GREEN),
                                        );
                                    } else {
                                        ui.label(
                                            egui::RichText::new("✖ FAIBLE")
                                                .color(egui::Color32::DARK_GRAY),
                                        );
                                    }
                                });

                                // Surtension Volume avec validation
                                ui.horizontal(|ui| {
                                    ui.label(format!(
                                        "Surtension Volume : {:.2}x",
                                        signal.ratio_volume
                                    ))
                                    .on_hover_text(format!(
                                        "Ratio = volume de la bougie / moyenne des {} derniers volumes.\n\
                                         Cassure si le ratio dépasse {:.2}x.\n\
                                         Utilité : confirmer qu'un mouvement est soutenu par une vraie participation du marché.\n\
                                         Poids dans le score : 30 pts.",
                                        strat.volume.periode_moyenne_mobile,
                                        strat.volume.multiplicateur
                                    ));
                                    if signal.volume_valide {
                                        ui.label(
                                            egui::RichText::new("✔ CASSURE")
                                                .color(egui::Color32::GREEN),
                                        );
                                    } else {
                                        ui.label(
                                            egui::RichText::new("✖ NORMAL")
                                                .color(egui::Color32::DARK_GRAY),
                                        );
                                    }
                                });

                                ui.add_space(6.0);

                                // Jauge de score corrigée (calcul f32 explicite)
                                ui.label(format!("Score global : {}/100", signal.score))
                                    .on_hover_text(
                                        "Somme des critères : Macro 40 + ADX 30 + Volume 30.\n\
                                         Score à 0 si la tendance de fond est neutre.\n\
                                         Alerte si le score atteint le seuil d'alerte réglé dans la barre de paramètres.",
                                    );

                                let ratio_score = (signal.score as f32) / 100.0;
                                let couleur_barre = if signal.score >= strat.score_min_alerte.min(100) as u8 {
                                    egui::Color32::GREEN
                                } else if signal.score >= 50 {
                                    egui::Color32::YELLOW
                                } else {
                                    egui::Color32::GRAY
                                };

                                ui.add(
                                    egui::ProgressBar::new(ratio_score)
                                        .fill(couleur_barre)
                                        .animate(signal.est_alerte),
                                );

                                let portee = Portee::Paire(symbole.clone());
                                let propre = self.parametrages.a_un_reglage_propre(&portee);
                                egui::CollapsingHeader::new(if propre {
                                    "⚙ Paramètres de la paire ★"
                                } else {
                                    "⚙ Paramètres de la paire (défaut)"
                                })
                                    .id_salt(("params", symbole))
                                    .show(ui, |ui| {
                                        ui.horizontal_wrapped(|ui| {
                                            if ui
                                                .small_button("📋 Copier")
                                                .on_hover_text("Copie les réglages de cette paire")
                                                .clicked()
                                            {
                                                actions.push(ActionReglages::Copier(portee.clone()));
                                            }
                                            let texte = match &presse_papiers {
                                                Some(source) => format!("📥 Coller « {source} »"),
                                                None => "📥 Coller".to_string(),
                                            };
                                            if ui
                                                .add_enabled(
                                                    presse_papiers.is_some(),
                                                    egui::Button::new(texte).small(),
                                                )
                                                .clicked()
                                            {
                                                actions.push(ActionReglages::Coller(portee.clone()));
                                            }
                                            if ui
                                                .add_enabled(
                                                    propre,
                                                    egui::Button::new("↺ Défaut").small(),
                                                )
                                                .on_hover_text("Supprime le réglage propre : la paire suit le défaut")
                                                .clicked()
                                            {
                                                actions.push(ActionReglages::RevenirAuDefaut(
                                                    portee.clone(),
                                                ));
                                            }
                                        });
                                        ui.vertical(|ui| {
                                            ui.add(
                                                egui::Slider::new(
                                                    &mut strat.score_min_alerte,
                                                    0..=100,
                                                )
                                                .text("Seuil"),
                                            );
                                            editeur_indicateurs(ui, &mut strat, true);
                                        });
                                        if ui.button("✔ Appliquer").clicked() {
                                            appliquer = true;
                                        }
                                    });
                            });
                        });
                        if strat != strat_initiale {
                            modifications.push((symbole.clone(), strat));
                        }
                    }
                });
            });
            for (symbole, strat) in modifications {
                self.parametrages.ecrire(&Portee::Paire(symbole), strat);
            }
            for action in actions {
                self.executer_action_reglages(action);
            }
            if appliquer {
                self.reinitialiser();
                self.statut = "Chargement en cours...".to_string();
                self.lancer();
            }
            if retour_direct {
                self.selection = None;
            }
        });
    }
}
