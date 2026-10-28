use crate::config::{ConfigurationStrategie, Parametrages, Portee};
use crate::core::modeles::Tendance;
use crate::data::sources::Fournisseur;
use chrono::{Datelike, Duration, NaiveDate};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver, UnboundedSender};

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

pub struct ApplicationCryptobot {
    pub rx_mpsc: Receiver<SignalVisuel>,
    pub paires: HashMap<String, SignalVisuel>,
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
    /// Portée (générique, bloc ou paire) actuellement éditée.
    portee: Portee,
    symboles: Vec<String>,
    symboles_test_disponibles: Vec<String>,
    symboles_test: HashSet<String>,
    pub generation: u64,
    pub statut: String,
    /// Alerte cliquée dans le journal : sa paire affiche les données de cette alerte.
    pub selection: Option<SignalVisuel>,
    /// Messages reçus du flux en direct depuis le lancement de la session.
    nb_messages: u64,
    dernier_message: Option<Instant>,
    tx_commandes: Option<UnboundedSender<CommandeMoteur>>,
    rx_statut: Option<UnboundedReceiver<(u64, NotificationMoteur)>>,
    erreur_dialogue: Option<String>,
}

impl ApplicationCryptobot {
    pub fn new(
        rx_mpsc: Receiver<SignalVisuel>,
        parametrages: Parametrages,
        fournisseurs: Vec<Fournisseur>,
        symboles: Vec<String>,
    ) -> Self {
        let mut symboles_test_disponibles = symboles.clone();
        symboles_test_disponibles.extend(parametrages.paires.keys().cloned());
        for groupe in parametrages.groupes.values() {
            symboles_test_disponibles.extend(groupe.symboles.iter().cloned());
        }
        symboles_test_disponibles = symboles_test_disponibles
            .into_iter()
            .map(|s| s.to_uppercase())
            .collect();
        symboles_test_disponibles.sort_unstable();
        symboles_test_disponibles.dedup();

        Self {
            parametrages,
            fournisseurs,
            portee: Portee::Generique,
            symboles_test: symboles.iter().map(|s| s.to_uppercase()).collect(),
            symboles,
            symboles_test_disponibles,
            rx_mpsc,
            paires: HashMap::new(),
            journal_alertes: Vec::new(),
            mode_actif: ModeExecution::TempsReel,
            date_debut: "2026-01-01".to_string(),
            date_fin: "2026-02-01".to_string(),
            date_debut_mois: NaiveDate::from_ymd_opt(2026, 1, 1).unwrap(),
            date_fin_mois: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
            calendrier_debut_ouvert: false,
            calendrier_fin_ouvert: false,
            chemin_fichier_test: "historique.json".to_string(),
            generation: 0,
            statut: String::new(),
            selection: None,
            nb_messages: 0,
            dernier_message: None,
            tx_commandes: None,
            rx_statut: None,
            erreur_dialogue: None,
        }
    }

    /// Branche l'IHM sur le moteur : envoi des commandes et réception des messages d'état.
    pub fn avec_moteur(
        mut self,
        tx_commandes: UnboundedSender<CommandeMoteur>,
        rx_statut: UnboundedReceiver<(u64, NotificationMoteur)>,
    ) -> Self {
        self.tx_commandes = Some(tx_commandes);
        self.rx_statut = Some(rx_statut);
        self
    }

    pub fn avec_etat_initial(
        mut self,
        generation: u64,
        mode: ModeExecution,
        signaux: Vec<SignalVisuel>,
        statut: Option<(u64, NotificationMoteur)>,
    ) -> Self {
        self.generation = generation;
        self.mode_actif = mode;
        for signal in signaux {
            if signal.generation == generation {
                self.paires.insert(signal.symbole.clone(), signal);
            }
        }
        if let Some((generation_statut, notification)) = statut {
            if generation_statut == generation {
                match notification {
                    NotificationMoteur::Statut(texte) => self.statut = texte,
                    NotificationMoteur::Erreur(texte) | NotificationMoteur::Echec(texte) => {
                        self.erreur_dialogue = Some(texte);
                    }
                }
            }
        }
        self
    }

    /// Efface toutes les valeurs affichées et invalide les signaux encore en transit.
    fn reinitialiser(&mut self) {
        self.generation += 1;
        self.paires.clear();
        self.journal_alertes.clear();
        self.selection = None;
        self.statut.clear();
        self.nb_messages = 0;
        self.dernier_message = None;
        while self.rx_mpsc.try_recv().is_ok() {}
    }

    fn envoyer(&mut self, commande: CommandeMoteur) {
        if let Some(tx) = &self.tx_commandes {
            if tx.send(commande).is_err() {
                self.statut = "Connexion au moteur perdue.".to_string();
            }
        }
    }

    /// Change de mode : tout est réinitialisé ; temps réel et simulation démarrent aussitôt,
    /// les modes de test attendent le clic sur « Charger & Lancer ».
    fn changer_de_mode(&mut self) {
        self.reinitialiser();
        match self.mode_actif {
            ModeExecution::TempsReel | ModeExecution::Simulation => self.lancer(),
            ModeExecution::BacktestPeriode | ModeExecution::BacktestFichier => {
                self.envoyer(CommandeMoteur::Arreter {
                    generation: self.generation,
                    mode: self.mode_actif,
                });
                self.statut =
                    "Renseignez les paramètres puis cliquez sur « Charger & Lancer ».".to_string();
            }
        }
    }

    fn lancer(&mut self) {
        self.envoyer(CommandeMoteur::Demarrer {
            generation: self.generation,
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
}

/// Champs d'édition des indicateurs d'un paramétrage (utilisés par la barre et par chaque carte).
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

impl eframe::App for ApplicationCryptobot {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        // 1. Réception des signaux du moteur Tokio
        while let Ok(signal) = self.rx_mpsc.try_recv() {
            if signal.generation != self.generation {
                continue;
            }
            // Les signaux d'amorçage (avant l'ouverture du flux) ne comptent pas.
            if self.statut.starts_with('🔴') || self.statut.starts_with('🟡') {
                self.nb_messages += 1;
                self.dernier_message = Some(Instant::now());
            }
            if signal.est_alerte {
                self.journal_alertes.insert(0, signal.clone());
                if self.journal_alertes.len() > 5000 {
                    self.journal_alertes.pop();
                }
            }
            self.paires.insert(signal.symbole.clone(), signal);
        }
        if let Some(rx) = &mut self.rx_statut {
            while let Ok((generation, notification)) = rx.try_recv() {
                if generation == self.generation {
                    match notification {
                        NotificationMoteur::Statut(texte) => self.statut = texte,
                        NotificationMoteur::Erreur(texte) | NotificationMoteur::Echec(texte) => {
                            if let Some(erreur) = &mut self.erreur_dialogue {
                                erreur.push('\n');
                                erreur.push_str(&texte);
                            } else {
                                self.erreur_dialogue = Some(texte);
                            }
                        }
                    }
                }
            }
        }

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

        ctx.request_repaint();

        let mode_avant = self.mode_actif;
        let mut lancer_test = false;
        let mut fournisseurs_modifies = false;

        // 2. BARRE SUPÉRIEURE : Sélection du mode & Seuil
        egui::Panel::top("en_tete").show(ui, |ui| {
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
                            if ui.button("📂 Parcourir...").clicked() {
                                let mut dialogue =
                                    rfd::FileDialog::new().add_filter("Historique JSON", &["json"]);
                                if let Some(dossier) =
                                    std::path::Path::new(&self.chemin_fichier_test)
                                        .parent()
                                        .filter(|d| d.is_dir())
                                {
                                    dialogue = dialogue.set_directory(dossier);
                                }
                                if let Some(chemin) = dialogue.pick_file() {
                                    self.chemin_fichier_test = chemin.display().to_string();
                                }
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
            let mut sauvegarder = false;
            let mut heriter = false;
            let mut propager = false;
            ui.horizontal_wrapped(|ui| {
                ui.label("Paramétrage :");
                let libelle = |p: &Portee| match p {
                    Portee::Generique => "Générique".to_string(),
                    Portee::Groupe(nom) => format!("Bloc {nom}"),
                    Portee::Paire(symbole) => format!("Paire {symbole}"),
                };
                egui::ComboBox::from_id_salt("portee")
                    .selected_text(libelle(&self.portee))
                    .show_ui(ui, |ui| {
                        ui.selectable_value(&mut self.portee, Portee::Generique, "Générique");
                        for nom in self.parametrages.groupes.keys() {
                            let p = Portee::Groupe(nom.clone());
                            let texte = libelle(&p);
                            ui.selectable_value(&mut self.portee, p, texte);
                        }
                        for symbole in &self.symboles {
                            let p = Portee::Paire(symbole.to_uppercase());
                            let texte = libelle(&p);
                            ui.selectable_value(&mut self.portee, p, texte);
                        }
                    });
                if self.portee != Portee::Generique {
                    let propre = self.parametrages.a_un_reglage_propre(&self.portee);
                    ui.label(if propre {
                        "(réglage propre)"
                    } else {
                        "(hérité)"
                    });
                    if ui
                        .add_enabled(propre, egui::Button::new("↺ Hériter"))
                        .clicked()
                    {
                        heriter = true;
                    }
                }
                if !matches!(self.portee, Portee::Paire(_)) {
                    if ui
                        .button("⇊ Copier vers les paires")
                        .on_hover_text("Écrase le réglage propre des paires concernées")
                        .clicked()
                    {
                        propager = true;
                    }
                }
                if ui.button("💾 Sauvegarder").clicked() {
                    sauvegarder = true;
                }
            });
            if heriter {
                self.parametrages.reinitialiser(&self.portee);
            }
            if propager {
                self.parametrages.propager(&self.portee, &self.symboles);
            }
            if sauvegarder {
                self.statut = "Sauvegarde des paramétrages sur le moteur...".to_string();
                self.envoyer(CommandeMoteur::SauvegarderParametrages {
                    parametrages: self.parametrages.clone(),
                });
            }

            let mut s = self.parametrages.lire(&self.portee);
            let avant = s.clone();
            let bloc_vertical = matches!(self.portee, Portee::Groupe(_));
            if bloc_vertical {
                ui.vertical(|ui| {
                    ui.add(
                        egui::Slider::new(&mut s.score_min_alerte, 0..=100).text("Seuil d'alerte"),
                    );
                    editeur_indicateurs(ui, &mut s, true);
                    if ui.button("✔ Appliquer").clicked() {
                        lancer_test = true;
                    }
                });
            } else {
                ui.horizontal_wrapped(|ui| {
                    ui.add(
                        egui::Slider::new(&mut s.score_min_alerte, 0..=100).text("Seuil d'alerte"),
                    );
                    ui.separator();
                    editeur_indicateurs(ui, &mut s, false);
                    ui.separator();
                    if ui.button("✔ Appliquer").clicked() {
                        lancer_test = true;
                    }
                });
            }
            if s != avant {
                self.parametrages.ecrire(&self.portee, s);
            }
        });

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
            ui.heading("📊 Paires Surveillées en Temps Réel");
            if !self.statut.is_empty() {
                ui.label(
                    egui::RichText::new(&self.statut).color(egui::Color32::from_rgb(90, 60, 0)),
                );
            }
            ui.separator();

            let mut retour_direct = false;
            let mut appliquer = false;
            let mut modifications: Vec<(String, ConfigurationStrategie)> = Vec::new();
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

                                egui::CollapsingHeader::new("⚙ Paramètres de la paire")
                                    .id_salt(("params", symbole))
                                    .show(ui, |ui| {
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
