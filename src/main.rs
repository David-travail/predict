// Sous Windows l'exe est une application « fenêtre » : un double-clic n'ouvre pas de console.
// En ligne de commande, on se rattache à la console du terminal parent (voir `rattacher_console`).
#![cfg_attr(windows, windows_subsystem = "windows")]

mod config;
mod core;
mod data;
mod indicators;
mod ipc;
mod ui;

use crate::config::{ConfigurationApplication, ConfigurationStrategie, Parametrages};
use crate::core::modeles::{AnalyseTechnique, Bougie, Tendance};
use crate::core::scoring::calculer_score;
use crate::data::rest::{telecharger_bougies_periode, telecharger_plage_bougies};
use crate::data::simulation::{generer_historique_fictif, simuler_flux_temps_reel};
use crate::data::sources::{Agregateur, BougieSourcee, Fournisseur, agreger_historique};
use crate::data::websocket::ecouter_flux_temps_reel;
use crate::indicators::adx::calculer_adx;
use crate::indicators::ema::calculer_ema;
use crate::indicators::supertrend::supertrend_haussier;
use crate::indicators::volume::{calculer_sma, valider_cassure_volume};
use crate::ui::{CommandeMoteur, ModeExecution, NotificationMoteur, SignalVisuel};

use chrono::{Local, NaiveDate};
use clap::{Parser, ValueEnum};
use std::collections::{HashMap, HashSet};
use std::fs::OpenOptions;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{Level, error, info, warn};
use tracing_subscriber::FmtSubscriber;
use tracing_subscriber::fmt::writer::MakeWriterExt;

/// Nombre de bougies antérieures au début d'un test, nécessaires pour amorcer EMA 200 et ADX.
const BOUGIES_PRECHAUFFAGE: usize = 200;
const HISTORIQUE_MAX: usize = 300;

/// Mode d'exécution choisi en ligne de commande (avec `--console`).
#[derive(Clone, Copy, ValueEnum)]
enum ModeCli {
    TempsReel,
    Simulation,
    Periode,
    Fichier,
}

/// Détecteur de tendances crypto : serveur en console ou interface connectée à un serveur.
#[derive(Parser)]
#[command(version, about)]
struct Arguments {
    /// Lance une analyse en console sans serveur (mode historique).
    #[arg(long)]
    console: bool,
    /// Lance le moteur et l'IHM ensemble ; fermer l'IHM arrête aussi le moteur.
    #[arg(long, conflicts_with_all = ["console", "serve", "connect"])]
    local: bool,
    /// Lance le moteur comme serveur réseau contrôlable par l'IHM.
    #[arg(long, conflicts_with_all = ["console", "connect"])]
    serve: bool,
    /// Adresse d'écoute du serveur (défaut : 0.0.0.0:8765).
    #[arg(long, requires = "serve")]
    listen: Option<SocketAddr>,
    /// Adresse du moteur auquel connecter l'IHM (défaut : 127.0.0.1:8765).
    #[arg(long, conflicts_with = "console")]
    connect: Option<SocketAddr>,
    /// Mode d'exécution en console
    #[arg(long, value_enum, default_value = "temps-reel", requires = "console")]
    mode: ModeCli,
    /// Date de début AAAA-MM-JJ (mode periode)
    #[arg(long, requires = "console")]
    debut: Option<String>,
    /// Date de fin AAAA-MM-JJ incluse (mode periode)
    #[arg(long, requires = "console")]
    fin: Option<String>,
    /// Fichier JSON de bougies (mode fichier)
    #[arg(long, requires = "console")]
    fichier: Option<String>,
    /// Fichier de configuration (défaut : config.toml du dossier courant ou de l'exe, sinon valeurs intégrées)
    #[arg(long)]
    config: Option<PathBuf>,
}

/// Chaque dossier candidat : courant, dossier de l'exe puis ses parents (utile depuis `target\debug`).
fn chercher_config() -> Option<PathBuf> {
    let mut dossiers = vec![std::env::current_dir().ok()?];
    if let Ok(exe) = std::env::current_exe() {
        dossiers.extend(exe.ancestors().skip(1).take(4).map(PathBuf::from));
    }
    dossiers
        .into_iter()
        .map(|d| d.join("config.toml"))
        .find(|c| c.is_file())
}

fn charger_configuration(
    demandee: Option<&PathBuf>,
) -> Result<ConfigurationApplication, Box<dyn std::error::Error>> {
    match demandee.cloned().or_else(chercher_config) {
        Some(chemin) => {
            info!("Configuration : {}", chemin.display());
            ConfigurationApplication::charger_config(&chemin)
        }
        None => {
            info!("config.toml introuvable : configuration intégrée utilisée");
            ConfigurationApplication::depuis_toml(include_str!("../config.toml"))
        }
    }
}

/// Fichier de sauvegarde des paramétrages, à côté du fichier de configuration.
fn chemin_parametrages(demandee: Option<&PathBuf>) -> PathBuf {
    demandee
        .cloned()
        .or_else(chercher_config)
        .and_then(|c| c.parent().map(|p| p.join("parametres.toml")))
        .unwrap_or_else(|| PathBuf::from("parametres.toml"))
}

/// Paramétrages de config.toml, remplacés par la sauvegarde si elle existe et est valide.
fn charger_parametrages(
    config: &ConfigurationApplication,
    chemin: &std::path::Path,
) -> Parametrages {
    let mut parametrages = if chemin.is_file() {
        match Parametrages::charger(chemin) {
            Ok(p) => {
                info!("Paramétrages chargés : {}", chemin.display());
                p
            }
            Err(erreur) => {
                error!("Sauvegarde ignorée : {erreur}");
                config.parametrages()
            }
        }
    } else {
        config.parametrages()
    };
    parametrages.completer(&config.paires.symboles);
    parametrages
}

/// Rattache le processus à la console du terminal qui l'a lancé, pour que `println!`, les logs
/// et `--help` s'affichent malgré le sous-système « windows ».
#[cfg(windows)]
fn rattacher_console() {
    use std::os::windows::io::AsRawHandle;

    unsafe extern "system" {
        fn AttachConsole(id_processus: u32) -> i32;
        fn GetStdHandle(identifiant: u32) -> *mut std::ffi::c_void;
        fn SetStdHandle(identifiant: u32, handle: *mut std::ffi::c_void) -> i32;
    }
    const ATTACH_PARENT_PROCESS: u32 = u32::MAX;
    const STD_OUTPUT_HANDLE: u32 = -11i32 as u32;
    const STD_ERROR_HANDLE: u32 = -12i32 as u32;

    // SAFETY: appels Win32 simples ; les handles ouverts sont conservés (`forget`) pour toute la vie du processus.
    unsafe {
        // Sortie déjà redirigée (pipe, fichier) : on la laisse telle quelle.
        let deja_valide = |h: *mut std::ffi::c_void| !h.is_null() && h as isize != -1;
        let sortie_redirigee = deja_valide(GetStdHandle(STD_OUTPUT_HANDLE));
        let erreur_redirigee = deja_valide(GetStdHandle(STD_ERROR_HANDLE));

        if AttachConsole(ATTACH_PARENT_PROCESS) == 0 {
            return;
        }
        if sortie_redirigee && erreur_redirigee {
            return;
        }
        if let Ok(console) = std::fs::OpenOptions::new().write(true).open("CONOUT$") {
            if !sortie_redirigee {
                SetStdHandle(STD_OUTPUT_HANDLE, console.as_raw_handle().cast());
            }
            if !erreur_redirigee {
                SetStdHandle(STD_ERROR_HANDLE, console.as_raw_handle().cast());
            }
            std::mem::forget(console);
        }
    }
}

#[cfg(not(windows))]
fn rattacher_console() {}

fn main() {
    let avec_arguments = std::env::args_os().len() > 1;
    // Sans argument (double-clic) : interface graphique sans console.
    if avec_arguments {
        rattacher_console();
    }
    if let Err(erreur) = executer(avec_arguments) {
        eprintln!("Erreur : {erreur}");
        // Sans console, l'erreur serait invisible : on l'affiche dans une boîte de dialogue.
        if !avec_arguments {
            rfd::MessageDialog::new()
                .set_level(rfd::MessageLevel::Error)
                .set_title("Crypto Trend Detector H24")
                .set_description(erreur.to_string())
                .show();
        }
        std::process::exit(1);
    }
}

fn executer(_avec_arguments: bool) -> Result<(), Box<dyn std::error::Error>> {
    let arguments = Arguments::parse();

    // 1. Initialisation des journaux (logs)
    let fichier_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open("predict.log")?;
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_writer(std::io::stdout.and(fichier_log))
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Démarrage du Détecteur Crypto H24...");

    // 2. Chargement de la configuration
    let config = charger_configuration(arguments.config.as_ref())?;
    info!("Paires surveillées : {:?}", config.paires.symboles);
    crate::data::reseau::definir_proxy(config.reseau.proxy.clone());

    let chemin_parametrages = chemin_parametrages(arguments.config.as_ref());
    let parametrages = charger_parametrages(&config, &chemin_parametrages);
    let runtime = tokio::runtime::Runtime::new()?;

    if arguments.local {
        let (tx_ui, rx_ui) = mpsc::channel::<SignalVisuel>(256);
        let (tx_commandes, rx_commandes) = mpsc::unbounded_channel::<CommandeMoteur>();
        let (tx_statut, rx_statut) = mpsc::unbounded_channel::<(u64, NotificationMoteur)>();
        tx_commandes.send(CommandeMoteur::Demarrer {
            generation: 0,
            mode: ModeExecution::TempsReel,
            date_debut: String::new(),
            date_fin: String::new(),
            chemin_fichier: String::new(),
            symboles_test: config.paires.symboles.clone(),
            parametrages: parametrages.clone(),
            fournisseurs: config.sources_donnees.fournisseurs.clone(),
        })?;
        runtime.spawn(moteur(
            Arc::new(config.clone()),
            tx_ui,
            tx_statut,
            rx_commandes,
        ));
        let options = eframe::NativeOptions {
            viewport: egui::ViewportBuilder::default().with_inner_size([900.0, 600.0]),
            ..Default::default()
        };
        let resultat = eframe::run_native(
            "Crypto Trend Detector H24",
            options,
            Box::new(move |_cc| {
                Box::new(
                    ui::ApplicationCryptobot::new(
                        rx_ui,
                        parametrages,
                        config.sources_donnees.fournisseurs.clone(),
                        config.paires.symboles.clone(),
                    )
                    .avec_moteur(tx_commandes, rx_statut),
                )
            }),
        );
        runtime.shutdown_background();
        resultat?;
        return Ok(());
    }

    if arguments.console {
        let (tx_ui, rx_ui) = mpsc::channel::<SignalVisuel>(256);
        let (tx_commandes, rx_commandes) = mpsc::unbounded_channel::<CommandeMoteur>();
        let (tx_statut, rx_statut) = mpsc::unbounded_channel::<(u64, NotificationMoteur)>();
        let (mode, date_debut, date_fin, chemin_fichier) = parametres_console(&arguments)?;
        tx_commandes.send(CommandeMoteur::Demarrer {
            generation: 0,
            mode,
            date_debut,
            date_fin,
            chemin_fichier,
            symboles_test: config.paires.symboles.clone(),
            parametrages,
            fournisseurs: config.sources_donnees.fournisseurs.clone(),
        })?;
        runtime.spawn(moteur(Arc::new(config), tx_ui, tx_statut, rx_commandes));
        let resultat = runtime.block_on(boucle_console(mode, rx_ui, rx_statut));
        drop(tx_commandes);
        runtime.shutdown_background();
        return resultat.map_err(|erreur| std::io::Error::other(erreur.to_string()).into());
    }

    if arguments.serve {
        let jeton = charger_jeton()?;
        let adresse = arguments
            .listen
            .unwrap_or_else(|| "0.0.0.0:8765".parse().expect("Adresse par défaut valide"));
        let listener = std::net::TcpListener::bind(adresse)?;
        listener.set_nonblocking(true)?;
        let (tx_ui, rx_ui) = mpsc::channel::<SignalVisuel>(256);
        let (tx_commandes, rx_commandes) = mpsc::unbounded_channel::<CommandeMoteur>();
        let (tx_statut, rx_statut) = mpsc::unbounded_channel::<(u64, NotificationMoteur)>();
        tx_commandes.send(CommandeMoteur::Demarrer {
            generation: 0,
            mode: ModeExecution::TempsReel,
            date_debut: String::new(),
            date_fin: String::new(),
            chemin_fichier: String::new(),
            symboles_test: config.paires.symboles.clone(),
            parametrages: parametrages.clone(),
            fournisseurs: config.sources_donnees.fournisseurs.clone(),
        })?;
        let etat_initial = ipc::EtatServeur {
            symboles: config.paires.symboles.clone(),
            parametrages,
            fournisseurs: config.sources_donnees.fournisseurs.clone(),
            generation: 0,
            mode: ModeExecution::TempsReel,
            statut: None,
            signaux: Vec::new(),
        };
        let resultat = runtime.block_on(async {
            let listener = tokio::net::TcpListener::from_std(listener)?;
            runtime.spawn(moteur(Arc::new(config), tx_ui, tx_statut, rx_commandes));
            ipc::servir(
                listener,
                jeton,
                etat_initial,
                tx_commandes,
                chemin_parametrages,
                rx_ui,
                rx_statut,
            )
            .await
        });
        runtime.shutdown_background();
        return resultat.map_err(|erreur| std::io::Error::other(erreur.to_string()).into());
    }

    let jeton = charger_jeton()?;
    let adresse = arguments
        .connect
        .unwrap_or_else(|| "127.0.0.1:8765".parse().expect("Adresse par défaut valide"));
    let connexion = runtime
        .block_on(ipc::connecter(&adresse.to_string(), jeton))
        .map_err(|erreur| std::io::Error::other(erreur.to_string()))?;
    let etat = connexion.etat;
    let parametrages_ui = etat.parametrages.clone();
    let symboles_ui = etat.symboles.clone();
    let fournisseurs_ui = etat.fournisseurs.clone();
    let generation = etat.generation;
    let mode = etat.mode;
    let signaux = etat.signaux;
    let statut = etat.statut;

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([900.0, 600.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Crypto Trend Detector H24",
        options,
        Box::new(move |_cc| {
            Box::new(
                ui::ApplicationCryptobot::new(
                    connexion.rx_signaux,
                    parametrages_ui,
                    fournisseurs_ui,
                    symboles_ui,
                )
                .avec_moteur(connexion.tx_commandes, connexion.rx_statut)
                .avec_etat_initial(generation, mode, signaux, statut),
            )
        }),
    )?;

    runtime.shutdown_background();
    Ok(())
}

fn charger_jeton() -> Result<String, Box<dyn std::error::Error>> {
    let jeton = std::env::var("PREDICT_TOKEN").map_err(
        |_| "Définissez la variable d'environnement PREDICT_TOKEN sur le moteur et l'IHM",
    )?;
    if jeton.len() < 16 {
        return Err("PREDICT_TOKEN doit contenir au moins 16 caractères".into());
    }
    Ok(jeton)
}

fn parametres_console(
    arguments: &Arguments,
) -> Result<(ModeExecution, String, String, String), String> {
    let mut debut = String::new();
    let mut fin = String::new();
    let mut fichier = String::new();
    let mode = match arguments.mode {
        ModeCli::TempsReel => ModeExecution::TempsReel,
        ModeCli::Simulation => ModeExecution::Simulation,
        ModeCli::Periode => {
            debut = arguments
                .debut
                .clone()
                .ok_or("--debut est requis avec --mode periode")?;
            fin = arguments
                .fin
                .clone()
                .ok_or("--fin est requis avec --mode periode")?;
            ModeExecution::BacktestPeriode
        }
        ModeCli::Fichier => {
            fichier = arguments
                .fichier
                .clone()
                .ok_or("--fichier est requis avec --mode fichier")?;
            ModeExecution::BacktestFichier
        }
    };
    Ok((mode, debut, fin, fichier))
}

/// Affiche statuts et alertes jusqu'à la fin du test, une erreur ou Ctrl+C.
/// En temps réel et en simulation, les analyses sont déjà tracées par les logs.
async fn boucle_console(
    mode: ModeExecution,
    mut rx_signaux: mpsc::Receiver<SignalVisuel>,
    mut rx_statut: mpsc::UnboundedReceiver<(u64, NotificationMoteur)>,
) -> Result<(), String> {
    let rejeu = matches!(
        mode,
        ModeExecution::BacktestPeriode | ModeExecution::BacktestFichier
    );
    loop {
        tokio::select! {
            signal = rx_signaux.recv() => {
                let Some(s) = signal else { return Ok(()) };
                if rejeu && s.est_alerte {
                    println!(
                        "[{}] ALERTE {} {} | Prix: {:.2} | ADX: {:.1} | Vol Ratio: {:.2}x | SCORE: {}/100",
                        s.horodatage, s.symbole, s.tendance.libelle(), s.prix, s.adx, s.ratio_volume, s.score
                    );
                }
            }
            notification = rx_statut.recv() => {
                let Some((_, notification)) = notification else { return Ok(()) };
                match notification {
                    NotificationMoteur::Statut(texte) => {
                        println!("{texte}");
                        if texte.starts_with('✅') {
                            return Ok(());
                        }
                    }
                    NotificationMoteur::Erreur(texte) => eprintln!("Erreur de source : {texte}"),
                    NotificationMoteur::Echec(texte) => {
                        eprintln!("Erreur : {texte}");
                        return Err(texte);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}

/// Ressources partagées par une session d'analyse (un mode, une génération).
struct Contexte {
    config: Arc<ConfigurationApplication>,
    tx_ui: mpsc::Sender<SignalVisuel>,
    tx_statut: mpsc::UnboundedSender<(u64, NotificationMoteur)>,
    generation: u64,
    parametrages: Parametrages,
    fournisseurs: Vec<Fournisseur>,
}

type Historiques = HashMap<String, Vec<Bougie>>;

/// Moteur : exécute une session à la fois et remplace la précédente à chaque commande de l'IHM.
async fn moteur(
    config: Arc<ConfigurationApplication>,
    tx_ui: mpsc::Sender<SignalVisuel>,
    tx_statut: mpsc::UnboundedSender<(u64, NotificationMoteur)>,
    mut rx_commandes: mpsc::UnboundedReceiver<CommandeMoteur>,
) {
    let mut session: Option<JoinHandle<()>> = None;

    while let Some(commande) = rx_commandes.recv().await {
        if let Some(ancienne) = session.take() {
            ancienne.abort();
        }

        let CommandeMoteur::Demarrer {
            generation,
            mode,
            date_debut,
            date_fin,
            chemin_fichier,
            symboles_test,
            parametrages,
            fournisseurs,
        } = commande
        else {
            continue;
        };

        let ctx = Contexte {
            config: Arc::clone(&config),
            tx_ui: tx_ui.clone(),
            tx_statut: tx_statut.clone(),
            generation,
            parametrages,
            fournisseurs,
        };
        session = Some(tokio::spawn(async move {
            let resultat = match mode {
                ModeExecution::TempsReel => ctx.temps_reel().await,
                ModeExecution::Simulation => ctx.simulation().await,
                ModeExecution::BacktestPeriode => {
                    ctx.test_periode(&date_debut, &date_fin, &symboles_test)
                        .await
                }
                ModeExecution::BacktestFichier => {
                    ctx.test_fichier(&chemin_fichier, &symboles_test).await
                }
            };
            if let Err(erreur) = resultat {
                error!("{erreur}");
                ctx.echec(erreur);
            }
        }));
    }
}

impl Contexte {
    fn statut(&self, texte: impl Into<String>) {
        let _ = self
            .tx_statut
            .send((self.generation, NotificationMoteur::Statut(texte.into())));
    }

    fn erreur(&self, texte: impl Into<String>) {
        let _ = self
            .tx_statut
            .send((self.generation, NotificationMoteur::Erreur(texte.into())));
    }

    fn echec(&self, texte: impl Into<String>) {
        let _ = self
            .tx_statut
            .send((self.generation, NotificationMoteur::Echec(texte.into())));
    }

    fn delai_reseau(&self) -> Option<Duration> {
        Some(Duration::from_millis(
            self.config.reseau.delai_limitation_debit_ms,
        ))
    }

    async fn temps_reel(&self) -> Result<(), String> {
        let symboles = &self.config.paires.symboles;
        let fournisseurs = &self.fournisseurs;

        self.statut("Téléchargement de l'historique multi-sources...");
        let mut bougies_sources = Vec::new();
        let mut erreurs_sources = Vec::new();
        for symbole in symboles {
            let unite = &self.parametrages.resoudre(symbole).unite_temps;
            for fournisseur in fournisseurs {
                info!(
                    "Téléchargement de l'historique {} pour {}...",
                    fournisseur.libelle(),
                    symbole
                );
                let delai = self.delai_reseau().unwrap_or_default();
                let resultat = match fournisseur {
                    Fournisseur::Binance => {
                        telecharger_bougies_periode(
                            symbole,
                            unite,
                            None,
                            None,
                            BOUGIES_PRECHAUFFAGE + 1,
                            self.delai_reseau(),
                        )
                        .await
                    }
                    Fournisseur::Kraken => {
                        crate::data::kraken::telecharger_historique(
                            symbole,
                            unite,
                            BOUGIES_PRECHAUFFAGE,
                            delai,
                        )
                        .await
                    }
                    Fournisseur::Coinbase => {
                        crate::data::coinbase::telecharger_historique(
                            symbole,
                            unite,
                            BOUGIES_PRECHAUFFAGE,
                            delai,
                        )
                        .await
                    }
                };
                match resultat {
                    Ok(bougies) => {
                        bougies_sources.extend(bougies.into_iter().map(|bougie| BougieSourcee {
                            fournisseur: *fournisseur,
                            bougie,
                        }))
                    }
                    Err(erreur) => {
                        let message = format!(
                            "Historique {} indisponible pour {symbole} : {erreur}",
                            fournisseur.libelle()
                        );
                        warn!("{message}");
                        self.erreur(message.clone());
                        erreurs_sources.push(message);
                    }
                }
            }
        }

        let bougies_initiales: Vec<_> = bougies_sources
            .iter()
            .filter(|bougie| !bougie.bougie.cloturee)
            .cloned()
            .collect();
        let bougies_agregees = agreger_historique(bougies_sources, fournisseurs);
        let mut historiques = Historiques::new();
        for symbole in symboles {
            let mut bougies: Vec<_> = bougies_agregees
                .iter()
                .filter(|bougie| bougie.symbole.eq_ignore_ascii_case(symbole))
                .cloned()
                .collect();
            if bougies.len() < BOUGIES_PRECHAUFFAGE {
                let mut message = format!(
                    "Historique multi-sources insuffisant pour {symbole} : {} bougies agrégées sur {BOUGIES_PRECHAUFFAGE} requises",
                    bougies.len()
                );
                if !erreurs_sources.is_empty() {
                    message.push_str(&format!("; erreurs : {}", erreurs_sources.join(" | ")));
                }
                return Err(message);
            }
            bougies.drain(..bougies.len() - BOUGIES_PRECHAUFFAGE);
            historiques.insert(symbole.to_uppercase(), bougies);
        }

        if !self.publier_dernieres(&historiques).await {
            return Ok(());
        }
        let libelles = fournisseurs
            .iter()
            .map(|fournisseur| fournisseur.libelle())
            .collect::<Vec<_>>()
            .join(", ");
        self.statut(format!("🔴 Flux en direct : {libelles}"));
        info!("Flux multi-sources lancé. Traitement en temps réel activé...");

        let (tx_bougies, rx_bougies) = mpsc::channel::<BougieSourcee>(500);
        let mut flux = tokio::task::JoinSet::new();
        for (unite, groupe) in self.parametrages.par_unite(symboles) {
            for fournisseur in fournisseurs {
                match fournisseur {
                    Fournisseur::Binance => {
                        let (tx_binance, mut rx_binance) = mpsc::channel::<Bougie>(100);
                        let tx = tx_bougies.clone();
                        let groupe_ecoute = groupe.clone();
                        let unite_ecoute = unite.clone();
                        flux.spawn(async move {
                            ecouter_flux_temps_reel(&groupe_ecoute, &unite_ecoute, tx_binance).await
                        });
                        flux.spawn(async move {
                            while let Some(bougie) = rx_binance.recv().await {
                                if tx
                                    .send(BougieSourcee {
                                        fournisseur: Fournisseur::Binance,
                                        bougie,
                                    })
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }
                        });
                    }
                    Fournisseur::Kraken => {
                        let tx = tx_bougies.clone();
                        let groupe = groupe.clone();
                        let unite = unite.clone();
                        flux.spawn(async move {
                            crate::data::kraken::ecouter_flux_temps_reel(&groupe, &unite, tx).await
                        });
                    }
                    Fournisseur::Coinbase => {
                        let tx = tx_bougies.clone();
                        let groupe = groupe.clone();
                        let unite = unite.clone();
                        flux.spawn(async move {
                            crate::data::coinbase::ecouter_flux_temps_reel(&groupe, &unite, tx)
                                .await
                        });
                    }
                }
            }
        }
        drop(tx_bougies);
        tokio::select! {
            _ = flux.join_all() => {}
            _ = self.traiter_flux_agrege(&mut historiques, rx_bougies, fournisseurs, bougies_initiales) => {}
        }
        Ok(())
    }

    async fn traiter_flux_agrege(
        &self,
        historiques: &mut Historiques,
        mut rx: mpsc::Receiver<BougieSourcee>,
        fournisseurs: &[Fournisseur],
        bougies_initiales: Vec<BougieSourcee>,
    ) {
        let mut agregateur = Agregateur::nouveau(fournisseurs, Duration::from_secs(5));
        for bougie_sourcee in bougies_initiales {
            if let Some(bougie) = agregateur.recevoir(bougie_sourcee)
                && self
                    .traiter_bougie(historiques, bougie, false)
                    .await
                    .is_none()
            {
                return;
            }
        }
        let mut verification_delai = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! {
                reception = rx.recv() => {
                    let Some(bougie_sourcee) = reception else { return };
                    if let Some(bougie) = agregateur.recevoir(bougie_sourcee)
                        && self.traiter_bougie(historiques, bougie, false).await.is_none()
                    {
                        return;
                    }
                }
                _ = verification_delai.tick() => {
                    for bougie in agregateur.emettre_expirees() {
                        if self.traiter_bougie(historiques, bougie, false).await.is_none() {
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn simulation(&self) -> Result<(), String> {
        let symboles = &self.config.paires.symboles;

        let mut historiques: Historiques = symboles
            .iter()
            .map(|s| {
                (
                    s.to_uppercase(),
                    generer_historique_fictif(
                        s,
                        &self.parametrages.resoudre(s).unite_temps,
                        BOUGIES_PRECHAUFFAGE,
                    ),
                )
            })
            .collect();

        if !self.publier_dernieres(&historiques).await {
            return Ok(());
        }
        self.statut("🟡 Simulation locale : prix fictifs");

        let (tx_bougies, rx_bougies) = mpsc::channel::<Bougie>(100);
        let mut flux = tokio::task::JoinSet::new();
        for (unite, groupe) in self.parametrages.par_unite(symboles) {
            let tx = tx_bougies.clone();
            flux.spawn(async move { simuler_flux_temps_reel(&groupe, &unite, tx).await });
        }
        drop(tx_bougies);
        tokio::select! {
            _ = flux.join_all() => {}
            _ = self.traiter_flux(&mut historiques, rx_bougies) => {}
        }
        Ok(())
    }

    /// Télécharge sur Binance la plage `[debut, fin]` de chaque paire puis la rejoue bougie par bougie.
    async fn test_periode(
        &self,
        debut: &str,
        fin: &str,
        symboles: &[String],
    ) -> Result<(), String> {
        let (debut_ms, fin_ms) = plage_en_millisecondes(debut, fin)?;
        if symboles.is_empty() {
            return Err("Sélectionnez au moins une paire à jouer.".into());
        }

        let mut historiques = Historiques::new();
        let mut rejeu = Vec::new();
        let mut sauvegarde: Vec<Bougie> = Vec::new();
        for (i, symbole) in symboles.iter().enumerate() {
            let unite = &self.parametrages.resoudre(symbole).unite_temps;
            self.statut(format!(
                "Téléchargement de {symbole} ({}/{}) du {debut} au {fin}...",
                i + 1,
                symboles.len()
            ));

            let mut prechauffage = telecharger_bougies_periode(
                symbole,
                unite,
                None,
                Some(debut_ms - 1),
                BOUGIES_PRECHAUFFAGE,
                self.delai_reseau(),
            )
            .await
            .map_err(|e| format!("Préchauffage impossible pour {symbole} : {e}"))?;
            prechauffage.retain(|b| b.cloturee);

            let mut plage =
                telecharger_plage_bougies(symbole, unite, debut_ms, fin_ms, self.delai_reseau())
                    .await
                    .map_err(|e| format!("Téléchargement impossible pour {symbole} : {e}"))?;
            plage.retain(|b| b.cloturee);
            if plage.is_empty() {
                return Err(format!(
                    "Aucune donnée Binance pour {symbole} du {debut} au {fin}"
                ));
            }

            historiques.insert(symbole.to_uppercase(), prechauffage.clone());
            sauvegarde.extend(prechauffage);
            sauvegarde.extend(plage.iter().cloned());
            rejeu.extend(plage);
        }

        // Le jeu de données est conservé pour pouvoir le rejouer via le mode « Fichier Local ».
        let fichier = format!("historique_{}_{}.json", debut.trim(), fin.trim());
        let json = serde_json::to_string(&sauvegarde)
            .map_err(|e| format!("Sérialisation impossible : {e}"))?;
        tokio::fs::write(&fichier, json)
            .await
            .map_err(|e| format!("Écriture de '{fichier}' impossible : {e}"))?;
        info!("Jeu de données enregistré dans {fichier}");

        self.rejouer(
            historiques,
            rejeu,
            &format!("du {debut} au {fin} (données dans {fichier})"),
        )
        .await
    }

    /// Rejoue un fichier JSON contenant une liste de bougies.
    async fn test_fichier(&self, chemin: &str, symboles: &[String]) -> Result<(), String> {
        if !chemin.to_lowercase().ends_with(".json") {
            return Err("Seuls les fichiers JSON (liste de bougies) sont pris en charge".into());
        }
        if symboles.is_empty() {
            return Err("Sélectionnez au moins une paire à jouer.".into());
        }
        self.statut(format!("Lecture de {chemin}..."));

        let contenu = tokio::fs::read_to_string(chemin)
            .await
            .map_err(|e| format!("Lecture de '{chemin}' impossible : {e}"))?;
        let bougies: Vec<Bougie> = serde_json::from_str(&contenu)
            .map_err(|e| format!("Fichier '{chemin}' invalide : {e}"))?;
        let mut bougies = filtrer_bougies_par_symboles(bougies, symboles, chemin)?;
        for bougie in &mut bougies {
            bougie.cloturee = true;
        }

        let mut historiques = Historiques::new();
        for bougie in &bougies {
            historiques
                .entry(bougie.symbole.to_uppercase())
                .or_default();
        }
        self.rejouer(historiques, bougies, chemin).await
    }

    /// Analyse les bougies dans l'ordre chronologique, toutes paires confondues.
    async fn rejouer(
        &self,
        mut historiques: Historiques,
        mut bougies: Vec<Bougie>,
        description: &str,
    ) -> Result<(), String> {
        bougies.sort_by_key(|b| b.instant_ouverture);
        let total = bougies.len();
        self.statut(format!("Analyse de {total} bougies {description}..."));

        let mut alertes = 0;
        for bougie in bougies {
            match self.traiter_bougie(&mut historiques, bougie, true).await {
                Some(true) => alertes += 1,
                Some(false) => {}
                None => return Ok(()),
            }
        }

        self.statut(format!(
            "✅ Test terminé {description} : {total} bougies analysées, {alertes} alertes"
        ));
        Ok(())
    }

    async fn traiter_flux(&self, historiques: &mut Historiques, mut rx: mpsc::Receiver<Bougie>) {
        while let Some(bougie) = rx.recv().await {
            if self
                .traiter_bougie(historiques, bougie, false)
                .await
                .is_none()
            {
                return;
            }
        }
    }

    /// Envoie à l'IHM la dernière bougie clôturée de chaque paire. `false` si l'IHM est fermée.
    async fn publier_dernieres(&self, historiques: &Historiques) -> bool {
        for historique in historiques.values() {
            if let Some(derniere) = historique.last() {
                let signal = construire_signal(
                    historique,
                    derniere,
                    self.parametrages.resoudre(&derniere.symbole),
                    self.generation,
                    false,
                );
                if self.tx_ui.send(signal).await.is_err() {
                    return false;
                }
            }
        }
        true
    }

    /// Analyse une bougie et envoie le signal à l'IHM. Retourne `None` si l'IHM est fermée,
    /// sinon `Some(est_alerte)`.
    async fn traiter_bougie(
        &self,
        historiques: &mut Historiques,
        bougie: Bougie,
        rejeu: bool,
    ) -> Option<bool> {
        let historique = historiques.get_mut(&bougie.symbole.to_uppercase())?;

        // Une bougie en cours (mise à jour ~toutes les 2 s) n'est analysée qu'à titre provisoire.
        let provisoire;
        let serie: &[Bougie] = if bougie.cloturee {
            historique.push(bougie.clone());
            if historique.len() > HISTORIQUE_MAX {
                historique.remove(0);
            }
            historique.as_slice()
        } else {
            provisoire = [historique.as_slice(), std::slice::from_ref(&bougie)].concat();
            &provisoire
        };

        let signal = construire_signal(
            serie,
            &bougie,
            self.parametrages.resoudre(&bougie.symbole),
            self.generation,
            rejeu,
        );
        let est_alerte = signal.est_alerte;
        self.tx_ui.send(signal).await.ok()?;
        Some(est_alerte)
    }
}

fn filtrer_bougies_par_symboles(
    mut bougies: Vec<Bougie>,
    symboles: &[String],
    chemin: &str,
) -> Result<Vec<Bougie>, String> {
    if bougies.is_empty() {
        return Err(format!("Le fichier '{chemin}' ne contient aucune bougie"));
    }
    if symboles.is_empty() {
        return Err("Sélectionnez au moins une paire à jouer.".into());
    }

    let symboles_demandes: HashSet<String> = symboles.iter().map(|s| s.to_uppercase()).collect();
    let symboles_fichier: HashSet<String> =
        bougies.iter().map(|b| b.symbole.to_uppercase()).collect();
    let absents: Vec<String> = symboles_demandes
        .difference(&symboles_fichier)
        .cloned()
        .collect();
    if !absents.is_empty() {
        return Err(format!(
            "Paire(s) absente(s) du fichier '{chemin}' : {}",
            absents.join(", ")
        ));
    }

    bougies.retain(|b| symboles_demandes.contains(&b.symbole.to_uppercase()));
    Ok(bougies)
}

/// Convertit deux dates `AAAA-MM-JJ` en bornes UTC en millisecondes (fin incluse, jusqu'à 23:59:59.999).
fn plage_en_millisecondes(debut: &str, fin: &str) -> Result<(i64, i64), String> {
    let lire = |texte: &str, nom: &str| {
        NaiveDate::parse_from_str(texte.trim(), "%Y-%m-%d")
            .map_err(|_| format!("Date de {nom} invalide : « {texte} » (format AAAA-MM-JJ)"))
    };
    let debut = lire(debut, "début")?;
    let fin = lire(fin, "fin")?;
    if fin < debut {
        return Err("La date de fin précède la date de début".into());
    }

    let minuit = |date: NaiveDate| {
        date.and_hms_opt(0, 0, 0)
            .map(|dt| dt.and_utc().timestamp_millis())
            .ok_or_else(|| "Date hors limites".to_string())
    };
    let lendemain = fin.succ_opt().ok_or("Date hors limites")?;
    Ok((minuit(debut)?, minuit(lendemain)? - 1))
}

fn construire_signal(
    historique: &[Bougie],
    bougie: &Bougie,
    strategie: &ConfigurationStrategie,
    generation: u64,
    rejeu: bool,
) -> SignalVisuel {
    let prix: Vec<f64> = historique.iter().map(|b| b.cloture).collect();
    let volumes: Vec<f64> = historique.iter().map(|b| b.volume).collect();
    let periode_volume = strategie.volume.periode_moyenne_mobile as usize;

    let emas = calculer_ema(&prix, strategie.filtre_macro.periode_ema as usize);
    let valeur_adx = calculer_adx(historique, strategie.adx.periode as usize).unwrap_or(0.0);
    let (volume_valide, ratio_vol) = valider_cassure_volume(
        bougie.volume,
        &volumes,
        periode_volume,
        strategie.volume.multiplicateur,
    );

    let dernier_ema = emas.as_ref().and_then(|v| v.last().copied());
    // Avec Supertrend activé, il doit confirmer le sens donné par l'EMA ; sans donnée, pas de tendance.
    let supertrend = if strategie.filtre_macro.utiliser_supertrend {
        supertrend_haussier(
            historique,
            strategie.filtre_macro.periode_supertrend as usize,
            strategie.filtre_macro.multiplicateur_supertrend,
        )
    } else {
        None
    };
    let confirme = |haussier: bool| {
        !strategie.filtre_macro.utiliser_supertrend || supertrend == Some(haussier)
    };
    let tendance = match dernier_ema {
        Some(ema) if bougie.cloture > ema && confirme(true) => Tendance::Haussiere,
        Some(ema) if bougie.cloture < ema && confirme(false) => Tendance::Baissiere,
        _ => Tendance::Neutre,
    };
    let adx_valide = valeur_adx >= strategie.adx.seuil_minimal;
    let analyse = AnalyseTechnique {
        tendance,
        valeur_adx,
        adx_valide,
        moyenne_volume: calculer_sma(&volumes, periode_volume).unwrap_or(0.0),
        volume_valide,
    };
    let score_min = strategie.score_min_alerte.min(100) as u8;
    let (score, est_une_alerte) = calculer_score(&analyse, score_min);

    if !rejeu {
        if est_une_alerte {
            info!(
                "🚨 ALERTE {:?} | Symbole: {} | Score: {}/100 | Prix: {:.2} | ADX: {:.1} | Vol Ratio: {:.2}x",
                tendance, bougie.symbole, score, bougie.cloture, valeur_adx, ratio_vol
            );
        } else {
            info!(
                "📊 ANALYSE | Symbole: {} | Score: {}/100 | Prix: {:.2} | ADX: {:.1} | Vol: {}",
                bougie.symbole, score, bougie.cloture, valeur_adx, volume_valide
            );
        }
    }

    // En test, l'horodatage est celui de la bougie rejouée ; en direct, l'heure courante.
    let horodatage = if rejeu {
        bougie
            .instant_ouverture
            .with_timezone(&Local)
            .format("%d/%m/%Y %H:%M:%S")
    } else {
        Local::now().format("%d/%m/%Y %H:%M:%S")
    }
    .to_string();

    SignalVisuel {
        generation,
        symbole: bougie.symbole.to_uppercase(),
        prix: bougie.cloture,
        prix_precedent: historique
            .len()
            .checked_sub(2)
            .map_or(bougie.cloture, |i| historique[i].cloture),
        score,
        adx: valeur_adx,
        adx_valide,
        ratio_volume: ratio_vol,
        volume_valide,
        tendance,
        est_alerte: est_une_alerte,
        unite: bougie.unite_temps.clone(),
        horodatage,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bougie_test(symbole: &str) -> Bougie {
        Bougie {
            symbole: symbole.to_string(),
            unite_temps: "1d".to_string(),
            instant_ouverture: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            ouverture: 1.0,
            haut: 1.0,
            bas: 1.0,
            cloture: 1.0,
            volume: 1.0,
            cloturee: true,
        }
    }

    #[test]
    fn convertit_la_plage_de_dates_en_millisecondes_utc() {
        let (debut, fin) = plage_en_millisecondes("2026-01-01", "2026-01-01").unwrap();
        assert_eq!(debut, 1_767_225_600_000);
        assert_eq!(fin - debut, 86_400_000 - 1);
    }

    #[test]
    fn rejette_les_plages_invalides() {
        assert!(plage_en_millisecondes("2026-02-01", "2026-01-01").is_err());
        assert!(plage_en_millisecondes("01/01/2026", "2026-01-02").is_err());
        assert!(plage_en_millisecondes("2026-01-01", "").is_err());
    }

    #[test]
    fn filtre_le_fichier_local_selon_les_paires_selectionnees() {
        let bougies = vec![
            bougie_test("BTCEUR"),
            bougie_test("ETHEUR"),
            bougie_test("SOLEUR"),
        ];
        let selection = vec!["btceur".to_string(), "SOLEUR".to_string()];

        let filtrees =
            filtrer_bougies_par_symboles(bougies, &selection, "historique.json").unwrap();

        assert_eq!(
            filtrees
                .iter()
                .map(|b| b.symbole.as_str())
                .collect::<Vec<_>>(),
            vec!["BTCEUR", "SOLEUR"]
        );
    }

    #[test]
    fn signale_les_paires_selectionnees_absentes_du_fichier() {
        let erreur = filtrer_bougies_par_symboles(
            vec![bougie_test("BTCEUR")],
            &["SOLEUR".to_string()],
            "historique.json",
        )
        .unwrap_err();

        assert!(erreur.contains("SOLEUR"));
    }
}
