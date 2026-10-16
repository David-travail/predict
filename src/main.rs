// Sous Windows l'exe est une application « fenêtre » : un double-clic n'ouvre pas de console.
// En ligne de commande, on se rattache à la console du terminal parent (voir `rattacher_console`).
#![cfg_attr(windows, windows_subsystem = "windows")]

mod config;
mod core;
mod data;
mod indicators;
mod ui;

use crate::config::{ConfigurationApplication, ConfigurationStrategie, Parametrages};
use crate::core::modeles::{AnalyseTechnique, Bougie, Tendance};
use crate::core::scoring::calculer_score;
use crate::data::rest::{telecharger_bougies_periode, telecharger_plage_bougies};
use crate::data::simulation::{generer_historique_fictif, simuler_flux_temps_reel};
use crate::data::websocket::ecouter_flux_temps_reel;
use crate::indicators::adx::calculer_adx;
use crate::indicators::ema::calculer_ema;
use crate::indicators::supertrend::supertrend_haussier;
use crate::indicators::volume::{calculer_sma, valider_cassure_volume};
use crate::ui::{CommandeMoteur, ModeExecution, SignalVisuel};

use chrono::{Local, NaiveDate};
use clap::{Parser, ValueEnum};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

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

/// Détecteur de tendances crypto : interface graphique par défaut, ou console avec `--console`.
#[derive(Parser)]
#[command(version, about)]
struct Arguments {
    /// Lance en mode console, sans interface graphique
    #[arg(long)]
    console: bool,
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
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    info!("Démarrage du Détecteur Crypto H24...");

    // 2. Chargement de la configuration
    let config = charger_configuration(arguments.config.as_ref())?;
    info!("Paires surveillées : {:?}", config.paires.symboles);
    crate::data::reseau::definir_proxy(config.reseau.proxy.clone());

    let chemin_parametrages = chemin_parametrages(arguments.config.as_ref());
    let parametrages = charger_parametrages(&config, &chemin_parametrages);
    let parametrages_ui = parametrages.clone();
    let symboles_ui = config.paires.symboles.clone();

    // Le moteur tourne sur la runtime Tokio ; egui occupe le thread principal.
    let runtime = tokio::runtime::Runtime::new()?;
    let (tx_ui, rx_ui) = mpsc::channel::<SignalVisuel>(256);
    let (tx_commandes, rx_commandes) = mpsc::unbounded_channel::<CommandeMoteur>();
    let (tx_statut, rx_statut) = mpsc::unbounded_channel::<(u64, String)>();

    let (mode, date_debut, date_fin, chemin_fichier) = if arguments.console {
        parametres_console(&arguments)?
    } else {
        (
            ModeExecution::TempsReel,
            String::new(),
            String::new(),
            String::new(),
        )
    };

    // Génération 0 : première session, démarrée avant l'ouverture de l'IHM.
    tx_commandes.send(CommandeMoteur::Demarrer {
        generation: 0,
        mode,
        date_debut,
        date_fin,
        chemin_fichier,
        symboles_test: config.paires.symboles.clone(),
        parametrages,
    })?;
    runtime.spawn(moteur(Arc::new(config), tx_ui, tx_statut, rx_commandes));

    if arguments.console {
        let resultat = runtime.block_on(boucle_console(mode, rx_ui, rx_statut));
        drop(tx_commandes);
        runtime.shutdown_background();
        return resultat.map_err(Into::into);
    }

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
                    rx_ui,
                    parametrages_ui,
                    symboles_ui,
                    chemin_parametrages,
                )
                .avec_moteur(tx_commandes, rx_statut),
            )
        }),
    )?;

    runtime.shutdown_background();
    Ok(())
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
    mut rx_statut: mpsc::UnboundedReceiver<(u64, String)>,
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
            statut = rx_statut.recv() => {
                let Some((_, texte)) = statut else { return Ok(()) };
                println!("{texte}");
                if texte.starts_with('❌') {
                    return Err(texte);
                }
                if texte.starts_with('✅') {
                    return Ok(());
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
    tx_statut: mpsc::UnboundedSender<(u64, String)>,
    generation: u64,
    parametrages: Parametrages,
}

type Historiques = HashMap<String, Vec<Bougie>>;

/// Moteur : exécute une session à la fois et remplace la précédente à chaque commande de l'IHM.
async fn moteur(
    config: Arc<ConfigurationApplication>,
    tx_ui: mpsc::Sender<SignalVisuel>,
    tx_statut: mpsc::UnboundedSender<(u64, String)>,
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
                ctx.statut(format!("❌ {erreur}"));
            }
        }));
    }
}

impl Contexte {
    fn statut(&self, texte: impl Into<String>) {
        let _ = self.tx_statut.send((self.generation, texte.into()));
    }

    fn delai_reseau(&self) -> Option<Duration> {
        Some(Duration::from_millis(
            self.config.reseau.delai_limitation_debit_ms,
        ))
    }

    async fn temps_reel(&self) -> Result<(), String> {
        let symboles = &self.config.paires.symboles;

        self.statut("Téléchargement de l'historique Binance...");
        let mut historiques = Historiques::new();
        for symbole in symboles {
            info!("Téléchargement de l'historique Binance pour {}...", symbole);
            let unite = &self.parametrages.resoudre(symbole).unite_temps;
            let mut bougies = telecharger_bougies_periode(
                symbole,
                unite,
                None,
                None,
                BOUGIES_PRECHAUFFAGE,
                self.delai_reseau(),
            )
            .await
            .map_err(|e| format!("Historique Binance indisponible pour {symbole} : {e}"))?;
            bougies.retain(|b| b.cloturee);
            if bougies.is_empty() {
                return Err(format!(
                    "Binance n'a renvoyé aucune bougie clôturée pour {symbole}"
                ));
            }
            historiques.insert(symbole.to_uppercase(), bougies);
        }

        if !self.publier_dernieres(&historiques).await {
            return Ok(());
        }
        self.statut("🔴 Flux Binance en direct");
        info!("Flux Binance lancé. Traitement en temps réel activé...");

        let (tx_bougies, rx_bougies) = mpsc::channel::<Bougie>(100);
        let mut flux = tokio::task::JoinSet::new();
        for (unite, groupe) in self.parametrages.par_unite(symboles) {
            let tx = tx_bougies.clone();
            flux.spawn(async move { ecouter_flux_temps_reel(&groupe, &unite, tx).await });
        }
        drop(tx_bougies);
        tokio::select! {
            _ = flux.join_all() => {}
            _ = self.traiter_flux(&mut historiques, rx_bougies) => {}
        }
        Ok(())
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
