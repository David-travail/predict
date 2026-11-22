//! Parsing et validation de la configuration de l'application à partir de `config.toml`.

use crate::data::sources::Fournisseur;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::error::Error;
use std::fs;
use std::path::Path;

/// Configuration racine de l'application, reflétant la structure de `config.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationApplication {
    #[serde(rename = "pairs")]
    pub paires: ConfigurationPaires,
    #[serde(rename = "timeframes")]
    pub unites_temps: ConfigurationUnitesTemps,
    #[serde(rename = "strategy")]
    pub strategie: ConfigurationStrategie,
    #[serde(rename = "network")]
    pub reseau: ConfigurationReseau,
    #[serde(rename = "data_sources", default)]
    pub sources_donnees: ConfigurationSources,
    #[serde(rename = "telegram", default)]
    pub telegram: ConfigurationTelegram,
    #[serde(rename = "logs", default)]
    pub journaux: ConfigurationJournaux,
    /// Blocs de paires (`[groups.<nom>]`), chacun avec son paramétrage optionnel.
    #[serde(rename = "groups", default)]
    pub groupes: BTreeMap<String, ConfigurationGroupe>,
    /// Paramétrage propre à une paire (`[overrides.<SYMBOLE>]`).
    #[serde(rename = "overrides", default)]
    pub surcharges: BTreeMap<String, ConfigurationStrategie>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigurationTelegram {
    #[serde(rename = "bot_token")]
    pub jeton_bot: String,
    #[serde(rename = "chat_id")]
    pub identifiant_chat: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigurationJournaux {
    #[serde(rename = "retention_days")]
    pub jours_retention: usize,
}

impl Default for ConfigurationJournaux {
    fn default() -> Self {
        Self { jours_retention: 7 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigurationSources {
    #[serde(rename = "providers")]
    pub fournisseurs: Vec<Fournisseur>,
}

impl Default for ConfigurationSources {
    /// Construit la configuration de sources avec les trois fournisseurs actifs.
    ///
    /// Sortie : configuration par défaut contenant Binance, Kraken et Coinbase.
    fn default() -> Self {
        Self {
            fournisseurs: vec![
                Fournisseur::Binance,
                Fournisseur::Kraken,
                Fournisseur::Coinbase,
            ],
        }
    }
}

/// Liste des paires/symboles à surveiller (section `[pairs]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationPaires {
    #[serde(rename = "symbols")]
    pub symboles: Vec<String>,
}

/// Configuration des timeframes (section `[timeframes]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationUnitesTemps {
    #[serde(rename = "execution")]
    pub unite_execution: String,
}

/// Configuration de la stratégie (section `[strategy]` et ses sous-tables).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationStrategie {
    #[serde(rename = "min_score_alert")]
    pub score_min_alerte: u32,
    #[serde(rename = "macro_filter")]
    pub filtre_macro: ConfigurationMacro,
    pub adx: ConfigurationAdx,
    pub volume: ConfigurationVolume,
    /// Période des bougies (ex. `1h`) propre à ce paramétrage.
    #[serde(rename = "timeframe", default = "unite_temps_defaut")]
    pub unite_temps: String,
}

/// Fournit l'unité de temps utilisée par défaut lorsqu'elle n'est pas configurée.
///
/// Sortie : chaîne `1h`.
fn unite_temps_defaut() -> String {
    "1h".to_string()
}

/// Filtre macro basé sur l'EMA / Supertrend (section `[strategy.macro_filter]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationMacro {
    #[serde(rename = "ema_period")]
    pub periode_ema: u32,
    #[serde(rename = "use_supertrend")]
    pub utiliser_supertrend: bool,
    #[serde(rename = "supertrend_period", default = "periode_supertrend_defaut")]
    pub periode_supertrend: u32,
    #[serde(
        rename = "supertrend_multiplier",
        default = "multiplicateur_supertrend_defaut"
    )]
    pub multiplicateur_supertrend: f64,
}

/// Fournit la période Supertrend par défaut.
///
/// Sortie : période de 10 bougies.
fn periode_supertrend_defaut() -> u32 {
    10
}

/// Fournit le multiplicateur Supertrend par défaut.
///
/// Sortie : multiplicateur `3.0`.
fn multiplicateur_supertrend_defaut() -> f64 {
    3.0
}

/// Configuration de l'indicateur ADX (section `[strategy.adx]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationAdx {
    #[serde(rename = "period")]
    pub periode: u32,
    #[serde(rename = "min_threshold")]
    pub seuil_minimal: f64,
}

/// Configuration du filtre de volume (section `[strategy.volume]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationVolume {
    #[serde(rename = "sma_period")]
    pub periode_moyenne_mobile: u32,
    #[serde(rename = "multiplier")]
    pub multiplicateur: f64,
}

/// Configuration réseau (section `[network]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationReseau {
    #[serde(rename = "rate_limit_ms")]
    pub delai_limitation_debit_ms: u64,
    /// Proxy HTTP optionnel (ex. `http://127.0.0.1:9000`) ; à défaut, variables d'environnement.
    #[serde(default)]
    pub proxy: Option<String>,
}

impl ConfigurationApplication {
    /// Charge et parse la configuration depuis un fichier TOML sur disque,
    /// puis la valide.
    ///
    /// Entrée : chemin du fichier TOML.
    /// Sortie : configuration validée ou erreur de lecture, de parsing ou de validation.
    pub fn charger_config(chemin: &Path) -> Result<Self, Box<dyn Error>> {
        let contenu = fs::read_to_string(chemin).map_err(|erreur| {
            format!(
                "Impossible de lire le fichier de config '{}': {erreur}",
                chemin.display()
            )
        })?;

        let configuration: ConfigurationApplication =
            toml::from_str(&contenu).map_err(|erreur| {
                format!(
                    "Erreur de parsing TOML dans '{}': {erreur}",
                    chemin.display()
                )
            })?;

        configuration.valider()?;

        Ok(configuration)
    }

    /// Parse et valide une configuration TOML déjà en mémoire.
    ///
    /// Entrée : contenu TOML.
    /// Sortie : configuration validée ou erreur de parsing/validation.
    pub fn depuis_toml(contenu: &str) -> Result<Self, Box<dyn Error>> {
        let configuration: ConfigurationApplication = toml::from_str(contenu)?;
        configuration.valider()?;
        Ok(configuration)
    }

    /// Complète les identifiants Telegram depuis des sources hors dépôt, puis revalide.
    ///
    /// Priorité : variables `PREDICT_TELEGRAM_TOKEN` / `PREDICT_TELEGRAM_CHAT_ID`, puis le
    /// fichier `.telegram.toml` (section `[telegram]`, ignoré par git) du dossier indiqué,
    /// puis la section `[telegram]` de `config.toml`.
    ///
    /// Entrée : dossier où chercher `.telegram.toml`.
    /// Sortie : `Ok(())`, ou erreur de lecture/parsing du fichier ou de validation.
    pub fn appliquer_secrets_telegram(&mut self, dossier: &Path) -> Result<(), Box<dyn Error>> {
        #[derive(Deserialize)]
        struct FichierSecrets {
            #[serde(default)]
            telegram: ConfigurationTelegram,
        }

        let chemin = dossier.join(".telegram.toml");
        if chemin.is_file() {
            let contenu = fs::read_to_string(&chemin).map_err(|erreur| {
                format!("Impossible de lire '{}': {erreur}", chemin.display())
            })?;
            let secrets: FichierSecrets = toml::from_str(&contenu).map_err(|erreur| {
                format!("Erreur de parsing TOML dans '{}': {erreur}", chemin.display())
            })?;
            if !secrets.telegram.jeton_bot.trim().is_empty() {
                self.telegram.jeton_bot = secrets.telegram.jeton_bot;
            }
            if !secrets.telegram.identifiant_chat.trim().is_empty() {
                self.telegram.identifiant_chat = secrets.telegram.identifiant_chat;
            }
        }

        if let Ok(jeton) = std::env::var("PREDICT_TELEGRAM_TOKEN") {
            if !jeton.trim().is_empty() {
                self.telegram.jeton_bot = jeton;
            }
        }
        if let Ok(chat) = std::env::var("PREDICT_TELEGRAM_CHAT_ID") {
            if !chat.trim().is_empty() {
                self.telegram.identifiant_chat = chat;
            }
        }

        self.valider()
    }

    /// Valide la cohérence des valeurs de configuration.
    ///
    /// Entrée : configuration courante.
    /// Sortie : `Ok(())` si les paires, fournisseurs, périodes et stratégies sont valides.
    pub fn valider(&self) -> Result<(), Box<dyn Error>> {
        if self.paires.symboles.is_empty() {
            return Err("La liste 'pairs.symbols' ne peut pas être vide".into());
        }

        if self.unites_temps.unite_execution.trim().is_empty() {
            return Err("'timeframes.execution' ne peut pas être vide".into());
        }

        if self.sources_donnees.fournisseurs.is_empty() {
            return Err("'data_sources.providers' ne peut pas être vide".into());
        }
        let fournisseurs: std::collections::HashSet<_> =
            self.sources_donnees.fournisseurs.iter().collect();
        if fournisseurs.len() != self.sources_donnees.fournisseurs.len() {
            return Err("'data_sources.providers' ne peut pas contenir de doublons".into());
        }

        let jeton_telegram_configure = !self.telegram.jeton_bot.trim().is_empty();
        let chat_telegram_configure = !self.telegram.identifiant_chat.trim().is_empty();
        if jeton_telegram_configure != chat_telegram_configure {
            return Err(
                "'telegram.bot_token' et 'telegram.chat_id' doivent être configurés ensemble"
                    .into(),
            );
        }

        if self.journaux.jours_retention == 0 {
            return Err("'logs.retention_days' doit être supérieur à 0".into());
        }

        self.parametrages().valider()
    }

    /// Construit les paramétrages (défaut + réglages de paires) depuis la configuration.
    ///
    /// Entrée : configuration courante.
    /// Sortie : défaut issu de `[strategy]` ; les paires d'un bloc `[groups]` reçoivent le réglage
    /// du bloc, les `[overrides]` restant prioritaires.
    pub fn parametrages(&self) -> Parametrages {
        let mut generique = self.strategie.clone();
        generique.unite_temps = self.unites_temps.unite_execution.clone();
        Parametrages::depuis_niveaux(generique, &self.groupes, &self.surcharges)
    }
}

impl ConfigurationStrategie {
    /// Valide les valeurs ; `chemin` préfixe les messages (ex. `strategy`, `groups.alts.strategy`).
    ///
    /// Entrée : chemin de cette stratégie dans la configuration.
    /// Sortie : `Ok(())` si les seuils et périodes sont valides, sinon erreur contextualisée.
    pub fn valider(&self, chemin: &str) -> Result<(), Box<dyn Error>> {
        if self.unite_temps.trim().is_empty() {
            return Err(format!("'{chemin}.timeframe' ne peut pas être vide").into());
        }
        if self.score_min_alerte > 100 {
            return Err(
                format!("'{chemin}.min_score_alert' doit être compris entre 0 et 100").into(),
            );
        }
        if self.filtre_macro.periode_ema == 0 {
            return Err(
                format!("'{chemin}.macro_filter.ema_period' doit être supérieur à 0").into(),
            );
        }
        if self.filtre_macro.periode_supertrend == 0 {
            return Err(format!(
                "'{chemin}.macro_filter.supertrend_period' doit être supérieur à 0"
            )
            .into());
        }
        if self.filtre_macro.multiplicateur_supertrend <= 0.0 {
            return Err(format!(
                "'{chemin}.macro_filter.supertrend_multiplier' doit être strictement positif"
            )
            .into());
        }
        if self.adx.periode == 0 {
            return Err(format!("'{chemin}.adx.period' doit être supérieur à 0").into());
        }
        if self.adx.seuil_minimal < 0.0 {
            return Err(format!("'{chemin}.adx.min_threshold' ne peut pas être négatif").into());
        }
        if self.volume.periode_moyenne_mobile == 0 {
            return Err(format!("'{chemin}.volume.sma_period' doit être supérieur à 0").into());
        }
        if self.volume.multiplicateur <= 0.0 {
            return Err(
                format!("'{chemin}.volume.multiplier' doit être strictement positif").into(),
            );
        }
        Ok(())
    }
}

/// Bloc de paires partageant un même paramétrage (section `[groups.<nom>]` de `config.toml`).
///
/// Conservé pour compatibilité : au chargement, le réglage du bloc est recopié dans chacune
/// de ses paires (voir [`ConfigurationApplication::parametrages`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationGroupe {
    #[serde(rename = "symbols")]
    pub symboles: Vec<String>,
    /// Paramétrage du bloc ; à défaut, celui du générique.
    #[serde(rename = "strategy", default, skip_serializing_if = "Option::is_none")]
    pub strategie: Option<ConfigurationStrategie>,
}

/// Portée d'un réglage : le réglage par défaut ou celui d'une paire.
#[derive(Debug, Clone, PartialEq)]
pub enum Portee {
    Generique,
    Paire(String),
}

/// Réglages des indicateurs : un réglage par défaut et, éventuellement, un réglage propre
/// à chaque paire. Une paire sans réglage propre utilise le réglage par défaut.
///
/// Sur disque (`parametres.toml`) et sur le réseau, chaque réglage est une table plate :
/// copier les réglages d'une paire revient à dupliquer sa section et à changer son nom.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "FichierParametrages", into = "FichierParametrages")]
pub struct Parametrages {
    pub generique: ConfigurationStrategie,
    pub paires: BTreeMap<String, ConfigurationStrategie>,
}

/// Format de `parametres.toml` : `[defaut]` puis une section `[paires.<SYMBOLE>]` par paire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FichierParametrages {
    defaut: ReglageIndicateurs,
    #[serde(default)]
    paires: BTreeMap<String, ReglageIndicateurs>,
}

/// Réglage d'indicateurs sous forme plate (une seule table TOML, sans sous-table).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReglageIndicateurs {
    timeframe: String,
    min_score_alert: u32,
    ema_period: u32,
    use_supertrend: bool,
    #[serde(default = "periode_supertrend_defaut")]
    supertrend_period: u32,
    #[serde(default = "multiplicateur_supertrend_defaut")]
    supertrend_multiplier: f64,
    adx_period: u32,
    adx_min_threshold: f64,
    volume_sma_period: u32,
    volume_multiplier: f64,
}

impl From<ConfigurationStrategie> for ReglageIndicateurs {
    fn from(s: ConfigurationStrategie) -> Self {
        Self {
            timeframe: s.unite_temps,
            min_score_alert: s.score_min_alerte,
            ema_period: s.filtre_macro.periode_ema,
            use_supertrend: s.filtre_macro.utiliser_supertrend,
            supertrend_period: s.filtre_macro.periode_supertrend,
            supertrend_multiplier: s.filtre_macro.multiplicateur_supertrend,
            adx_period: s.adx.periode,
            adx_min_threshold: s.adx.seuil_minimal,
            volume_sma_period: s.volume.periode_moyenne_mobile,
            volume_multiplier: s.volume.multiplicateur,
        }
    }
}

impl From<ReglageIndicateurs> for ConfigurationStrategie {
    fn from(r: ReglageIndicateurs) -> Self {
        Self {
            score_min_alerte: r.min_score_alert,
            filtre_macro: ConfigurationMacro {
                periode_ema: r.ema_period,
                utiliser_supertrend: r.use_supertrend,
                periode_supertrend: r.supertrend_period,
                multiplicateur_supertrend: r.supertrend_multiplier,
            },
            adx: ConfigurationAdx {
                periode: r.adx_period,
                seuil_minimal: r.adx_min_threshold,
            },
            volume: ConfigurationVolume {
                periode_moyenne_mobile: r.volume_sma_period,
                multiplicateur: r.volume_multiplier,
            },
            unite_temps: r.timeframe,
        }
    }
}

impl From<Parametrages> for FichierParametrages {
    fn from(p: Parametrages) -> Self {
        Self {
            defaut: p.generique.into(),
            paires: p.paires.into_iter().map(|(k, v)| (k, v.into())).collect(),
        }
    }
}

impl TryFrom<FichierParametrages> for Parametrages {
    type Error = String;

    fn try_from(f: FichierParametrages) -> Result<Self, String> {
        let parametrages = Self {
            generique: f.defaut.into(),
            paires: f
                .paires
                .into_iter()
                .map(|(k, v)| (k.to_uppercase(), v.into()))
                .collect(),
        };
        parametrages.valider().map_err(|e| e.to_string())?;
        Ok(parametrages)
    }
}

/// Ancien format de `parametres.toml` (`[strategy]`, `[groups]`, `[overrides]`), relu pour migration.
#[derive(Deserialize)]
struct AncienFichierParametrages {
    strategy: ConfigurationStrategie,
    #[serde(default)]
    groups: BTreeMap<String, ConfigurationGroupe>,
    #[serde(default)]
    overrides: BTreeMap<String, ConfigurationStrategie>,
}

impl Parametrages {
    /// Construit des paramétrages à partir d'un réglage par défaut, de blocs et de réglages de paires.
    ///
    /// Entrées : réglage par défaut, blocs de paires et réglages propres (prioritaires sur les blocs).
    /// Sortie : paramétrages où chaque paire d'un bloc reçoit le réglage du bloc.
    fn depuis_niveaux(
        generique: ConfigurationStrategie,
        groupes: &BTreeMap<String, ConfigurationGroupe>,
        surcharges: &BTreeMap<String, ConfigurationStrategie>,
    ) -> Self {
        let mut paires = BTreeMap::new();
        for groupe in groupes.values() {
            if let Some(s) = &groupe.strategie {
                for symbole in &groupe.symboles {
                    paires
                        .entry(symbole.to_uppercase())
                        .or_insert_with(|| s.clone());
                }
            }
        }
        for (symbole, s) in surcharges {
            paires.insert(symbole.to_uppercase(), s.clone());
        }
        Self { generique, paires }
    }

    /// Regroupe les symboles par période effective, pour ouvrir un flux par période.
    ///
    /// Entrée : symboles à regrouper.
    /// Sortie : table associant chaque unité de temps à ses symboles.
    pub fn par_unite(&self, symboles: &[String]) -> BTreeMap<String, Vec<String>> {
        let mut groupes: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for s in symboles {
            groupes
                .entry(self.resoudre(s).unite_temps.clone())
                .or_default()
                .push(s.clone());
        }
        groupes
    }

    /// Résout le réglage effectif d'une paire : son réglage propre, sinon le réglage par défaut.
    ///
    /// Entrée : symbole de la paire.
    /// Sortie : référence à la stratégie applicable.
    pub fn resoudre(&self, symbole: &str) -> &ConfigurationStrategie {
        self.paires
            .get(&symbole.to_uppercase())
            .unwrap_or(&self.generique)
    }

    /// Lit le réglage effectif d'une portée.
    ///
    /// Entrée : portée à consulter.
    /// Sortie : copie de la stratégie effective pour cette portée.
    pub fn lire(&self, portee: &Portee) -> ConfigurationStrategie {
        match portee {
            Portee::Generique => self.generique.clone(),
            Portee::Paire(symbole) => self.resoudre(symbole).clone(),
        }
    }

    /// Enregistre un réglage pour le défaut ou pour une paire.
    ///
    /// Entrées : portée ciblée et réglage à appliquer.
    /// Sortie : met à jour les paramétrages ; une paire reçoit ainsi un réglage propre.
    pub fn ecrire(&mut self, portee: &Portee, strategie: ConfigurationStrategie) {
        match portee {
            Portee::Generique => self.generique = strategie,
            Portee::Paire(symbole) => {
                self.paires.insert(symbole.to_uppercase(), strategie);
            }
        }
    }

    /// Copie le réglage effectif d'une portée vers une liste de paires.
    ///
    /// Entrées : portée source et symboles cibles (la source elle-même est ignorée).
    /// Sortie : chaque cible reçoit un réglage propre identique à la source.
    pub fn copier(&mut self, source: &Portee, cibles: &[String]) {
        let reglage = self.lire(source);
        for cible in cibles {
            let cible = cible.to_uppercase();
            if *source != Portee::Paire(cible.clone()) {
                self.paires.insert(cible, reglage.clone());
            }
        }
    }

    /// Supprime le réglage propre d'une paire, qui revient au réglage par défaut.
    ///
    /// Entrée : portée à réinitialiser (sans effet sur le défaut).
    /// Sortie : retire le réglage propre de la paire s'il existe.
    pub fn reinitialiser(&mut self, portee: &Portee) {
        if let Portee::Paire(symbole) = portee {
            self.paires.remove(&symbole.to_uppercase());
        }
    }

    /// Indique si une portée possède un réglage explicite.
    ///
    /// Entrée : portée à vérifier.
    /// Sortie : `true` si le réglage est défini directement (le défaut l'est toujours).
    pub fn a_un_reglage_propre(&self, portee: &Portee) -> bool {
        match portee {
            Portee::Generique => true,
            Portee::Paire(symbole) => self.paires.contains_key(&symbole.to_uppercase()),
        }
    }

    /// Valide le réglage par défaut et celui de chaque paire.
    ///
    /// Sortie : `Ok(())` si chaque stratégie est cohérente, sinon la première erreur rencontrée.
    pub fn valider(&self) -> Result<(), Box<dyn Error>> {
        self.generique.valider("defaut")?;
        for (symbole, s) in &self.paires {
            s.valider(&format!("paires.{symbole}"))?;
        }
        Ok(())
    }

    /// Lit des paramétrages au format TOML (format actuel, ou ancien format migré).
    ///
    /// Entrée : contenu TOML.
    /// Sortie : paramétrages valides, ou erreur de parsing/validation.
    pub fn depuis_toml(contenu: &str) -> Result<Self, Box<dyn Error>> {
        let valeur: toml::Table = toml::from_str(contenu)?;
        if valeur.contains_key("strategy") {
            let ancien: AncienFichierParametrages = valeur.try_into()?;
            let mut p = Self::depuis_niveaux(ancien.strategy, &ancien.groups, &ancien.overrides);
            // L'ancien format recopiait le générique dans chaque paire : ces copies sont retirées
            // pour que ces paires suivent de nouveau le réglage par défaut.
            let generique = p.generique.clone();
            p.paires.retain(|_, s| *s != generique);
            p.valider()?;
            return Ok(p);
        }
        Ok(valeur.try_into()?)
    }

    /// Convertit les paramétrages en TOML lisible, précédé d'un en-tête explicatif.
    ///
    /// Sortie : contenu prêt à être écrit dans `parametres.toml`.
    pub fn vers_toml(&self) -> Result<String, Box<dyn Error>> {
        const EN_TETE: &str = "\
# Réglages des indicateurs, enregistrés par l'IHM (prioritaires sur config.toml).
# [defaut] s'applique à toute paire qui n'a pas sa propre section [paires.<SYMBOLE>].
# Pour copier les réglages d'une paire vers une autre : dupliquer sa section et changer
# le symbole. Pour revenir au défaut : supprimer la section de la paire.

";
        Ok(format!("{EN_TETE}{}", toml::to_string_pretty(self)?))
    }

    /// Charge et valide une sauvegarde de paramétrages.
    ///
    /// Entrée : chemin du fichier.
    /// Sortie : paramétrages valides, ou erreur de lecture/parsing/validation.
    pub fn charger(chemin: &Path) -> Result<Self, Box<dyn Error>> {
        let contenu = fs::read_to_string(chemin)?;
        Self::depuis_toml(&contenu).map_err(|erreur| {
            format!(
                "Paramétrages invalides dans '{}': {erreur}",
                chemin.display()
            )
            .into()
        })
    }

    /// Valide puis écrit les paramétrages sur disque (écriture atomique via un fichier temporaire).
    ///
    /// Entrée : chemin du fichier de sauvegarde.
    /// Sortie : `Ok(())` si l'écriture réussit, sinon une erreur de validation, sérialisation ou I/O.
    pub fn sauvegarder(&self, chemin: &Path) -> Result<(), Box<dyn Error>> {
        self.valider()?;
        let temporaire = chemin.with_extension("toml.tmp");
        fs::write(&temporaire, self.vers_toml()?)?;
        fs::rename(&temporaire, chemin)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toml_exemple() -> &'static str {
        r#"
        [pairs]
        symbols = ["BTCUSDT", "ETHUSDT"]

        [timeframes]
        execution = "1h"

        [strategy]
        min_score_alert = 80

        [strategy.macro_filter]
        ema_period = 200
        use_supertrend = true
        supertrend_period = 10
        supertrend_multiplier = 3.0
        [strategy.adx]
        period = 14
        min_threshold = 22.0

        [strategy.volume]
        sma_period = 20
        multiplier = 1.2

        [network]
        rate_limit_ms = 200
        "#
    }

    #[test]
    fn analyse_et_valide_la_configuration_exemple() {
        let configuration: ConfigurationApplication =
            toml::from_str(toml_exemple()).expect("doit parser");
        configuration.valider().expect("doit être valide");

        assert_eq!(configuration.paires.symboles.len(), 2);
        assert_eq!(configuration.unites_temps.unite_execution, "1h");
        assert_eq!(configuration.strategie.score_min_alerte, 80);
        assert!(configuration.strategie.filtre_macro.utiliser_supertrend);
        assert_eq!(configuration.strategie.filtre_macro.periode_supertrend, 10);
        assert_eq!(
            configuration
                .strategie
                .filtre_macro
                .multiplicateur_supertrend,
            3.0
        );
        assert_eq!(configuration.strategie.adx.periode, 14);
        assert_eq!(configuration.strategie.volume.periode_moyenne_mobile, 20);
        assert_eq!(configuration.reseau.delai_limitation_debit_ms, 200);
        assert!(configuration.telegram.jeton_bot.is_empty());
        assert!(configuration.telegram.identifiant_chat.is_empty());
        assert_eq!(configuration.journaux.jours_retention, 7);
        assert_eq!(
            configuration.sources_donnees.fournisseurs,
            vec![
                Fournisseur::Binance,
                Fournisseur::Kraken,
                Fournisseur::Coinbase
            ]
        );
    }

    #[test]
    fn lit_et_valide_les_identifiants_telegram() {
        let toml = toml_exemple().replace(
            "[network]",
            "[telegram]\nbot_token = \"token-test\"\nchat_id = \"12345\"\n\n[network]",
        );
        let configuration = ConfigurationApplication::depuis_toml(&toml).unwrap();
        assert_eq!(configuration.telegram.jeton_bot, "token-test");
        assert_eq!(configuration.telegram.identifiant_chat, "12345");

        let incomplet = toml.replace("chat_id = \"12345\"", "chat_id = \"\"");
        assert!(ConfigurationApplication::depuis_toml(&incomplet).is_err());
    }

    #[test]
    fn lit_et_valide_la_retention_des_journaux() {
        let toml = toml_exemple().replace("[network]", "[logs]\nretention_days = 14\n\n[network]");
        let configuration = ConfigurationApplication::depuis_toml(&toml).unwrap();
        assert_eq!(configuration.journaux.jours_retention, 14);

        let invalide = toml.replace("retention_days = 14", "retention_days = 0");
        assert!(ConfigurationApplication::depuis_toml(&invalide).is_err());
    }

    #[test]
    fn valide_la_configuration_reelle_avec_les_trois_sources() {
        let configuration =
            ConfigurationApplication::depuis_toml(include_str!("../config.toml")).unwrap();
        assert_eq!(
            configuration.sources_donnees.fournisseurs,
            vec![
                Fournisseur::Binance,
                Fournisseur::Kraken,
                Fournisseur::Coinbase
            ]
        );
    }

    #[test]
    fn lit_les_fournisseurs_de_donnees_configures() {
        let toml = toml_exemple().replace(
            "[network]",
            "[data_sources]\nproviders = [\"binance\", \"kraken\", \"coinbase\"]\n\n[network]",
        );
        let configuration = ConfigurationApplication::depuis_toml(&toml).unwrap();
        assert_eq!(
            configuration.sources_donnees.fournisseurs,
            vec![
                Fournisseur::Binance,
                Fournisseur::Kraken,
                Fournisseur::Coinbase
            ]
        );
    }

    #[test]
    fn rejette_une_liste_de_symboles_vide() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        configuration.paires.symboles.clear();
        assert!(configuration.valider().is_err());
    }

    #[test]
    fn un_bloc_est_recopie_dans_ses_paires_et_la_surcharge_reste_prioritaire() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut bloc = configuration.strategie.clone();
        bloc.adx.periode = 20;
        configuration.groupes.insert(
            "majeures".into(),
            ConfigurationGroupe {
                symboles: vec!["BTCUSDT".into(), "ETHUSDT".into()],
                strategie: Some(bloc),
            },
        );
        let mut paire = configuration.strategie.clone();
        paire.adx.periode = 30;
        configuration.surcharges.insert("btcusdt".into(), paire);
        let p = configuration.parametrages();
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 30);
        assert_eq!(p.resoudre("ethusdt").adx.periode, 20);
        assert_eq!(p.resoudre("SOLUSDT").adx.periode, 14);
    }

    #[test]
    fn une_paire_sans_reglage_propre_suit_le_defaut() {
        let configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut p = configuration.parametrages();
        assert!(p.paires.is_empty());
        p.generique.adx.periode = 99;
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 99);

        let mut propre = p.generique.clone();
        propre.adx.periode = 7;
        p.ecrire(&Portee::Paire("btcusdt".into()), propre);
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 7);
        p.reinitialiser(&Portee::Paire("BTCUSDT".into()));
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 99);
    }

    #[test]
    fn copie_le_reglage_d_une_paire_vers_d_autres() {
        let configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut p = configuration.parametrages();
        let mut source = p.generique.clone();
        source.volume.multiplicateur = 2.5;
        p.ecrire(&Portee::Paire("BTCUSDT".into()), source.clone());
        p.copier(
            &Portee::Paire("BTCUSDT".into()),
            &["ethusdt".into(), "BTCUSDT".into(), "SOLUSDT".into()],
        );
        assert_eq!(p.resoudre("ETHUSDT"), &source);
        assert_eq!(p.resoudre("SOLUSDT"), &source);
        // La copie est indépendante de la source.
        p.generique.volume.multiplicateur = 9.0;
        assert_eq!(p.resoudre("ETHUSDT").volume.multiplicateur, 2.5);
    }

    #[test]
    fn sauvegarde_dans_un_format_plat_et_rechargement() {
        let configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut p = configuration.parametrages();
        p.ecrire(&Portee::Paire("ETHUSDT".into()), {
            let mut s = p.generique.clone();
            s.volume.multiplicateur = 2.0;
            s
        });
        let texte = p.vers_toml().unwrap();
        assert!(texte.contains("[defaut]"));
        assert!(texte.contains("[paires.ETHUSDT]"));
        assert!(
            !texte.contains(".adx]"),
            "aucune sous-table attendue :\n{texte}"
        );

        let chemin = std::env::temp_dir().join("predict_parametres_test.toml");
        p.sauvegarder(&chemin).unwrap();
        let relu = Parametrages::charger(&chemin).unwrap();
        let _ = std::fs::remove_file(&chemin);
        assert_eq!(relu, p);

        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(serde_json::from_str::<Parametrages>(&json).unwrap(), p);
    }

    #[test]
    fn une_section_copiee_a_la_main_est_relue() {
        let texte = r#"
            [defaut]
            timeframe = "1h"
            min_score_alert = 80
            ema_period = 200
            use_supertrend = true
            adx_period = 14
            adx_min_threshold = 22.0
            volume_sma_period = 20
            volume_multiplier = 1.2

            [paires.solEur]
            timeframe = "4h"
            min_score_alert = 70
            ema_period = 100
            use_supertrend = false
            adx_period = 10
            adx_min_threshold = 20.0
            volume_sma_period = 30
            volume_multiplier = 1.5
        "#;
        let p = Parametrages::depuis_toml(texte).unwrap();
        assert_eq!(p.resoudre("SOLEUR").unite_temps, "4h");
        assert_eq!(p.resoudre("SOLEUR").filtre_macro.periode_supertrend, 10);
        assert_eq!(p.resoudre("BTCEUR").unite_temps, "1h");

        let faute = texte.replace("adx_period = 10", "adx_periode = 10");
        assert!(Parametrages::depuis_toml(&faute).is_err());
        let invalide = texte.replace("adx_period = 10", "adx_period = 0");
        assert!(Parametrages::depuis_toml(&invalide).is_err());
    }

    #[test]
    fn valide_le_fichier_parametres_reel() {
        Parametrages::depuis_toml(include_str!("../parametres.toml")).unwrap();
    }

    #[test]
    fn migre_l_ancien_format_de_sauvegarde() {
        let ancien = r#"
            [strategy]
            min_score_alert = 80
            timeframe = "1h"
            [strategy.macro_filter]
            ema_period = 200
            use_supertrend = true
            [strategy.adx]
            period = 14
            min_threshold = 22.0
            [strategy.volume]
            sma_period = 20
            multiplier = 1.2

            [groups.alts]
            symbols = ["SOLEUR"]
            [groups.alts.strategy]
            min_score_alert = 70
            timeframe = "4h"
            [groups.alts.strategy.macro_filter]
            ema_period = 100
            use_supertrend = false
            [groups.alts.strategy.adx]
            period = 14
            min_threshold = 20.0
            [groups.alts.strategy.volume]
            sma_period = 20
            multiplier = 1.2

            [overrides.BTCEUR]
            min_score_alert = 80
            timeframe = "1h"
            [overrides.BTCEUR.macro_filter]
            ema_period = 200
            use_supertrend = true
            [overrides.BTCEUR.adx]
            period = 14
            min_threshold = 22.0
            [overrides.BTCEUR.volume]
            sma_period = 20
            multiplier = 1.2
        "#;
        let p = Parametrages::depuis_toml(ancien).unwrap();
        // BTCEUR était une simple copie du générique : elle suit de nouveau le défaut.
        assert_eq!(p.paires.keys().collect::<Vec<_>>(), vec!["SOLEUR"]);
        assert_eq!(p.resoudre("SOLEUR").unite_temps, "4h");
    }

    #[test]
    fn rejette_un_score_minimal_alerte_invalide() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        configuration.strategie.score_min_alerte = 150;
        assert!(configuration.valider().is_err());
    }

    #[test]
    fn rejette_les_fournisseurs_vides_ou_dupliques() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        configuration.sources_donnees.fournisseurs.clear();
        assert!(configuration.valider().is_err());

        configuration.sources_donnees.fournisseurs =
            vec![Fournisseur::Binance, Fournisseur::Binance];
        assert!(configuration.valider().is_err());
    }
}
