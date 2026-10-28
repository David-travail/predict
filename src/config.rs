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
    /// Blocs de paires (`[groups.<nom>]`), chacun avec son paramétrage optionnel.
    #[serde(rename = "groups", default)]
    pub groupes: BTreeMap<String, ConfigurationGroupe>,
    /// Paramétrage propre à une paire (`[overrides.<SYMBOLE>]`).
    #[serde(rename = "overrides", default)]
    pub surcharges: BTreeMap<String, ConfigurationStrategie>,
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

        self.parametrages().valider()
    }

    /// Construit les paramétrages génériques, de groupes et de paires depuis la configuration.
    ///
    /// Entrée : configuration courante.
    /// Sortie : paramétrages avec symboles de surcharge normalisés en majuscules.
    pub fn parametrages(&self) -> Parametrages {
        let mut generique = self.strategie.clone();
        generique.unite_temps = self.unites_temps.unite_execution.clone();
        Parametrages {
            generique,
            groupes: self.groupes.clone(),
            paires: self
                .surcharges
                .iter()
                .map(|(k, v)| (k.to_uppercase(), v.clone()))
                .collect(),
        }
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

/// Bloc de paires partageant un même paramétrage (section `[groups.<nom>]`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigurationGroupe {
    #[serde(rename = "symbols")]
    pub symboles: Vec<String>,
    /// Paramétrage du bloc ; à défaut, celui du générique.
    #[serde(rename = "strategy", default, skip_serializing_if = "Option::is_none")]
    pub strategie: Option<ConfigurationStrategie>,
}

/// Portée d'un paramétrage, de la plus large à la plus précise.
#[derive(Debug, Clone, PartialEq)]
pub enum Portee {
    Generique,
    Groupe(String),
    Paire(String),
}

/// Ensemble des paramétrages : priorité paire > bloc > générique.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Parametrages {
    #[serde(rename = "strategy")]
    pub generique: ConfigurationStrategie,
    #[serde(rename = "groups", default)]
    pub groupes: BTreeMap<String, ConfigurationGroupe>,
    #[serde(rename = "overrides", default)]
    pub paires: BTreeMap<String, ConfigurationStrategie>,
}

impl Parametrages {
    /// Crée un réglage propre pour chaque symbole qui n'en possède pas déjà un.
    ///
    /// Entrée : symboles à compléter.
    /// Sortie : met à jour les surcharges en copiant le réglage effectif hérité.
    pub fn completer(&mut self, symboles: &[String]) {
        for symbole in symboles {
            let cle = symbole.to_uppercase();
            if !self.paires.contains_key(&cle) {
                let s = self.resoudre(&cle).clone();
                self.paires.insert(cle, s);
            }
        }
    }

    /// Copie le réglage d'une portée vers les paires qu'elle couvre (toutes pour le générique).
    ///
    /// Entrées : portée source et liste des symboles; la portée groupe utilise ses propres membres.
    /// Sortie : met à jour les réglages explicites des paires concernées.
    pub fn propager(&mut self, portee: &Portee, symboles: &[String]) {
        let source = self.lire(portee);
        let cibles: Vec<String> = match portee {
            Portee::Generique => symboles.to_vec(),
            Portee::Groupe(nom) => self
                .groupes
                .get(nom)
                .map(|g| g.symboles.clone())
                .unwrap_or_default(),
            Portee::Paire(_) => Vec::new(),
        };
        for symbole in cibles {
            self.paires.insert(symbole.to_uppercase(), source.clone());
        }
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

    /// Résout le paramétrage effectif d'une paire selon la priorité paire > groupe > générique.
    ///
    /// Entrée : symbole de la paire.
    /// Sortie : référence à la stratégie applicable.
    pub fn resoudre(&self, symbole: &str) -> &ConfigurationStrategie {
        let symbole = symbole.to_uppercase();
        if let Some(s) = self.paires.get(&symbole) {
            return s;
        }
        self.groupes
            .values()
            .filter(|g| g.symboles.iter().any(|x| x.eq_ignore_ascii_case(&symbole)))
            .find_map(|g| g.strategie.as_ref())
            .unwrap_or(&self.generique)
    }

    /// Lit le paramétrage d'une portée, hérité du niveau générique si nécessaire.
    ///
    /// Entrée : portée à consulter.
    /// Sortie : copie de la stratégie effective pour cette portée.
    pub fn lire(&self, portee: &Portee) -> ConfigurationStrategie {
        match portee {
            Portee::Generique => self.generique.clone(),
            Portee::Groupe(nom) => self
                .groupes
                .get(nom)
                .and_then(|g| g.strategie.clone())
                .unwrap_or_else(|| self.generique.clone()),
            Portee::Paire(symbole) => self.resoudre(symbole).clone(),
        }
    }

    /// Enregistre un réglage pour la portée générique, un groupe ou une paire.
    ///
    /// Entrées : portée ciblée et configuration stratégique à appliquer.
    /// Sortie : met à jour les paramétrages; une portée de groupe inconnue reste inchangée.
    pub fn ecrire(&mut self, portee: &Portee, strategie: ConfigurationStrategie) {
        match portee {
            Portee::Generique => self.generique = strategie,
            Portee::Groupe(nom) => {
                if let Some(g) = self.groupes.get_mut(nom) {
                    g.strategie = Some(strategie);
                }
            }
            Portee::Paire(symbole) => {
                self.paires.insert(symbole.to_uppercase(), strategie);
            }
        }
    }

    /// Supprime le réglage propre d'une portée (le générique ne peut pas être supprimé).
    ///
    /// Entrée : portée à réinitialiser.
    /// Sortie : retire la surcharge de paire ou de groupe si elle existe.
    pub fn reinitialiser(&mut self, portee: &Portee) {
        match portee {
            Portee::Generique => {}
            Portee::Groupe(nom) => {
                if let Some(g) = self.groupes.get_mut(nom) {
                    g.strategie = None;
                }
            }
            Portee::Paire(symbole) => {
                self.paires.remove(&symbole.to_uppercase());
            }
        }
    }

    /// Indique si une portée possède un réglage explicite.
    ///
    /// Entrée : portée à vérifier.
    /// Sortie : `true` si le réglage est défini directement (la portée générique l'est toujours).
    pub fn a_un_reglage_propre(&self, portee: &Portee) -> bool {
        match portee {
            Portee::Generique => true,
            Portee::Groupe(nom) => self.groupes.get(nom).is_some_and(|g| g.strategie.is_some()),
            Portee::Paire(symbole) => self.paires.contains_key(&symbole.to_uppercase()),
        }
    }

    /// Valide le réglage générique, tous les groupes et toutes les paires.
    ///
    /// Sortie : `Ok(())` si chaque stratégie est cohérente, sinon la première erreur rencontrée.
    pub fn valider(&self) -> Result<(), Box<dyn Error>> {
        self.generique.valider("strategy")?;
        for (nom, groupe) in &self.groupes {
            if let Some(s) = &groupe.strategie {
                s.valider(&format!("groups.{nom}.strategy"))?;
            }
        }
        for (symbole, s) in &self.paires {
            s.valider(&format!("overrides.{symbole}"))?;
        }
        Ok(())
    }

    /// Charge et valide une sauvegarde de paramétrages au format TOML.
    ///
    /// Entrée : chemin du fichier.
    /// Sortie : paramétrages valides avec les symboles normalisés, ou erreur de lecture/parsing/validation.
    pub fn charger(chemin: &Path) -> Result<Self, Box<dyn Error>> {
        let contenu = fs::read_to_string(chemin)?;
        let mut parametrages: Self = toml::from_str(&contenu).map_err(|erreur| {
            format!(
                "Erreur de parsing TOML dans '{}': {erreur}",
                chemin.display()
            )
        })?;
        parametrages.paires = parametrages
            .paires
            .into_iter()
            .map(|(k, v)| (k.to_uppercase(), v))
            .collect();
        parametrages.valider()?;
        Ok(parametrages)
    }

    /// Valide puis écrit les paramétrages au format TOML sur disque.
    ///
    /// Entrée : chemin du fichier de sauvegarde.
    /// Sortie : `Ok(())` si l'écriture réussit, sinon une erreur de validation, sérialisation ou I/O.
    pub fn sauvegarder(&self, chemin: &Path) -> Result<(), Box<dyn Error>> {
        self.valider()?;
        fs::write(chemin, toml::to_string_pretty(self)?)?;
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
    fn priorite_paire_puis_bloc_puis_generique() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut bloc = configuration.strategie.clone();
        bloc.adx.periode = 20;
        configuration.groupes.insert(
            "majeures".into(),
            ConfigurationGroupe {
                symboles: vec!["BTCUSDT".into()],
                strategie: Some(bloc),
            },
        );
        let mut paire = configuration.strategie.clone();
        paire.adx.periode = 30;
        configuration.surcharges.insert("btcusdt".into(), paire);
        let p = configuration.parametrages();
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 30);
        let mut p2 = p.clone();
        p2.reinitialiser(&Portee::Paire("BTCUSDT".into()));
        assert_eq!(p2.resoudre("BTCUSDT").adx.periode, 20);
        assert_eq!(p2.resoudre("ETHUSDT").adx.periode, 14);
    }

    #[test]
    fn chaque_paire_recoit_son_propre_reglage() {
        let configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut p = configuration.parametrages();
        p.completer(&configuration.paires.symboles);
        assert_eq!(p.paires.len(), 2);
        p.generique.adx.periode = 99;
        assert_eq!(p.resoudre("BTCUSDT").adx.periode, 14);
        p.propager(&Portee::Generique, &configuration.paires.symboles);
        assert_eq!(p.resoudre("ETHUSDT").adx.periode, 99);
    }

    #[test]
    fn sauvegarde_et_rechargement() {
        let configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        let mut p = configuration.parametrages();
        p.ecrire(&Portee::Paire("ETHUSDT".into()), {
            let mut s = p.generique.clone();
            s.volume.multiplicateur = 2.0;
            s
        });
        let chemin = std::env::temp_dir().join("predict_parametres_test.toml");
        p.sauvegarder(&chemin).unwrap();
        let relu = Parametrages::charger(&chemin).unwrap();
        let _ = std::fs::remove_file(&chemin);
        assert_eq!(relu, p);
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
