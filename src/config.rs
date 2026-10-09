//! Parsing et validation de la configuration de l'application à partir de `config.toml`.

use serde::{Deserialize, Serialize};
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationStrategie {
    #[serde(rename = "min_score_alert")]
    pub score_min_alerte: u32,
    #[serde(rename = "macro_filter")]
    pub filtre_macro: ConfigurationMacro,
    pub adx: ConfigurationAdx,
    pub volume: ConfigurationVolume,
}

/// Filtre macro basé sur l'EMA / Supertrend (section `[strategy.macro_filter]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn periode_supertrend_defaut() -> u32 {
    10
}

fn multiplicateur_supertrend_defaut() -> f64 {
    3.0
}

/// Configuration de l'indicateur ADX (section `[strategy.adx]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationAdx {
    #[serde(rename = "period")]
    pub periode: u32,
    #[serde(rename = "min_threshold")]
    pub seuil_minimal: f64,
}

/// Configuration du filtre de volume (section `[strategy.volume]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub fn depuis_toml(contenu: &str) -> Result<Self, Box<dyn Error>> {
        let configuration: ConfigurationApplication = toml::from_str(contenu)?;
        configuration.valider()?;
        Ok(configuration)
    }

    /// Valide la cohérence des valeurs de configuration.
    pub fn valider(&self) -> Result<(), Box<dyn Error>> {
        if self.paires.symboles.is_empty() {
            return Err("La liste 'pairs.symbols' ne peut pas être vide".into());
        }

        if self.unites_temps.unite_execution.trim().is_empty() {
            return Err("'timeframes.execution' ne peut pas être vide".into());
        }

        if self.strategie.score_min_alerte > 100 {
            return Err("'strategy.min_score_alert' doit être compris entre 0 et 100".into());
        }

        if self.strategie.filtre_macro.periode_ema == 0 {
            return Err("'strategy.macro_filter.ema_period' doit être supérieur à 0".into());
        }

        if self.strategie.filtre_macro.periode_supertrend == 0 {
            return Err("'strategy.macro_filter.supertrend_period' doit être supérieur à 0".into());
        }

        if self.strategie.filtre_macro.multiplicateur_supertrend <= 0.0 {
            return Err(
                "'strategy.macro_filter.supertrend_multiplier' doit être strictement positif"
                    .into(),
            );
        }

        if self.strategie.adx.periode == 0 {
            return Err("'strategy.adx.period' doit être supérieur à 0".into());
        }

        if self.strategie.adx.seuil_minimal < 0.0 {
            return Err("'strategy.adx.min_threshold' ne peut pas être négatif".into());
        }

        if self.strategie.volume.periode_moyenne_mobile == 0 {
            return Err("'strategy.volume.sma_period' doit être supérieur à 0".into());
        }

        if self.strategie.volume.multiplicateur <= 0.0 {
            return Err("'strategy.volume.multiplier' doit être strictement positif".into());
        }

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
            configuration.strategie.filtre_macro.multiplicateur_supertrend,
            3.0
        );
        assert_eq!(configuration.strategie.adx.periode, 14);
        assert_eq!(configuration.strategie.volume.periode_moyenne_mobile, 20);
        assert_eq!(configuration.reseau.delai_limitation_debit_ms, 200);
    }

    #[test]
    fn rejette_une_liste_de_symboles_vide() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        configuration.paires.symboles.clear();
        assert!(configuration.valider().is_err());
    }

    #[test]
    fn rejette_un_score_minimal_alerte_invalide() {
        let mut configuration: ConfigurationApplication = toml::from_str(toml_exemple()).unwrap();
        configuration.strategie.score_min_alerte = 150;
        assert!(configuration.valider().is_err());
    }
}
