use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bougie {
    pub symbole: String,
    pub unite_temps: String,
    pub instant_ouverture: DateTime<Utc>,
    pub ouverture: f64,
    pub haut: f64,
    pub bas: f64,
    pub cloture: f64,
    pub volume: f64,
    pub cloturee: bool,
}

/// Sens de la tendance de fond.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tendance {
    Haussiere,
    Baissiere,
    Neutre,
}

impl Tendance {
    /// Retourne le libellé affiché pour cette tendance.
    ///
    /// Entrée : tendance courante.
    /// Sortie : libellé statique avec son indicateur visuel.
    pub fn libelle(self) -> &'static str {
        match self {
            Tendance::Haussiere => "HAUSSE 📈",
            Tendance::Baissiere => "BAISSE 📉",
            Tendance::Neutre => "NEUTRE ➖",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalyseTechnique {
    pub tendance: Tendance,
    pub valeur_adx: f64,
    pub adx_valide: bool,
    pub moyenne_volume: f64,
    pub volume_valide: bool,
}
