use crate::core::modeles::Bougie;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Fournisseur {
    Binance,
    Kraken,
    Coinbase,
}

impl Fournisseur {
    /// Retourne le nom lisible du fournisseur.
    ///
    /// Entrée : fournisseur courant.
    /// Sortie : libellé statique (`Binance`, `Kraken` ou `Coinbase`).
    pub fn libelle(self) -> &'static str {
        match self {
            Self::Binance => "Binance",
            Self::Kraken => "Kraken",
            Self::Coinbase => "Coinbase",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BougieSourcee {
    pub fournisseur: Fournisseur,
    pub bougie: Bougie,
}

#[derive(Debug, Clone)]
pub struct Agregateur {
    fournisseurs_attendus: HashSet<Fournisseur>,
    delai_attente: Duration,
    bougies: BTreeMap<(String, String, i64), GroupeBougies>,
}

#[derive(Debug, Clone)]
struct GroupeBougies {
    par_fournisseur: HashMap<Fournisseur, Bougie>,
    premiere_reception: Instant,
    emis: bool,
    finalise: bool,
}

impl Agregateur {
    /// Crée un agrégateur pour les fournisseurs attendus et le délai de regroupement.
    ///
    /// Entrées : liste des fournisseurs à agréger et délai maximal d'attente.
    /// Sortie : agrégateur vide prêt à recevoir des bougies.
    pub fn nouveau(fournisseurs: &[Fournisseur], delai_attente: Duration) -> Self {
        Self {
            fournisseurs_attendus: fournisseurs.iter().copied().collect(),
            delai_attente,
            bougies: BTreeMap::new(),
        }
    }

    /// Ajoute une mise à jour source et agrège les bougies correspondantes si possible.
    ///
    /// Entrée : bougie identifiée par son fournisseur.
    /// Sortie : bougie agrégée lorsqu'assez de sources sont disponibles, sinon `None`.
    pub fn recevoir(&mut self, bougie_sourcee: BougieSourcee) -> Option<Bougie> {
        if !self
            .fournisseurs_attendus
            .contains(&bougie_sourcee.fournisseur)
        {
            return None;
        }
        let bougie = bougie_sourcee.bougie;
        let cle = (
            bougie.symbole.to_uppercase(),
            bougie.unite_temps.clone(),
            bougie.instant_ouverture.timestamp_millis(),
        );
        let courante = self
            .bougies
            .entry(cle.clone())
            .or_insert_with(|| GroupeBougies {
                par_fournisseur: HashMap::new(),
                premiere_reception: Instant::now(),
                emis: false,
                finalise: false,
            });
        if courante.finalise {
            return None;
        }
        let precedente = courante.par_fournisseur.get(&bougie_sourcee.fournisseur);
        if precedente.is_some_and(|ancienne| ancienne.cloturee && !bougie.cloturee) {
            return None;
        }
        courante
            .par_fournisseur
            .insert(bougie_sourcee.fournisseur, bougie);

        let sources_minimales = self.fournisseurs_attendus.len().min(2);
        let toutes_recues = self
            .fournisseurs_attendus
            .iter()
            .all(|fournisseur| courante.par_fournisseur.contains_key(fournisseur));
        if courante.par_fournisseur.len() < sources_minimales
            || (!courante.emis
                && !toutes_recues
                && courante.premiere_reception.elapsed() < self.delai_attente)
        {
            self.nettoyer(&cle);
            return None;
        }

        let resultat = agreger(&courante.par_fournisseur);
        if let Some(bougie) = &resultat {
            courante.emis = true;
            courante.finalise = bougie.cloturee;
        }
        self.nettoyer(&cle);
        resultat
    }

    /// Émet les groupes de bougies dont le délai d'attente est écoulé.
    ///
    /// Sortie : bougies agrégées nouvellement disponibles.
    pub fn emettre_expirees(&mut self) -> Vec<Bougie> {
        let sources_minimales = self.fournisseurs_attendus.len().min(2);
        let mut resultats = Vec::new();
        for groupe in self.bougies.values_mut() {
            if groupe.emis
                || groupe.par_fournisseur.len() < sources_minimales
                || groupe.premiere_reception.elapsed() < self.delai_attente
            {
                continue;
            }
            if let Some(bougie) = agreger(&groupe.par_fournisseur) {
                groupe.emis = true;
                groupe.finalise = bougie.cloturee;
                resultats.push(bougie);
            }
        }
        resultats
    }

    /// Supprime les groupes anciens du même symbole et de la même unité de temps.
    ///
    /// Entrée : clé de la bougie courante, utilisée pour calculer la fenêtre conservée.
    fn nettoyer(&mut self, cle_courante: &(String, String, i64)) {
        let Some(duree) = duree_unite(&cle_courante.1) else {
            return;
        };
        let seuil = cle_courante.2 - duree.num_milliseconds() * 3;
        self.bougies.retain(|(symbole, unite, ouverture), _| {
            symbole != &cle_courante.0 || unite != &cle_courante.1 || *ouverture >= seuil
        });
    }
}

/// Agrège les bougies historiques clôturées par symbole, unité de temps et ouverture.
///
/// Entrées : bougies étiquetées par fournisseur et liste des fournisseurs attendus.
/// Sortie : bougies agrégées disposant du nombre minimal de sources.
pub fn agreger_historique(
    bougies: Vec<BougieSourcee>,
    fournisseurs: &[Fournisseur],
) -> Vec<Bougie> {
    let attendus: HashSet<Fournisseur> = fournisseurs.iter().copied().collect();
    let sources_minimales = attendus.len().min(2);
    let mut groupes: BTreeMap<(String, String, i64), HashMap<Fournisseur, Bougie>> =
        BTreeMap::new();

    for sourcee in bougies.into_iter().filter(|b| b.bougie.cloturee) {
        let bougie = sourcee.bougie;
        let cle = (
            bougie.symbole.to_uppercase(),
            bougie.unite_temps.clone(),
            bougie.instant_ouverture.timestamp_millis(),
        );
        groupes
            .entry(cle)
            .or_default()
            .insert(sourcee.fournisseur, bougie);
    }

    groupes
        .into_values()
        .filter_map(|groupe| {
            (groupe.len() >= sources_minimales)
                .then(|| agreger(&groupe))
                .flatten()
        })
        .collect()
}

/// Calcule une bougie agrégée à partir des bougies fournies.
///
/// Entrée : bougies indexées par fournisseur pour une même période.
/// Sortie : moyenne OHLC et volume cumulé, ou `None` si l'entrée est vide.
fn agreger(bougies: &HashMap<Fournisseur, Bougie>) -> Option<Bougie> {
    if bougies.is_empty() {
        return None;
    }
    let sources: Vec<&Bougie> = bougies.values().collect();
    let nombre = sources.len() as f64;
    let premiere = sources.first()?;

    Some(Bougie {
        symbole: premiere.symbole.to_uppercase(),
        unite_temps: premiere.unite_temps.clone(),
        instant_ouverture: premiere.instant_ouverture,
        ouverture: sources.iter().map(|b| b.ouverture).sum::<f64>() / nombre,
        haut: sources.iter().map(|b| b.haut).sum::<f64>() / nombre,
        bas: sources.iter().map(|b| b.bas).sum::<f64>() / nombre,
        cloture: sources.iter().map(|b| b.cloture).sum::<f64>() / nombre,
        volume: sources.iter().map(|b| b.volume).sum(),
        cloturee: sources.iter().all(|b| b.cloturee),
    })
}

/// Convertit une unité de temps positive (`1m`, `4h`, `1d`, etc.) en durée.
///
/// Entrée : unité composée d'un entier et d'un suffixe reconnu.
/// Sortie : durée correspondante, ou `None` si le format ou le suffixe est invalide.
pub fn duree_unite(unite: &str) -> Option<chrono::Duration> {
    let (nombre, suffixe) = unite.split_at(unite.len().checked_sub(1)?);
    let nombre: i64 = nombre.parse().ok()?;
    if nombre <= 0 {
        return None;
    }
    match suffixe {
        "m" => Some(chrono::Duration::minutes(nombre)),
        "h" => Some(chrono::Duration::hours(nombre)),
        "d" => Some(chrono::Duration::days(nombre)),
        "w" => Some(chrono::Duration::weeks(nombre)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono::Utc;

    fn bougie(fournisseur: Fournisseur, ouverture: i64, base: f64) -> BougieSourcee {
        BougieSourcee {
            fournisseur,
            bougie: Bougie {
                symbole: "BTCEUR".into(),
                unite_temps: "1h".into(),
                instant_ouverture: Utc.timestamp_millis_opt(ouverture).unwrap(),
                ouverture: base,
                haut: base + 10.0,
                bas: base - 10.0,
                cloture: base + 2.0,
                volume: base,
                cloturee: true,
            },
        }
    }

    #[test]
    fn agrege_ohlc_par_moyenne_et_volume_par_somme() {
        let fournisseurs = [Fournisseur::Binance, Fournisseur::Kraken];
        let mut agregateur = Agregateur::nouveau(&fournisseurs, Duration::from_secs(5));

        assert!(
            agregateur
                .recevoir(bougie(Fournisseur::Binance, 1_700_000_000_000, 100.0))
                .is_none()
        );
        let resultat = agregateur
            .recevoir(bougie(Fournisseur::Kraken, 1_700_000_000_000, 120.0))
            .unwrap();

        assert_eq!(resultat.ouverture, 110.0);
        assert_eq!(resultat.haut, 120.0);
        assert_eq!(resultat.bas, 100.0);
        assert_eq!(resultat.cloture, 112.0);
        assert_eq!(resultat.volume, 220.0);
        assert!(resultat.cloturee);
    }

    #[test]
    fn n_agrege_pas_les_horodatages_differents_ni_les_bougies_ouvertes() {
        let fournisseurs = [Fournisseur::Binance, Fournisseur::Kraken];
        let resultat = agreger_historique(
            vec![
                bougie(Fournisseur::Binance, 1_700_000_000_000, 100.0),
                bougie(Fournisseur::Kraken, 1_700_000_000_001, 120.0),
            ],
            &fournisseurs,
        );
        assert!(resultat.is_empty());
    }

    #[test]
    fn agrege_deux_sources_disponibles_apres_delai_si_la_troisieme_manque() {
        let fournisseurs = [
            Fournisseur::Binance,
            Fournisseur::Kraken,
            Fournisseur::Coinbase,
        ];
        let mut agregateur = Agregateur::nouveau(&fournisseurs, Duration::from_millis(30));
        let ouverture = 1_700_000_000_000;

        assert!(
            agregateur
                .recevoir(bougie(Fournisseur::Binance, ouverture, 100.0))
                .is_none()
        );
        assert!(
            agregateur
                .recevoir(bougie(Fournisseur::Kraken, ouverture, 120.0))
                .is_none()
        );
        std::thread::sleep(Duration::from_millis(40));
        let resultat = agregateur.emettre_expirees().pop().unwrap();
        assert_eq!(resultat.ouverture, 110.0);
        assert_eq!(resultat.volume, 220.0);
    }
}
