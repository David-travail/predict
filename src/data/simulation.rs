//! Flux temps réel simulé (marche aléatoire), utile quand Binance est inaccessible.

use crate::core::modeles::Bougie;
use chrono::Utc;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tracing::{info, warn};

const INTERVALLE: Duration = Duration::from_secs(2);
/// Amplitude maximale d'une variation de clôture par bougie (±0,5 %).
const VOLATILITE: f64 = 0.005;

/// Générateur pseudo-aléatoire xorshift64* (évite une dépendance externe).
struct Alea(u64);

impl Alea {
    /// Initialise le générateur pseudo-aléatoire avec une graine non nulle.
    ///
    /// Entrée : graine entière.
    /// Sortie : générateur déterministe initialisé.
    fn nouveau(graine: u64) -> Self {
        Self(graine | 1)
    }

    /// Produit le prochain nombre pseudo-aléatoire uniforme dans `[0, 1)`.
    ///
    /// Entrée : état courant du générateur.
    /// Sortie : valeur pseudo-aléatoire et état du générateur avancé.
    fn suivant(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        let valeur = self.0.wrapping_mul(0x2545_F491_4F6C_DD1D);
        (valeur >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Produit le prochain nombre pseudo-aléatoire uniforme dans `[-1, 1)`.
    ///
    /// Entrée : état courant du générateur.
    /// Sortie : valeur centrée dérivée de [`Alea::suivant`].
    fn centre(&mut self) -> f64 {
        self.suivant() * 2.0 - 1.0
    }
}

/// Hash FNV-1a, pour dériver un prix de départ stable par symbole.
///
/// Entrée : texte à hacher.
/// Sortie : empreinte entière déterministe.
fn hacher(texte: &str) -> u64 {
    texte.bytes().fold(0xcbf2_9ce4_8422_2325, |h, o| {
        (h ^ u64::from(o)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

/// Calcule un prix de départ stable à partir du symbole.
///
/// Entrée : symbole de la paire.
/// Sortie : prix positif déterministe utilisé par le flux simulé.
fn prix_depart(symbole: &str) -> f64 {
    50.0 + (hacher(symbole) % 50_000) as f64
}

/// Durée d'une unité de temps Binance (`1m`, `1h`, `1d`...) ; 1 heure par défaut.
///
/// Entrée : unité de temps à convertir.
/// Sortie : durée correspondante; une unité mal formée utilise les valeurs de repli du simulateur.
fn duree_unite(unite_temps: &str) -> chrono::Duration {
    let (nombre, suffixe) = unite_temps.split_at(unite_temps.len().saturating_sub(1));
    let n: i64 = nombre.parse().unwrap_or(1).max(1);
    match suffixe {
        "s" => chrono::Duration::seconds(n),
        "m" => chrono::Duration::minutes(n),
        "d" => chrono::Duration::days(n),
        "w" => chrono::Duration::weeks(n),
        _ => chrono::Duration::hours(n),
    }
}

/// Génère `nombre` bougies fermées fictives (marche aléatoire) se terminant au prix de départ
/// utilisé par [`simuler_flux_temps_reel`], de la plus ancienne à la plus récente.
///
/// Entrées : symbole, unité de temps et nombre de bougies.
/// Sortie : historique synthétique ordonné chronologiquement.
pub fn generer_historique_fictif(symbole: &str, unite_temps: &str, nombre: usize) -> Vec<Bougie> {
    let mut alea =
        Alea::nouveau(hacher(symbole) ^ Utc::now().timestamp_nanos_opt().unwrap_or(1) as u64);
    let pas = duree_unite(unite_temps);
    let fin = Utc::now();

    let mut cours = 1.0_f64;
    let mut bougies: Vec<Bougie> = (0..nombre)
        .map(|i| {
            let ouverture = cours;
            let cloture = (ouverture * (1.0 + VOLATILITE * alea.centre())).max(0.0001);
            cours = cloture;
            Bougie {
                symbole: symbole.to_uppercase(),
                unite_temps: unite_temps.to_string(),
                instant_ouverture: fin - pas * (nombre - i) as i32,
                ouverture,
                haut: ouverture.max(cloture) * (1.0 + VOLATILITE * 0.5 * alea.suivant()),
                bas: ouverture.min(cloture) * (1.0 - VOLATILITE * 0.5 * alea.suivant()),
                cloture,
                volume: 10.0 + 990.0 * alea.suivant(),
                cloturee: true,
            }
        })
        .collect();

    // Mise à l'échelle pour que la dernière clôture coïncide avec le prix de départ du flux simulé.
    if let Some(derniere) = bougies.last() {
        let facteur = prix_depart(symbole) / derniere.cloture;
        for b in &mut bougies {
            b.ouverture *= facteur;
            b.haut *= facteur;
            b.bas *= facteur;
            b.cloture *= facteur;
        }
    }
    bougies
}

/// Émet toutes les 2 secondes une bougie fermée simulée par symbole, sans fin.
///
/// Entrées : symboles, unité de temps et canal recevant les bougies.
/// Sortie : tourne jusqu'à ce que la liste soit vide ou que le récepteur du canal soit fermé.
pub async fn simuler_flux_temps_reel(symboles: &[String], unite_temps: &str, tx: Sender<Bougie>) {
    if symboles.is_empty() {
        warn!("Aucun symbole à simuler : flux simulé non démarré");
        return;
    }

    info!("Démarrage du flux simulé");
    let horloge = Utc::now().timestamp_nanos_opt().unwrap_or(1) as u64;
    let mut alea = Alea::nouveau(horloge);
    let mut prix: Vec<f64> = symboles.iter().map(|s| prix_depart(s)).collect();

    loop {
        for (symbole, dernier) in symboles.iter().zip(prix.iter_mut()) {
            let ouverture = *dernier;
            let cloture = (ouverture * (1.0 + VOLATILITE * alea.centre())).max(0.01);
            let haut = ouverture.max(cloture) * (1.0 + VOLATILITE * 0.5 * alea.suivant());
            let bas = ouverture.min(cloture) * (1.0 - VOLATILITE * 0.5 * alea.suivant());
            let volume = 10.0 + 990.0 * alea.suivant();
            *dernier = cloture;

            let bougie = Bougie {
                symbole: symbole.to_uppercase(),
                unite_temps: unite_temps.to_string(),
                instant_ouverture: Utc::now(),
                ouverture,
                haut,
                bas,
                cloture,
                volume,
                cloturee: true,
            };

            if tx.send(bougie).await.is_err() {
                info!("Récepteur fermé : arrêt du flux simulé");
                return;
            }
        }

        sleep(INTERVALLE).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn emet_des_bougies_valides() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let symboles = vec!["btceur".to_string(), "ethusdt".to_string()];
        tokio::spawn(async move { simuler_flux_temps_reel(&symboles, "1h", tx).await });

        for _ in 0..6 {
            let b = rx.recv().await.unwrap();
            assert!(b.cloturee && b.volume > 0.0 && b.bas > 0.0);
            assert!(b.haut >= b.ouverture.max(b.cloture));
            assert!(b.bas <= b.ouverture.min(b.cloture));
        }
    }
}
