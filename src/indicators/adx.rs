//! Average Directional Index (ADX) : mesure la force d'une tendance, sans en donner le sens.

use crate::core::modeles::Bougie;

/// Calcule la dernière valeur de l'ADX (0 à 100) avec le lissage de Wilder.
///
/// Étapes : True Range et mouvements directionnels (+DM / -DM) pour chaque
/// bougie, lissage de Wilder sur `periode`, indicateurs directionnels
/// (+DI / -DI), puis DX. Les `periode` premiers DX sont moyennés pour amorcer
/// l'ADX, qui est ensuite lissé à chaque nouvelle bougie.
///
/// Retourne `None` si `periode` vaut 0 ou s'il y a moins de `2 * periode` bougies.
pub fn calculer_adx(bougies: &[Bougie], periode: usize) -> Option<f64> {
    let bougies_requises = periode.checked_mul(2)?;
    if periode == 0 || bougies.len() < bougies_requises {
        return None;
    }

    // Valeurs lissées de Wilder (TR, +DM, -DM), initialisées par somme simple.
    let mut tr_lisse = 0.0;
    let mut dm_positif_lisse = 0.0;
    let mut dm_negatif_lisse = 0.0;
    let mut valeurs_dx = Vec::with_capacity(periode);
    let mut adx = None;

    for indice in 1..bougies.len() {
        let courante = &bougies[indice];
        let precedente = &bougies[indice - 1];

        // True Range : plus grand écart entre haut/bas et la clôture précédente.
        let plage_vraie = (courante.haut - courante.bas)
            .max((courante.haut - precedente.cloture).abs())
            .max((courante.bas - precedente.cloture).abs());

        // Seul le mouvement directionnel dominant est retenu (+DM ou -DM).
        let mouvement_haussier = courante.haut - precedente.haut;
        let mouvement_baissier = precedente.bas - courante.bas;
        let dm_positif = if mouvement_haussier > mouvement_baissier && mouvement_haussier > 0.0 {
            mouvement_haussier
        } else {
            0.0
        };
        let dm_negatif = if mouvement_baissier > mouvement_haussier && mouvement_baissier > 0.0 {
            mouvement_baissier
        } else {
            0.0
        };

        if indice <= periode {
            // Phase d'amorçage : somme des `periode` premières valeurs.
            tr_lisse += plage_vraie;
            dm_positif_lisse += dm_positif;
            dm_negatif_lisse += dm_negatif;
        } else {
            tr_lisse = tr_lisse - tr_lisse / periode as f64 + plage_vraie;
            dm_positif_lisse = dm_positif_lisse - dm_positif_lisse / periode as f64 + dm_positif;
            dm_negatif_lisse = dm_negatif_lisse - dm_negatif_lisse / periode as f64 + dm_negatif;
        }

        if indice >= periode {
            // +DI / -DI en pourcentage du True Range lissé, puis DX.
            let di_positif = if tr_lisse == 0.0 {
                0.0
            } else {
                100.0 * dm_positif_lisse / tr_lisse
            };
            let di_negatif = if tr_lisse == 0.0 {
                0.0
            } else {
                100.0 * dm_negatif_lisse / tr_lisse
            };
            let somme_di = di_positif + di_negatif;
            let dx = if somme_di == 0.0 {
                0.0
            } else {
                100.0 * (di_positif - di_negatif).abs() / somme_di
            };

            if let Some(adx_precedent) = adx {
                adx = Some((adx_precedent * (periode - 1) as f64 + dx) / periode as f64);
            } else {
                valeurs_dx.push(dx);
                if valeurs_dx.len() == periode {
                    adx = Some(valeurs_dx.iter().sum::<f64>() / periode as f64);
                }
            }
        }
    }

    adx
}

#[cfg(test)]
mod tests {
    use super::calculer_adx;
    use crate::core::modeles::Bougie;
    use chrono::{TimeZone, Utc};

    fn creer_bougies_haussieres(nombre: usize) -> Vec<Bougie> {
        (0..nombre)
            .map(|indice| {
                let prix = indice as f64 * 2.0;
                Bougie {
                    symbole: "TEST".to_string(),
                    unite_temps: "1m".to_string(),
                    instant_ouverture: Utc.timestamp_opt(indice as i64, 0).single().unwrap(),
                    ouverture: prix + 8.5,
                    haut: prix + 10.0,
                    bas: prix + 8.0,
                    cloture: prix + 9.0,
                    volume: 1.0,
                    cloturee: true,
                }
            })
            .collect()
    }

    #[test]
    fn calcule_adx_wilder_pour_une_tendance_haussiere_reguliere() {
        let bougies = creer_bougies_haussieres(4);

        assert_eq!(calculer_adx(&bougies, 2), Some(100.0));
        assert_eq!(calculer_adx(&bougies[..3], 2), None);
        assert_eq!(calculer_adx(&bougies, 0), None);
    }
}
