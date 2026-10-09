//! Indicateurs de volume : moyenne mobile simple et détection de cassure.

/// Calcule la moyenne mobile simple (SMA) des `periode` dernières valeurs.
///
/// Retourne `None` si `periode` vaut 0 ou si `valeurs` est trop courte.
pub fn calculer_sma(valeurs: &[f64], periode: usize) -> Option<f64> {
    if periode == 0 || valeurs.len() < periode {
        return None;
    }

    let fenetre = &valeurs[valeurs.len() - periode..];
    Some(fenetre.iter().sum::<f64>() / periode as f64)
}

/// Vérifie si le volume courant dépasse `multiplicateur * SMA(historique_volume)`.
///
/// Retourne `(cassure, ratio)` où `ratio = volume_courant / SMA`.
/// Sans SMA calculable : `(false, 0.0)`. Si la SMA est nulle, le ratio vaut
/// `f64::INFINITY` pour un volume positif, sinon `0.0`.
pub fn valider_cassure_volume(
    volume_courant: f64,
    historique_volume: &[f64],
    periode: usize,
    multiplicateur: f64,
) -> (bool, f64) {
    let Some(moyenne_volume) = calculer_sma(historique_volume, periode) else {
        return (false, 0.0);
    };

    let ratio_volume = if moyenne_volume == 0.0 {
        if volume_courant > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    } else {
        volume_courant / moyenne_volume
    };

    (
        volume_courant > multiplicateur * moyenne_volume,
        ratio_volume,
    )
}

#[cfg(test)]
mod tests {
    use super::{calculer_sma, valider_cassure_volume};

    #[test]
    fn calcule_sma_et_valide_une_cassure_de_volume() {
        let historique = [10.0, 20.0, 30.0];

        assert_eq!(calculer_sma(&historique, 2), Some(25.0));
        assert_eq!(calculer_sma(&historique, 0), None);
        assert_eq!(calculer_sma(&historique[..2], 3), None);
        assert_eq!(
            valider_cassure_volume(31.0, &historique, 3, 1.0),
            (true, 1.55)
        );
        assert_eq!(
            valider_cassure_volume(30.0, &historique, 3, 1.5),
            (false, 1.5)
        );
        assert_eq!(
            valider_cassure_volume(10.0, &historique, 0, 1.0),
            (false, 0.0)
        );
    }
}
