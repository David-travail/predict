//! Moyenne mobile exponentielle (EMA).

/// Calcule l'EMA d'une série de prix.
///
/// La première valeur est la moyenne simple (SMA) des `periode` premiers prix ;
/// les suivantes appliquent `EMA = (prix - EMA_précédente) * k + EMA_précédente`
/// avec `k = 2 / (periode + 1)`.
///
/// Retourne `None` si `periode` vaut 0 ou si la série est trop courte. Sinon le
/// vecteur contient `prix.len() - periode + 1` valeurs, alignées sur les prix à
/// partir de l'indice `periode - 1`.
///
/// Entrées : série de prix et période de calcul.
/// Sortie : série EMA alignée sur la première fenêtre complète, ou `None` si les entrées sont insuffisantes.
pub fn calculer_ema(prix: &[f64], periode: usize) -> Option<Vec<f64>> {
    if periode == 0 || prix.len() < periode {
        return None;
    }

    // Amorçage : SMA sur la première fenêtre.
    let premiere_ema = prix[..periode].iter().sum::<f64>() / periode as f64;
    let multiplicateur = 2.0 / (periode + 1) as f64;
    let mut valeurs_ema = Vec::with_capacity(prix.len() - periode + 1);
    valeurs_ema.push(premiere_ema);

    let mut ema_precedente = premiere_ema;
    for prix_courant in &prix[periode..] {
        let ema = (prix_courant - ema_precedente) * multiplicateur + ema_precedente;
        valeurs_ema.push(ema);
        ema_precedente = ema;
    }

    Some(valeurs_ema)
}

#[cfg(test)]
mod tests {
    use super::calculer_ema;

    #[test]
    fn calcule_ema_a_partir_de_la_sma_initiale() {
        let prix = [1.0, 2.0, 3.0, 4.0, 5.0];

        assert_eq!(calculer_ema(&prix, 3), Some(vec![2.0, 3.0, 4.0]));
        assert_eq!(calculer_ema(&prix, 0), None);
        assert_eq!(calculer_ema(&prix[..2], 3), None);
    }
}
