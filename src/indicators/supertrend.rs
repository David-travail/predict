use crate::core::modeles::Bougie;

/// Calcule le Supertrend (ATR de Wilder) et retourne `true` si la dernière bougie est en tendance haussière.
/// Retourne `None` s'il y a moins de `periode + 1` bougies.
pub fn supertrend_haussier(
    bougies: &[Bougie],
    periode: usize,
    multiplicateur: f64,
) -> Option<bool> {
    if periode == 0 || bougies.len() < periode + 1 {
        return None;
    }

    let vrai_intervalle = |i: usize| {
        let b = &bougies[i];
        let cloture_prec = bougies[i - 1].cloture;
        (b.haut - b.bas)
            .max((b.haut - cloture_prec).abs())
            .max((b.bas - cloture_prec).abs())
    };

    let mut atr = (1..=periode).map(vrai_intervalle).sum::<f64>() / periode as f64;
    let mut bande_haute = f64::MAX;
    let mut bande_basse = f64::MIN;
    let mut haussier = true;

    for i in periode..bougies.len() {
        if i > periode {
            atr = (atr * (periode as f64 - 1.0) + vrai_intervalle(i)) / periode as f64;
        }
        let b = &bougies[i];
        let milieu = (b.haut + b.bas) / 2.0;
        let haute = milieu + multiplicateur * atr;
        let basse = milieu - multiplicateur * atr;
        let cloture_prec = bougies[i - 1].cloture;

        // Une bande ne s'éloigne du prix que si la clôture précédente l'a franchie.
        let nouvelle_haute = if haute < bande_haute || cloture_prec > bande_haute {
            haute
        } else {
            bande_haute
        };
        let nouvelle_basse = if basse > bande_basse || cloture_prec < bande_basse {
            basse
        } else {
            bande_basse
        };
        bande_haute = nouvelle_haute;
        bande_basse = nouvelle_basse;

        if haussier && b.cloture < bande_basse {
            haussier = false;
        } else if !haussier && b.cloture > bande_haute {
            haussier = true;
        }
    }
    Some(haussier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bougie(i: i64, cloture: f64) -> Bougie {
        Bougie {
            symbole: "BTCEUR".into(),
            unite_temps: "1h".into(),
            instant_ouverture: Utc.timestamp_opt(i * 3600, 0).unwrap(),
            ouverture: cloture,
            haut: cloture + 1.0,
            bas: cloture - 1.0,
            cloture,
            volume: 1.0,
            cloturee: true,
        }
    }

    #[test]
    fn tendance_haussiere_sur_une_hausse_reguliere() {
        let serie: Vec<Bougie> = (0..60).map(|i| bougie(i, 100.0 + i as f64 * 2.0)).collect();
        assert_eq!(supertrend_haussier(&serie, 10, 3.0), Some(true));
    }

    #[test]
    fn tendance_baissiere_sur_une_baisse_reguliere() {
        let serie: Vec<Bougie> = (0..60).map(|i| bougie(i, 300.0 - i as f64 * 2.0)).collect();
        assert_eq!(supertrend_haussier(&serie, 10, 3.0), Some(false));
    }

    #[test]
    fn historique_insuffisant() {
        let serie: Vec<Bougie> = (0..5).map(|i| bougie(i, 100.0)).collect();
        assert_eq!(supertrend_haussier(&serie, 10, 3.0), None);
    }
}
