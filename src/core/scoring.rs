use crate::core::modeles::{AnalyseTechnique, Tendance};

pub fn calculer_score(analyse: &AnalyseTechnique, seuil_alerte_minimal: u8) -> (u8, bool) {
    let score = if analyse.tendance != Tendance::Neutre {
        40 + if analyse.adx_valide { 30 } else { 0 } + if analyse.volume_valide { 30 } else { 0 }
    } else {
        0
    };

    (score, score >= seuil_alerte_minimal)
}

#[cfg(test)]
mod tests {
    use super::calculer_score;
    use crate::core::modeles::{AnalyseTechnique, Tendance};

    fn creer_analyse(tendance: Tendance, adx_valide: bool, volume_valide: bool) -> AnalyseTechnique {
        AnalyseTechnique {
            tendance,
            valeur_adx: 0.0,
            adx_valide,
            moyenne_volume: 0.0,
            volume_valide,
        }
    }

    #[test]
    fn calcule_le_score_et_verifie_le_seuil_alerte() {
        let h = Tendance::Haussiere;
        let b = Tendance::Baissiere;
        assert_eq!(calculer_score(&creer_analyse(h, true, true), 80), (100, true));
        assert_eq!(calculer_score(&creer_analyse(b, true, true), 80), (100, true));
        assert_eq!(calculer_score(&creer_analyse(h, false, true), 80), (70, false));
        assert_eq!(
            calculer_score(&creer_analyse(Tendance::Neutre, true, true), 0),
            (0, true)
        );
    }
}
