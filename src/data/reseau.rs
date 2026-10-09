//! Proxy HTTP partagé par les clients REST et WebSocket.

use std::sync::OnceLock;

static PROXY_CONFIGURE: OnceLock<Option<String>> = OnceLock::new();

/// Fixe le proxy issu de la configuration (`network.proxy`). Sans effet après le premier appel.
pub fn definir_proxy(proxy: Option<String>) {
    let _ = PROXY_CONFIGURE.set(proxy.filter(|p| !p.trim().is_empty()));
}

/// Proxy à utiliser : configuration d'abord, sinon variables d'environnement usuelles.
pub fn proxy() -> Option<String> {
    PROXY_CONFIGURE.get().cloned().flatten().or_else(|| {
        [
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
            "HTTP_PROXY",
            "http_proxy",
        ]
        .iter()
        .filter_map(|nom| std::env::var(nom).ok())
        .find(|valeur| !valeur.trim().is_empty())
    })
}

/// Extrait `hote:port` d'une URL de proxy (`http://127.0.0.1:9000`).
pub fn adresse_proxy(proxy: &str) -> String {
    let sans_schema = proxy.split_once("://").map_or(proxy, |(_, reste)| reste);
    let sans_chemin = sans_schema.trim_end_matches('/');
    if sans_chemin.contains(':') {
        sans_chemin.to_owned()
    } else {
        format!("{sans_chemin}:80")
    }
}

/// Client HTTP configuré avec le proxy éventuel.
pub fn client_http() -> Result<reqwest::Client, reqwest::Error> {
    let mut constructeur = reqwest::Client::builder().timeout(std::time::Duration::from_secs(30));
    if let Some(proxy) = proxy() {
        constructeur = constructeur.proxy(reqwest::Proxy::all(proxy)?);
    }
    constructeur.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extrait_l_adresse_du_proxy() {
        assert_eq!(adresse_proxy("http://127.0.0.1:9000"), "127.0.0.1:9000");
        assert_eq!(adresse_proxy("proxy.local:8080/"), "proxy.local:8080");
        assert_eq!(adresse_proxy("http://proxy.local"), "proxy.local:80");
    }
}
