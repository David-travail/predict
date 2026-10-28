//! Proxy HTTP partagé par les clients REST et WebSocket.

use std::error::Error;
use std::sync::OnceLock;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::handshake::client::Response;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config, connect_async,
};

static PROXY_CONFIGURE: OnceLock<Option<String>> = OnceLock::new();

/// Fixe le proxy issu de la configuration (`network.proxy`). Sans effet après le premier appel.
///
/// Entrée : proxy optionnel; une chaîne vide est traitée comme une absence de proxy.
/// Sortie : enregistre la valeur une seule fois.
pub fn definir_proxy(proxy: Option<String>) {
    let _ = PROXY_CONFIGURE.set(proxy.filter(|p| !p.trim().is_empty()));
}

/// Proxy à utiliser : configuration d'abord, sinon variables d'environnement usuelles.
///
/// Sortie : URL de proxy configurée ou trouvée dans l'environnement, sinon `None`.
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
///
/// Entrée : adresse de proxy éventuellement préfixée par un schéma.
/// Sortie : adresse `hote:port` (port 80 si absent).
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
///
/// Sortie : client avec délai de 30 secondes et proxy courant, ou erreur de configuration.
pub fn client_http() -> Result<reqwest::Client, reqwest::Error> {
    let mut constructeur = reqwest::Client::builder().timeout(std::time::Duration::from_secs(30));
    if let Some(proxy) = proxy() {
        constructeur = constructeur.proxy(reqwest::Proxy::all(proxy)?);
    }
    constructeur.build()
}

/// Ouvre une connexion WebSocket directe ou via le proxy configuré.
///
/// Entrées : URL cible et hôte/port auquel établir le tunnel HTTP CONNECT.
/// Sortie : flux WebSocket et réponse de handshake, ou erreur de connexion/proxy.
pub async fn ouvrir_websocket(
    url: &str,
    hote: &str,
) -> Result<(WebSocketStream<MaybeTlsStream<TcpStream>>, Response), Box<dyn Error>> {
    let Some(proxy) = proxy() else {
        return Ok(connect_async(url).await?);
    };

    let adresse = adresse_proxy(&proxy);
    let mut tcp = timeout(Duration::from_secs(15), TcpStream::connect(&adresse))
        .await
        .map_err(|_| format!("délai dépassé en se connectant au proxy {adresse}"))??;

    let requete = format!("CONNECT {hote} HTTP/1.1\r\nHost: {hote}\r\n\r\n");
    tcp.write_all(requete.as_bytes()).await?;

    let mut reponse = Vec::new();
    let mut octet = [0u8; 1];
    while !reponse.ends_with(b"\r\n\r\n") {
        if reponse.len() > 8192 || tcp.read(&mut octet).await? == 0 {
            return Err("réponse invalide du proxy au CONNECT".into());
        }
        reponse.push(octet[0]);
    }
    let entete = String::from_utf8_lossy(&reponse);
    let statut = entete.lines().next().unwrap_or_default();
    if !statut
        .split_whitespace()
        .nth(1)
        .is_some_and(|code| code == "200")
    {
        return Err(format!("le proxy a refusé le tunnel : {statut}").into());
    }

    Ok(client_async_tls_with_config(url, tcp, None, None).await?)
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
