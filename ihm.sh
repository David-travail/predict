#!/usr/bin/env bash
# Lance l'IHM predict sur macOS, détachée du terminal.
#
# Usage :
#   ./ihm.sh                     Se connecte à l'adresse par défaut (ou celle de .predict_connect)
#   ./ihm.sh 192.168.1.10:8765   Se connecte au moteur indiqué
#
# Adresse du moteur : argument, sinon PREDICT_CONNECT, sinon le fichier .predict_connect
# (une ligne IP:PORT), sinon le défaut du programme (127.0.0.1:8765).
# Exécutable : PREDICT_BIN, sinon target/release/predict, sinon target/debug/predict ;
# s'il n'existe pas, il est compilé avec cargo build --release.

set -euo pipefail

DOSSIER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DOSSIER"

# Lancé depuis le Finder, le PATH ne contient pas cargo.
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"

FICHIER_CONSOLE="$DOSSIER/predict-ihm.log"

erreur() {
    echo "$1" >&2
    # Affiche aussi une alerte quand le script est lancé sans terminal (Finder, Dock).
    if [[ ! -t 1 ]]; then
        osascript -e "display alert \"Predict IHM\" message \"$1\" as critical" >/dev/null 2>&1 || true
    fi
    exit 1
}

# Adresse du moteur
ADRESSE="${1:-${PREDICT_CONNECT:-}}"
if [[ -z "$ADRESSE" && -f "$DOSSIER/.predict_connect" ]]; then
    ADRESSE="$(head -n 1 "$DOSSIER/.predict_connect" | tr -d '\r\n ')"
fi

# Exécutable
BINAIRE="${PREDICT_BIN:-}"
if [[ -z "$BINAIRE" ]]; then
    for candidat in "$DOSSIER/target/release/predict" "$DOSSIER/target/debug/predict"; do
        if [[ -x "$candidat" ]]; then
            BINAIRE="$candidat"
            break
        fi
    done
fi
if [[ -z "$BINAIRE" ]]; then
    command -v cargo >/dev/null || erreur "Exécutable introuvable et cargo absent : lancez cargo build --release."
    echo "Compilation (cargo build --release)…"
    cargo build --release >>"$FICHIER_CONSOLE" 2>&1 \
        || erreur "La compilation a échoué : voir $FICHIER_CONSOLE"
    BINAIRE="$DOSSIER/target/release/predict"
fi

ARGS=()
[[ -n "$ADRESSE" ]] && ARGS+=(--connect "$ADRESSE")

{
    echo
    echo "===== IHM $(date '+%Y-%m-%d %H:%M:%S') : $BINAIRE ${ARGS[*]:-} ====="
} >>"$FICHIER_CONSOLE"

nohup "$BINAIRE" ${ARGS[@]+"${ARGS[@]}"} </dev/null >>"$FICHIER_CONSOLE" 2>&1 &
PID=$!

sleep 2
if ! kill -0 "$PID" 2>/dev/null; then
    erreur "L'IHM s'est arrêtée au démarrage. Voir $FICHIER_CONSOLE"
fi
echo "IHM lancée (PID $PID)${ADRESSE:+, connectée à $ADRESSE}."
