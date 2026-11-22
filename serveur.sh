#!/usr/bin/env bash
# Lance le moteur predict en serveur sur le Raspberry Pi, détaché du terminal :
# la fenêtre (ou la session SSH) peut être fermée sans arrêter le serveur.
#
# Usage :
#   ./serveur.sh [demarrer]   Démarre le serveur en arrière-plan (défaut)
#   ./serveur.sh arreter      Arrête proprement le serveur (équivalent Ctrl+C)
#   ./serveur.sh redemarrer   Arrête puis redémarre
#   ./serveur.sh statut       Indique si le serveur tourne
#   ./serveur.sh journal      Suit la sortie console (Ctrl+C quitte le suivi, pas le serveur)
#
# Jeton : PREDICT_TOKEN dans l'environnement, sinon le fichier .predict_token à côté
# du script (une seule ligne, au moins 16 caractères ; faire chmod 600 .predict_token).
#
# Variables facultatives :
#   PREDICT_BIN     chemin de l'exécutable (défaut : ./predict, puis target/release/predict)
#   PREDICT_LISTEN  adresse d'écoute (défaut : celle du programme, 0.0.0.0:8765)

set -euo pipefail

DOSSIER="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DOSSIER"

FICHIER_PID="$DOSSIER/predict-serveur.pid"
FICHIER_CONSOLE="$DOSSIER/predict-console.log"
FICHIER_JETON="$DOSSIER/.predict_token"

trouver_binaire() {
    if [[ -n "${PREDICT_BIN:-}" ]]; then
        echo "$PREDICT_BIN"
        return
    fi
    for candidat in "$DOSSIER/predict" "$DOSSIER/target/release/predict"; do
        if [[ -x "$candidat" ]]; then
            echo "$candidat"
            return
        fi
    done
    echo "Exécutable introuvable : placez 'predict' à côté du script," \
        "lancez 'cargo build --release' ou définissez PREDICT_BIN." >&2
    exit 1
}

pid_actif() {
    [[ -f "$FICHIER_PID" ]] || return 1
    local pid
    pid="$(cat "$FICHIER_PID")"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        echo "$pid"
        return 0
    fi
    rm -f "$FICHIER_PID"
    return 1
}

demarrer() {
    local pid
    if pid="$(pid_actif)"; then
        echo "Le serveur tourne déjà (PID $pid)."
        return 0
    fi

    if [[ -z "${PREDICT_TOKEN:-}" && -f "$FICHIER_JETON" ]]; then
        PREDICT_TOKEN="$(head -n 1 "$FICHIER_JETON" | tr -d '\r\n')"
    fi
    if [[ -z "${PREDICT_TOKEN:-}" ]]; then
        echo "PREDICT_TOKEN non défini : exportez-le ou écrivez-le dans $FICHIER_JETON." >&2
        exit 1
    fi
    if (( ${#PREDICT_TOKEN} < 16 )); then
        echo "PREDICT_TOKEN doit contenir au moins 16 caractères." >&2
        exit 1
    fi
    export PREDICT_TOKEN

    local binaire
    binaire="$(trouver_binaire)"
    local args=(--serve)
    if [[ -n "${PREDICT_LISTEN:-}" ]]; then
        args+=(--listen "$PREDICT_LISTEN")
    fi

    {
        echo
        echo "===== Démarrage $(date '+%Y-%m-%d %H:%M:%S') : $binaire ${args[*]} ====="
    } >>"$FICHIER_CONSOLE"

    # setsid : nouvelle session, sans terminal de contrôle ; nohup : ignore SIGHUP.
    # stdin fermé et sorties redirigées pour ne rien garder lié à la fenêtre.
    nohup setsid "$binaire" "${args[@]}" </dev/null >>"$FICHIER_CONSOLE" 2>&1 &
    echo $! >"$FICHIER_PID"

    sleep 2
    if pid="$(pid_actif)"; then
        echo "Serveur démarré (PID $pid). Vous pouvez fermer la fenêtre."
        echo "Sortie console : $FICHIER_CONSOLE"
    else
        echo "Le serveur s'est arrêté au démarrage. Dernières lignes :" >&2
        tail -n 20 "$FICHIER_CONSOLE" >&2
        exit 1
    fi
}

arreter() {
    local pid
    if ! pid="$(pid_actif)"; then
        echo "Le serveur ne tourne pas."
        return 0
    fi
    # SIGINT = Ctrl+C : le serveur s'arrête proprement.
    kill -INT "$pid"
    for _ in $(seq 1 20); do
        if ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$FICHIER_PID"
            echo "Serveur arrêté."
            return 0
        fi
        sleep 0.5
    done
    echo "Pas d'arrêt après 10 s : arrêt forcé."
    kill -TERM "$pid" 2>/dev/null || true
    sleep 1
    kill -KILL "$pid" 2>/dev/null || true
    rm -f "$FICHIER_PID"
}

statut() {
    local pid
    if pid="$(pid_actif)"; then
        echo "Le serveur tourne (PID $pid)."
        ps -o pid,etime,%cpu,rss,cmd -p "$pid" 2>/dev/null || true
    else
        echo "Le serveur ne tourne pas."
        return 1
    fi
}

case "${1:-demarrer}" in
    demarrer | start) demarrer ;;
    arreter | stop) arreter ;;
    redemarrer | restart) arreter; demarrer ;;
    statut | status) statut ;;
    journal | log) tail -n 50 -f "$FICHIER_CONSOLE" ;;
    *)
        echo "Usage : $0 [demarrer|arreter|redemarrer|statut|journal]" >&2
        exit 2
        ;;
esac
