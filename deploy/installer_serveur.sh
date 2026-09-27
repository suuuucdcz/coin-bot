#!/usr/bin/env bash
# Installe Memecoin Radar sur un serveur Ubuntu 24.04 (Google Cloud e2-micro, Oracle…) et le lance 24 h/24
# comme service système (redémarrage automatique, comme run.bat sur le PC).
#
# Usage, dans le dossier du radar sur le serveur :
#     bash deploy/installer_serveur.sh          # radar sans IA locale (règles seules pour lire les tweets)
#     bash deploy/installer_serveur.sh --ia     # + Ollama et le modèle LLM_MODEL du .env (4 Go de mémoire minimum)
#
# Avant : copier .env, data/radar.db et (option) data/session_x_export.json depuis le PC (voir HEBERGEMENT.md).
set -euo pipefail
cd "$(dirname "$0")/.."
DOSSIER="$(pwd)"
UTILISATEUR="$(whoami)"

if [ ! -f .env ]; then
    echo "❌ Fichier .env absent : copie-le depuis le PC (voir HEBERGEMENT.md), puis relance ce script."
    exit 1
fi

echo "== 1/6 Paquets système"
sudo apt-get update -y
sudo apt-get install -y python3 python3-venv python3-pip git sqlite3
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 11) else 1)' || {
    echo "❌ Python 3.11 ou plus est nécessaire : choisis l'image Ubuntu 24.04 pour le serveur."; exit 1; }
sudo timedatectl set-timezone Europe/Paris || true   # journaux à l'heure de Paris, comme sur le PC

MEM_MO="$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)"
if [ "$MEM_MO" -lt 2048 ] && ! swapon --show | grep -q .; then
    # Petite machine (e2-micro : 1 Go) : 2 Go de mémoire d'appoint sur le disque, sinon Chromium manque de place
    echo "   Mémoire : ${MEM_MO} Mo -> ajout de 2 Go de swap"
    sudo fallocate -l 2G /swapfile
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    grep -q '^/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
    echo 'vm.swappiness=10' | sudo tee /etc/sysctl.d/99-radar.conf >/dev/null
    sudo sysctl -p /etc/sysctl.d/99-radar.conf >/dev/null
fi
if [ "$MEM_MO" -lt 2048 ] && [ ! -f /etc/systemd/system/zram-radar.service ]; then
    # Swap COMPRESSÉ EN MÉMOIRE (zram), utilisé avant le swap disque. Vu en vrai sur e2-micro : le swap sur
    # disque standard (très lent) figeait le radar ; PumpPortal et Telegram décrochaient toutes les 5 min.
    echo "   Ajout d'un swap compressé en mémoire (zram, lz4)"
    sudo tee /etc/systemd/system/zram-radar.service >/dev/null <<'ZRAM'
[Unit]
Description=Swap compresse en memoire (zram) pour Memecoin Radar
Before=memecoin-radar.service

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'modprobe zram && zramctl /dev/zram0 --algorithm lz4 --size 768M && mkswap /dev/zram0 && swapon -p 100 /dev/zram0'
ExecStop=/bin/sh -c 'swapoff /dev/zram0; zramctl --reset /dev/zram0'

[Install]
WantedBy=multi-user.target
ZRAM
    sudo systemctl daemon-reload
    sudo systemctl enable --now zram-radar
fi

echo "== 2/6 Environnement Python"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt

echo "== 3/6 Navigateur de la veille X (Chromium de Playwright, sans fenêtre)"
.venv/bin/python -m playwright install --with-deps chromium
# Pas d'Edge sur un serveur Linux : Chromium, sans fenêtre
reglage() {  # reglage CLE VALEUR : remplace ou ajoute une ligne du .env
    if grep -q "^$1=" .env; then sed -i "s|^$1=.*|$1=$2|" .env; else echo "$1=$2" >> .env; fi
}
reglage X_BROWSER chromium
reglage X_HEADLESS 1
if [ "$MEM_MO" -lt 2048 ]; then
    reglage X_LIGHT 1   # ni images ni vidéos : moins de mémoire et moins de trafic sortant (1 Go gratuit/mois)
fi

echo "== 4/6 Session X"
if [ -f data/session_x_export.json ]; then
    if .venv/bin/python -m radar.sources.x_watch importer; then
        rm -f data/session_x_export.json
        echo "   (fichier de session supprimé du serveur)"
    else
        echo "   ⚠️ Session X refusée : la veille X restera inactive ici (le reste du radar fonctionne)."
    fi
else
    echo "   Pas de data/session_x_export.json : veille X inactive sur le serveur (voir HEBERGEMENT.md)."
fi

echo "== 5/6 IA locale"
if [ "${1:-}" = "--ia" ] && [ "$MEM_MO" -lt 4000 ]; then
    echo "   ⚠️ ${MEM_MO} Mo de mémoire : trop peu pour l'IA locale, elle reste désactivée."
    reglage LLM_ENABLED 0
elif [ "${1:-}" = "--ia" ]; then
    command -v ollama >/dev/null || curl -fsSL https://ollama.com/install.sh | sh
    MODELE="$(grep '^LLM_MODEL=' .env | cut -d= -f2- || true)"
    ollama pull "${MODELE:-gemma4:e4b}"
else
    reglage LLM_ENABLED 0
    echo "   IA locale désactivée (relance avec --ia pour l'installer)."
fi

echo "== 6/6 Service système (démarrage automatique et relance en cas d'arrêt)"
sudo tee /etc/systemd/system/memecoin-radar.service >/dev/null <<EOF
[Unit]
Description=Memecoin Radar (alertes Telegram)
After=network-online.target
Wants=network-online.target

[Service]
User=$UTILISATEUR
WorkingDirectory=$DOSSIER
ExecStart=$DOSSIER/.venv/bin/python -m radar.main
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now memecoin-radar
sleep 8
systemctl --no-pager --lines=0 status memecoin-radar || true
echo
echo "✅ Radar installé. Il tourne 24 h/24 et redémarre tout seul."
echo "   Journal en direct :  tail -f $DOSSIER/logs/radar.log"
echo "   Arrêter / relancer : sudo systemctl stop memecoin-radar  /  sudo systemctl restart memecoin-radar"
echo "   Mise à jour :        git pull && sudo systemctl restart memecoin-radar"
echo "⚠️  Arrête le radar du PC : deux radars sur le même bot = alertes en double et commandes Telegram en conflit."
