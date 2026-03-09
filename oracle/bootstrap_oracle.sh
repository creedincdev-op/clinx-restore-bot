#!/usr/bin/env bash
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/creedincdev-op/clinx-restore-bot.git}"
APP_DIR="${APP_DIR:-/home/ubuntu/clinx-restore-bot}"
BOT_USER="${BOT_USER:-ubuntu}"

sudo apt update
sudo apt install -y git python3 python3-venv

if [ ! -d "$APP_DIR/.git" ]; then
  sudo -u "$BOT_USER" git clone "$REPO_URL" "$APP_DIR"
else
  sudo -u "$BOT_USER" bash -lc "cd '$APP_DIR' && git pull origin main"
fi

sudo -u "$BOT_USER" bash -lc "cd '$APP_DIR' && python3 -m venv .venv && source .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt"

echo "Bootstrap complete. Next steps:"
echo "1) create $APP_DIR/.env with BOT_TOKEN=..."
echo "2) sudo cp $APP_DIR/oracle/clinx-bot.service /etc/systemd/system/clinx-bot.service"
echo "3) sudo systemctl daemon-reload && sudo systemctl enable --now clinx-bot"
