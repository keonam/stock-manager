#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-/opt/stock-manager}"
APP_USER="${APP_USER:-ubuntu}"
REPO_URL="${REPO_URL:-https://github.com/keonam/stock-manager.git}"
SERVICE_NAME="stock-manager"

sudo apt-get update
sudo apt-get install -y git python3 python3-venv python3-pip

if [ ! -d "$APP_DIR/.git" ]; then
  sudo mkdir -p "$APP_DIR"
  sudo chown "$APP_USER:$APP_USER" "$APP_DIR"
  sudo -u "$APP_USER" git clone "$REPO_URL" "$APP_DIR"
else
  sudo -u "$APP_USER" git -C "$APP_DIR" fetch origin main
  sudo -u "$APP_USER" git -C "$APP_DIR" reset --hard origin/main
fi

sudo -u "$APP_USER" python3 -m venv "$APP_DIR/.venv"
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/python" -m pip install --upgrade pip
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"
sudo -u "$APP_USER" "$APP_DIR/.venv/bin/pip" install pykrx==1.2.4 --no-deps

sudo cp "$APP_DIR/deploy/stock-manager.service" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo sed -i "s/^User=.*/User=${APP_USER}/" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo sed -i "s/^Group=.*/Group=${APP_USER}/" "/etc/systemd/system/${SERVICE_NAME}.service"
sudo systemctl daemon-reload
sudo systemctl enable --now "$SERVICE_NAME"
sudo systemctl status "$SERVICE_NAME" --no-pager

