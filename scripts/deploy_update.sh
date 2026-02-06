#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Updating repo"
git -C "${ROOT_DIR}" pull

echo "==> Updating venv deps"
source "${ROOT_DIR}/venv/bin/activate"
pip install -r "${ROOT_DIR}/requirements-all.txt"
python -m playwright install chromium

echo "==> Running migrations"
alembic -c "${ROOT_DIR}/alembic.ini" upgrade head

if command -v systemctl >/dev/null 2>&1; then
  if systemctl list-units --type=service --all | grep -q "teelo-update.service"; then
    echo "==> Restarting teelo-update.service"
    sudo systemctl restart teelo-update.service
  fi
fi

echo "==> Done"
