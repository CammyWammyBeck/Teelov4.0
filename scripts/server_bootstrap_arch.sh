#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> Installing system packages"

BASE_PACKAGES=(
  git
  python
  python-pip
  xorg-server-xvfb
  x11vnc
  nss
  atk
  at-spi2-atk
  libxcomposite
  libxdamage
  libxrandr
  mesa
  alsa-lib
  pango
  cairo
  gtk3
  libdrm
  libxshmfence
  libxext
  libxfixes
  libxkbcommon
  libxcb
  libxrender
  libxcursor
  glib2
  libcups
  libxss
  libxtst
  wayland
  fontconfig
  ttf-liberation
)

# Optional packages: may not exist in all Arch repos.
OPTIONAL_PACKAGES=(
  novnc
  websockify
  libgbm
)

INSTALL_PACKAGES=()
for pkg in "${BASE_PACKAGES[@]}"; do
  if pacman -Si "${pkg}" >/dev/null 2>&1; then
    INSTALL_PACKAGES+=("${pkg}")
  else
    echo "!! Package not found in repos: ${pkg}"
  fi
done

for pkg in "${OPTIONAL_PACKAGES[@]}"; do
  if pacman -Si "${pkg}" >/dev/null 2>&1; then
    INSTALL_PACKAGES+=("${pkg}")
  else
    echo "!! Optional package not found in repos: ${pkg}"
  fi
done

sudo pacman -S --needed --noconfirm "${INSTALL_PACKAGES[@]}"

echo "==> Creating virtualenv"
python -m venv "${ROOT_DIR}/venv"
source "${ROOT_DIR}/venv/bin/activate"
python -m pip install --upgrade pip

echo "==> Installing Python deps"
pip install -r "${ROOT_DIR}/requirements-all.txt"

echo "==> Installing Playwright Chromium"
python -m playwright install chromium

echo "==> Done"
