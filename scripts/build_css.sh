#!/bin/bash
set -e

# Ensure we're in the project root
cd "$(dirname "$0")/.."

TAILWIND_VERSION="v3.4.1"
TAILWIND_ARCH="linux-x64"
TAILWIND_BIN="./tailwindcss"

# Download Tailwind CLI if not present
if [ ! -f "$TAILWIND_BIN" ]; then
    echo "Downloading Tailwind CLI $TAILWIND_VERSION..."
    curl -sL "https://github.com/tailwindlabs/tailwindcss/releases/download/$TAILWIND_VERSION/tailwindcss-$TAILWIND_ARCH" -o "$TAILWIND_BIN"
    chmod +x "$TAILWIND_BIN"
fi

echo "Building CSS..."
$TAILWIND_BIN -i ./src/teelo/web/static/css/input.css -o ./src/teelo/web/static/css/styles.css --minify

echo "CSS build complete!"
