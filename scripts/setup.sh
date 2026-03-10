#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${HOME}/.claude/local/messages"
SYSTEMD_DIR="${HOME}/.config/systemd/user"

echo "=== Legion Messages Setup ==="

# 1. Create data directories
echo "Creating data directories..."
mkdir -p "${DATA_DIR}"/{events,secrets,storybooks,logs}

# 2. Install systemd service
echo "Installing systemd service..."
mkdir -p "${SYSTEMD_DIR}"
cp "${PLUGIN_DIR}/systemd/legion-messages.service" "${SYSTEMD_DIR}/"
systemctl --user daemon-reload
echo "  Installed legion-messages.service"

# 3. Install Node dependencies and build
echo "Building server..."
cd "${PLUGIN_DIR}/server"
npm install
npm run build
echo "  Built daemon.mjs and mcp.mjs"

# 4. Check config
if [ ! -f "${DATA_DIR}/config.yml" ]; then
  echo "WARNING: No config.yml found at ${DATA_DIR}/config.yml"
  echo "  Copy the template and configure your adapters."
fi

# 5. Check secrets
if [ ! -f "${DATA_DIR}/secrets/telegram.env" ]; then
  echo "WARNING: No telegram.env found at ${DATA_DIR}/secrets/telegram.env"
  echo "  Required: TELEGRAM_API_ID, TELEGRAM_API_HASH, TELEGRAM_STRING_SESSION"
fi

echo ""
echo "=== Setup complete ==="
echo "Start daemon: systemctl --user start legion-messages"
echo "Check status: systemctl --user status legion-messages"
echo "View logs:    tail -f ${DATA_DIR}/logs/daemon.log"
