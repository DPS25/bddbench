#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"
ENV_TEMPLATE="$SCRIPT_DIR/bddbench-nightly.env"
ENV_TARGET="$HOME/.config/bddbench-nightly.env"

echo "[install] Source unit dir:  $SCRIPT_DIR"
echo "[install] Target unit dir:  $UNIT_DIR"

mkdir -p "$UNIT_DIR"

cp "$SCRIPT_DIR/bddbench-nightly.service" "$UNIT_DIR/"
cp "$SCRIPT_DIR/bddbench-nightly.timer"   "$UNIT_DIR/"

if [[ ! -f "$ENV_TARGET" ]]; then
  if [[ -f "$ENV_TEMPLATE" ]]; then
    cp "$ENV_TEMPLATE" "$ENV_TARGET"
    echo "[install] Copied env template to: $ENV_TARGET"
    echo "[install] >>> Please edit this file and set a REAL INFLUX_TOKEN etc. <<<"
  else
    echo "[install] WARNING: Env template not found: $ENV_TEMPLATE"
    echo "[install]          Please create $ENV_TARGET manually."
  fi
else
  echo "[install] Existing env file found: $ENV_TARGET (kept as-is)"
fi

echo "[install] Reloading user systemd daemon..."
systemctl --user daemon-reload

echo "[install] Enabling + starting timer: bddbench-nightly.timer"
systemctl --user enable --now bddbench-nightly.timer

echo "[test] Running bddbench-nightly.service once for sanity check..."
if systemctl --user start --wait bddbench-nightly.service; then
  echo "[test] Service ran successfully."
else
  echo "[test] Service FAILED, recent logs:"
  systemctl --user status bddbench-nightly.service --no-pager || true
  journalctl --user -xeu bddbench-nightly.service --no-pager | tail -n 40 || true
fi

