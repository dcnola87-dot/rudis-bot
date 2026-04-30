#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
source "$SCRIPT_DIR/../venv/bin/activate"
exec /usr/bin/caffeinate -dimsu -- "$SCRIPT_DIR/../venv/bin/python" "$SCRIPT_DIR/premarket_loop.py" >> "$HOME/Library/Logs/rudis-bot/premarket.log" 2>&1
