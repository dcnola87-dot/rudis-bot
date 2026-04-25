#!/usr/bin/env bash
set -euo pipefail
cd "$HOME/rudis-bot"
source .venv/bin/activate
exec /usr/bin/caffeinate -dimsu -- python premarket_loop.py >> "$HOME/Library/Logs/rudis-bot/premarket.log" 2>&1
