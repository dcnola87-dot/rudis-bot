#!/bin/zsh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PY="$SCRIPT_DIR/venv/bin/python"

source "$SCRIPT_DIR/venv/bin/activate"

tmux has-session -t rbot-helius 2>/dev/null && tmux kill-session -t rbot-helius

tmux new -ds rbot-helius "$PY execution_bot/connectors/helius_webhook.py"

echo "Rudis Helius listener started in tmux session: rbot-helius"
