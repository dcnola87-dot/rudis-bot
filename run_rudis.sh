#!/bin/zsh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PY="$SCRIPT_DIR/venv/bin/python"

source "$SCRIPT_DIR/venv/bin/activate"

# Kill old rbot session quietly
tmux has-session -t rbot 2>/dev/null && tmux kill-session -t rbot

# Start execution bot
tmux new -ds rbot "$PY execution_bot/main.py"

echo "Rudis Execution Bot started in tmux session: rbot"
