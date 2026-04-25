#!/bin/zsh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/legacy"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PY="$SCRIPT_DIR/venv/bin/python"

source "$SCRIPT_DIR/venv/bin/activate"

tmux has-session -t rbot-stocks 2>/dev/null && tmux kill-session -t rbot-stocks

tmux new -ds rbot-stocks "$PY rth_loop.py"

echo "Rudis stock scanner started in tmux session: rbot-stocks"
