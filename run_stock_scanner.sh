#!/bin/zsh
cd /Users/DJ/Desktop/rudis-bot/legacy

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PY="/Users/DJ/Desktop/rudis-bot/venv/bin/python"

source /Users/DJ/Desktop/rudis-bot/venv/bin/activate

tmux has-session -t rbot-stocks 2>/dev/null && tmux kill-session -t rbot-stocks

tmux new -ds rbot-stocks "$PY rth_loop.py"

echo "Rudis stock scanner started in tmux session: rbot-stocks"
