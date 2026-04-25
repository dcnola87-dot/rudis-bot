#!/bin/zsh
cd /Users/DJ/Desktop/rudis-bot

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PY="/Users/DJ/Desktop/rudis-bot/venv/bin/python"

source /Users/DJ/Desktop/rudis-bot/venv/bin/activate

# Kill old rbot session quietly
tmux has-session -t rbot 2>/dev/null && tmux kill-session -t rbot

# Start execution bot
tmux new -ds rbot "$PY execution_bot/main.py"

echo "Rudis Execution Bot started in tmux session: rbot"