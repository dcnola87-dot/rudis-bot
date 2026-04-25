#!/bin/zsh
cd /Users/DJ/Desktop/rudis-bot

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PY="/Users/DJ/Desktop/rudis-bot/venv/bin/python"

source /Users/DJ/Desktop/rudis-bot/venv/bin/activate

tmux has-session -t rbot-helius 2>/dev/null && tmux kill-session -t rbot-helius

tmux new -ds rbot-helius "$PY execution_bot/connectors/helius_webhook.py"

echo "Rudis Helius listener started in tmux session: rbot-helius"
