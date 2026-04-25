#!/bin/zsh
cd /Users/DJ/Desktop/rudis-bot

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

PY="/Users/DJ/Desktop/rudis-bot/venv/bin/python"

source /Users/DJ/Desktop/rudis-bot/venv/bin/activate

tmux has-session -t rbot-crypto 2>/dev/null && tmux kill-session -t rbot-crypto

tmux new -ds rbot-crypto "$PY legacy/crypto_momentum_scanner.py"

echo "Rudis crypto graduation scanner started in tmux session: rbot-crypto"
