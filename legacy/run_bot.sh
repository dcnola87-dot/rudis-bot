#!/bin/zsh
cd /Users/DJ/Desktop/rudis-bot

# Make sure PATH works under launchd
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin"

TMUX="$(command -v tmux)"
PY="/Users/DJ/Desktop/rudis-bot/venv/bin/python"

# Activate venv (safe if already active)
source /Users/DJ/Desktop/rudis-bot/venv/bin/activate

# Kill any previous tmux session quietly, then start fresh
$TMUX has-session -t rbot 2>/dev/null && $TMUX kill-session -t rbot 2>/dev/null || true

# Start the Discord bot worker in its own tmux session
$TMUX new -ds rbot "$PY bot_worker.py"
