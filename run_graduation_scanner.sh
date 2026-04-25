#!/bin/zsh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PY="$SCRIPT_DIR/venv/bin/python"

source "$SCRIPT_DIR/venv/bin/activate"

tmux has-session -t rbot-crypto 2>/dev/null && tmux kill-session -t rbot-crypto

tmux new -ds rbot-crypto "$PY legacy/crypto_momentum_scanner.py"

echo "Rudis crypto graduation scanner started in tmux session: rbot-crypto"
