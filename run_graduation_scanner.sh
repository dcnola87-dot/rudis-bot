#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PY="$SCRIPT_DIR/venv/bin/python"

source "$SCRIPT_DIR/venv/bin/activate"

exec "$PY" legacy/crypto_loop.py
