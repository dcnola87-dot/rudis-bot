#!/bin/zsh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

./run_rudis.sh
./run_helius_listener.sh
./run_graduation_scanner.sh
./run_stock_scanner.sh

echo "All Rudis bot processes started."
