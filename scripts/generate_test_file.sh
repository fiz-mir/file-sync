#!/usr/bin/env bash
# Creates a 100 MB dummy file for testing transfers.
set -euo pipefail

DEST="${1:-./demo_data/restaurant_data.db}"
SIZE_MB=100

mkdir -p "$(dirname "$DEST")"
echo "Generating ${SIZE_MB} MB test file at ${DEST} …"
dd if=/dev/urandom of="$DEST" bs=1M count=$SIZE_MB 2>/dev/null
echo "Done: $(du -sh "$DEST" | cut -f1)  →  $DEST"
