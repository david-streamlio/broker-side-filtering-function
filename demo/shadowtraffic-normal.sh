#!/usr/bin/env bash
# Switch ShadowTraffic to NORMAL speed: throttleMs=5 (~150 msg/s)
set -euo pipefail
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"

cp "$DEMO_DIR/shadowtraffic/stock-quotes-normal.json" \
   "$DEMO_DIR/shadowtraffic/stock-quotes.json"

docker compose -f "$DEMO_DIR/docker-compose.yml" \
  up -d --no-deps --force-recreate shadowtraffic 2>&1 | tail -2

echo "✓ ShadowTraffic → NORMAL (throttleMs=5, ~150 msg/s)"
