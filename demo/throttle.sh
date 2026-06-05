#!/usr/bin/env bash
# Toggle ShadowTraffic throttle between 5ms (~150 msg/s) and 1ms (~800 msg/s)
# Usage: ./demo/throttle.sh [5|1]  — no arg toggles automatically

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$DEMO_DIR/shadowtraffic/stock-quotes.json"
COMPOSE="$DEMO_DIR/docker-compose.yml"

current=$(grep -oE '"throttleMs": [0-9]+' "$CONFIG" | grep -oE '[0-9]+$' || echo "${throttle_ms}")

if [[ "${1:-}" == "5" || "${1:-}" == "1" ]]; then
    target="$1"
elif [[ "$current" == "1" ]]; then
    target="5"
else
    target="1"
fi

echo "throttleMs: $current → $target"

# Replace in config (handles both template var and hardcoded values)
sed -i '' \
    -e "s/\"throttleMs\": \${throttle_ms}/\"throttleMs\": $target/" \
    -e "s/\"throttleMs\": [0-9]*/\"throttleMs\": $target/" \
    "$CONFIG"

docker compose -f "$COMPOSE" up -d --no-deps --force-recreate shadowtraffic 2>&1 | tail -2

echo "Done — ShadowTraffic restarted at throttleMs=$target"
[[ "$target" == "1" ]] && echo "  → ~800 msg/s" || echo "  → ~150 msg/s"
