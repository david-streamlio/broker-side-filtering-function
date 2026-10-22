#!/usr/bin/env bash
# Live demo: register a query, let it run, then modify it live and measure
# time from modification until the first message matching the NEW selector arrives.
#
# Default: AAPL → TSLA  (shows instant hot-swap of broker-side filter)
# Usage: ./demo/live-modify-demo.sh [from-selector] [to-selector]

set -euo pipefail

OBSERVABILITY="${OBSERVABILITY_URL:-http://localhost:3200}"
FROM_SEL="${1:-__key__ IN ('AAPL')}"
TO_SEL="${2:-__key__ IN ('TSLA')}"
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"

BOLD='\033[1m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'
YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

ts() { python3 -c "from datetime import datetime; print(datetime.now().strftime('%H:%M:%S.%f')[:12])"; }
now_ms() { python3 -c "import time; print(int(time.time()*1000))"; }

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Live Filter Modification Demo${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  FROM: ${RED}${FROM_SEL}${NC}"
echo -e "  TO:   ${GREEN}${TO_SEL}${NC}"
echo ""

# ── Step 1: Register initial query ───────────────────────────────────────────
echo -e "${BLUE}[1/5] Registering initial query: ${FROM_SEL}${NC}"
T_CREATE=$(now_ms)

RESPONSE=$(curl -s -X POST "$OBSERVABILITY/api/simulate/register-direct" \
  -H "Content-Type: application/json" \
  -d "{\"selector\": \"$FROM_SEL\", \"outputTopicPrefix\": \"modify-demo\"}")

QUERY_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('queryId',''))" 2>/dev/null)
OUTPUT_TOPIC=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('outputTopic',''))" 2>/dev/null)

[[ -z "$QUERY_ID" ]] && { echo "ERROR: $RESPONSE"; exit 1; }
echo -e "  ${GREEN}✓${NC} queryId: ${QUERY_ID}"
echo -e "    topic:   ${OUTPUT_TOPIC}"
echo ""

# ── Step 2: Wait for first FROM message ──────────────────────────────────────
echo -e "${BLUE}[2/5] Waiting for first message matching original selector...${NC}"
for i in $(seq 1 30); do
  COUNT=$(curl -s "$OBSERVABILITY/api/topic/peek?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")&count=1" 2>/dev/null | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); print(len(json.loads(raw)))" 2>/dev/null || echo 0)
  if [[ "$COUNT" -gt 0 ]]; then
    echo -e "  ${GREEN}✓${NC} Original filter active — messages flowing for: ${RED}${FROM_SEL}${NC}"
    break
  fi
  printf "  waiting... (%ds)\r" "$i"; sleep 1
done
echo ""

# ── Step 3: Let it run for a moment ──────────────────────────────────────────
echo -e "${BLUE}[3/5] Letting filter run for 3 seconds (showing FROM messages)...${NC}"
for i in 3 2 1; do
  PEEK=$(curl -s "$OBSERVABILITY/api/topic/peek?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")&count=3" 2>/dev/null)
  KEYS=$(echo "$PEEK" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); msgs=json.loads(raw); print(', '.join(m.get('key','?') for m in msgs[:3]))" 2>/dev/null || echo "...")
  printf "  %ds — recent keys: %s\r" "$i" "$KEYS"
  sleep 1
done
echo ""
echo ""

# ── Step 4: Modify the selector ──────────────────────────────────────────────
echo -e "${BLUE}[4/5] ${BOLD}MODIFYING selector live (no restart!)${NC}"
T_MODIFY=$(now_ms)
T_MODIFY_HUMAN=$(ts)

HTTP_CODE=$(curl -s -o /tmp/modify_result.txt -w "%{http_code}" \
  -X POST "$OBSERVABILITY/api/query/${QUERY_ID}/selector" \
  -H "Content-Type: application/json" \
  -d "{\"selector\": \"$TO_SEL\"}")

if [[ "$HTTP_CODE" == "204" || "$HTTP_CODE" == "200" ]]; then
  echo -e "  ${GREEN}✓${NC} Selector modified at ${BOLD}${T_MODIFY_HUMAN}${NC} (HTTP ${HTTP_CODE})"
  echo -e "    NEW selector: ${GREEN}${TO_SEL}${NC}"
else
  echo -e "  ${RED}✗${NC} Modify failed (HTTP ${HTTP_CODE}): $(cat /tmp/modify_result.txt)"
  exit 1
fi
echo ""

# ── Step 5: Wait for first TO message ────────────────────────────────────────
echo -e "${BLUE}[5/5] Waiting for first message matching NEW selector...${NC}"
FROM_KEY=$(echo "$FROM_SEL" | grep -oE "'[A-Z]+'" | head -1 | tr -d "'")
TO_KEY=$(echo "$TO_SEL" | grep -oE "'[A-Z]+'" | head -1 | tr -d "'")

for i in $(seq 1 60); do
  PEEK=$(curl -s "$OBSERVABILITY/api/topic/peek?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")&count=10" 2>/dev/null)
  NEW_MSG=$(echo "$PEEK" | python3 -c "
import sys,json,re
raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read())
msgs=json.loads(raw)
for m in msgs:
    k=m.get('key','')
    pt=m.get('publishTime',0)
    if '$TO_KEY' in k or $((T_MODIFY)) < pt:
        print(f'{k}|{pt}')
        break
" 2>/dev/null || echo "")

  if [[ -n "$NEW_MSG" ]]; then
    T_FIRST=$(now_ms)
    T_FIRST_HUMAN=$(ts)
    FIRST_KEY=$(echo "$NEW_MSG" | cut -d'|' -f1)
    ELAPSED=$((T_FIRST - T_MODIFY))
    echo ""
    echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
    echo -e "  Modified at:         ${T_MODIFY_HUMAN}"
    echo -e "  First new message:   ${T_FIRST_HUMAN}"
    echo -e "  First key:           ${BOLD}${FIRST_KEY}${NC}"
    echo ""
    echo -e "  ${GREEN}${BOLD}Time from MODIFY → first ${TO_KEY} message: ${ELAPSED}ms${NC}"
    echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  View topic: ${BLUE}$OBSERVABILITY/topic?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")${NC}"
    echo -e "  Cleanup:    curl -X DELETE $OBSERVABILITY/api/query/${QUERY_ID}"
    exit 0
  fi
  printf "  waiting for %s... (%ds)\r" "$TO_KEY" "$i"
  sleep 1
done

echo -e "${YELLOW}No ${TO_KEY} message in 60s — check the observability${NC}"
echo -e "  Cleanup: curl -X DELETE $OBSERVABILITY/api/query/${QUERY_ID}"
