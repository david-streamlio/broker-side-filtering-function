#!/usr/bin/env bash
# Live demo: register a query, measure time-to-first-message via Kafka consumer
#
# Shows the full pipeline latency:
#   T0 = query registered with orchestrator
#   T1 = first matching message arrives on the private output topic
#   T1 - T0 = end-to-end "filter activation latency"
#
# Usage: ./demo/live-query-demo.sh [selector]
# Default selector: __key__ IN ('AAPL','MSFT','NVDA')

set -euo pipefail

OBSERVABILITY="${OBSERVABILITY_URL:-http://localhost:3200}"
SELECTOR="${1:-__key__ IN ('AAPL','MSFT','NVDA')}"
DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"

BOLD='\033[1m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Live Query Demo — Broker-Side Filter Observability${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Selector: ${YELLOW}${SELECTOR}${NC}"
echo ""

# ── Step 1: Register query ───────────────────────────────────────────────────
echo -e "${BLUE}[1/4] Registering query with orchestrator...${NC}"
T0_MS=$(python3 -c "import time; print(int(time.time()*1000))")
T0_HUMAN=$(python3 -c "from datetime import datetime; print(datetime.now().strftime('%H:%M:%S.%f')[:12])")

RESPONSE=$(curl -s -X POST "$OBSERVABILITY/api/simulate/register-direct" \
  -H "Content-Type: application/json" \
  -d "{\"selector\": \"$SELECTOR\", \"outputTopicPrefix\": \"live-demo\"}")

QUERY_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('queryId',''))" 2>/dev/null)
OUTPUT_TOPIC=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('outputTopic',''))" 2>/dev/null)

if [[ -z "$QUERY_ID" ]]; then
  echo "ERROR: Failed to register query. Response: $RESPONSE"
  exit 1
fi

echo -e "  ${GREEN}✓${NC} Query registered at ${BOLD}${T0_HUMAN}${NC}"
echo -e "    queryId:     ${QUERY_ID}"
echo -e "    outputTopic: ${OUTPUT_TOPIC}"
echo ""

# ── Step 2: Wait for filter function to activate ────────────────────────────
echo -e "${BLUE}[2/4] Waiting for filter function to pick up selector...${NC}"
ACTIVATED=false
for i in $(seq 1 20); do
  FILTER_INFO=$(curl -s "$OBSERVABILITY/api/query/${QUERY_ID}" 2>/dev/null)
  FILTER_FN=$(echo "$FILTER_INFO" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(d.get('filterFunctionName',''))" 2>/dev/null || true)
  if [[ -n "$FILTER_FN" && "$FILTER_FN" != "null" ]]; then
    T_ACTIVATED=$(python3 -c "import time; print(int(time.time()*1000))")
    ACTIVATION_MS=$((T_ACTIVATED - T0_MS))
    echo -e "  ${GREEN}✓${NC} Filter function active after ${BOLD}${ACTIVATION_MS}ms${NC}: $FILTER_FN"
    ACTIVATED=true
    break
  fi
  printf "  waiting... (%ds)\r" "$i"
  sleep 1
done
[[ "$ACTIVATED" == "false" ]] && echo -e "  ${YELLOW}⚠ Filter not yet active (query registered, waiting for first message anyway)${NC}"
echo ""

# ── Step 3: Poll for first message ──────────────────────────────────────────
echo -e "${BLUE}[3/4] Polling output topic for first matched message...${NC}"
FIRST_MSG_MS=""
FIRST_MSG_KEY=""
FIRST_MSG_ELAPSED=""

for i in $(seq 1 60); do
  PEEK=$(curl -s "$OBSERVABILITY/api/topic/peek?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")&count=1" 2>/dev/null)
  MSG_COUNT=$(echo "$PEEK" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(len(d))" 2>/dev/null || echo "0")

  if [[ "$MSG_COUNT" -gt 0 ]]; then
    T1_MS=$(python3 -c "import time; print(int(time.time()*1000))")
    T1_HUMAN=$(python3 -c "from datetime import datetime; print(datetime.now().strftime('%H:%M:%S.%f')[:12])")
    FIRST_MSG_ELAPSED=$((T1_MS - T0_MS))
    FIRST_MSG_KEY=$(echo "$PEEK" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(d[0].get('key','?'))" 2>/dev/null || echo "?")
    FIRST_MSG_PUBLISH=$(echo "$PEEK" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(d[0].get('publishTime',0))" 2>/dev/null || echo "0")
    break
  fi
  printf "  no messages yet (%ds)...\r" "$i"
  sleep 1
done
echo ""

# ── Step 4: Results ──────────────────────────────────────────────────────────
echo -e "${BLUE}[4/4] Results${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
if [[ -n "$FIRST_MSG_ELAPSED" ]]; then
  echo -e "  Query created:       ${T0_HUMAN}"
  echo -e "  First message at:    ${T1_HUMAN}"
  echo -e "  First message key:   ${BOLD}${FIRST_MSG_KEY}${NC}"
  echo ""
  echo -e "  ${GREEN}${BOLD}Time to first message: ${FIRST_MSG_ELAPSED}ms${NC}"
  echo ""
  if [[ -n "$ACTIVATION_MS" ]]; then
    FILTER_LATENCY=$((FIRST_MSG_ELAPSED - ACTIVATION_MS))
    echo -e "  Breakdown:"
    echo -e "    Orchestrator activation: ~${ACTIVATION_MS}ms"
    echo -e "    Filter processing:       ~${FILTER_LATENCY}ms"
  fi
else
  echo -e "  ${YELLOW}No messages arrived within 60s${NC}"
  echo -e "  Check the observability at $OBSERVABILITY"
fi
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""
echo -e "  View output topic: ${BLUE}$OBSERVABILITY/topic?name=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$OUTPUT_TOPIC'))")${NC}"
echo -e "  Query detail:      ${BLUE}$OBSERVABILITY/query/${QUERY_ID}${NC}"
echo -e "  To clean up:       curl -X DELETE $OBSERVABILITY/api/query/${QUERY_ID}"
echo ""
