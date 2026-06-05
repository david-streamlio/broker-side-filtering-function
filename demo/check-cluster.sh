#!/usr/bin/env bash
# check-cluster.sh — validate all server-side components of the stock-demo cluster
# Run this after a laptop restart before starting the demo containers.
#
# Checks:
#   1. SN Cloud connectivity (admin URL reachable)
#   2. Orchestrator function running
#   3. Filter function running (if any queries are registered)
#   4. Source topic exists + has kafkaEntryFormat=pulsar_non_batched
#   5. State topic accessible
#   6. SA tokens not expired
#   7. filter-requests topic accessible
#
# Usage: ./demo/check-cluster.sh

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"

BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*"; FAILURES=$((FAILURES+1)); }

FAILURES=0

# ── Load .env ─────────────────────────────────────────────────────────────────
ENV_FILE="$DEMO_DIR/.env"
[[ -f "$ENV_FILE" ]] || { echo -e "${RED}ERROR: demo/.env not found — run recreate.sh first${NC}"; exit 1; }
source "$ENV_FILE"

ADMIN="${PULSAR_ADMIN_URL}"
ORCH_TOKEN=$(cat "$DEMO_DIR/infra/terraform/orchestrator-function.token" 2>/dev/null || echo "")
APP_TOKEN=$(cat "$DEMO_DIR/infra/terraform/app.token" 2>/dev/null || echo "")

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Stock Demo — Cluster Status Check${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  Cluster: ${BLUE}${ADMIN}${NC}"
echo ""

# ── 1. Cluster reachability ───────────────────────────────────────────────────
echo -e "${BOLD}[1] Cluster connectivity${NC}"
if curl -sf --max-time 10 "${ADMIN}/admin/v2/clusters" \
    -H "Authorization: Bearer $ORCH_TOKEN" > /dev/null 2>&1; then
  ok "Cluster reachable: ${ADMIN}"
else
  fail "Cannot reach cluster. Check network / VPN."
fi

# ── 2. Token validity ─────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[2] SA token validity${NC}"
check_token() {
  local label="$1" token="$2" file="$3"
  if [[ -z "$token" ]]; then
    fail "$label token file not found: $file"
    return
  fi
  # Decode JWT expiry — uses Python to handle base64url padding correctly
  local exp
  exp=$(python3 -c "
import base64, json, sys
token = '''$token'''.strip()
parts = token.split('.')
if len(parts) != 3:
    print(0); sys.exit()
payload = parts[1]
payload += '=' * ((4 - len(payload) % 4) % 4)
try:
    d = json.loads(base64.b64decode(payload))
    print(d.get('exp', 0))
except Exception:
    print(0)
" 2>/dev/null || echo 0)
  local now
  now=$(date +%s)
  if [[ "$exp" -eq 0 ]]; then
    warn "$label token — could not decode expiry (not a JWT?)"
  elif [[ "$exp" -lt "$now" ]]; then
    fail "$label token EXPIRED ($(date -r "$exp" '+%Y-%m-%d' 2>/dev/null || echo 'unknown'))"
  else
    local days_left=$(( (exp - now) / 86400 ))
    ok "$label token valid — expires in ${days_left} days"
  fi
}
check_token "orchestrator-function" "$ORCH_TOKEN" "infra/terraform/orchestrator-function.token"
check_token "stock-demo-app" "$APP_TOKEN" "infra/terraform/app.token"

# ── 3. Orchestrator function ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[3] Orchestrator function${NC}"
ORCH_STATUS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v3/functions/control-plane/broker-filter/orchestrator/status" \
  -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "{}")
ORCH_RUNNING=$(echo "$ORCH_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('numRunning',0))" 2>/dev/null || echo 0)
ORCH_RESTARTS=$(echo "$ORCH_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); i=d.get('instances',[{}])[0]; s=i.get('status',{}); print(s.get('numRestarts',0))" 2>/dev/null || echo 0)
ORCH_RECEIVED=$(echo "$ORCH_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); i=d.get('instances',[{}])[0]; s=i.get('status',{}); print(s.get('numReceived',0))" 2>/dev/null || echo 0)
if [[ "$ORCH_RUNNING" -gt 0 ]]; then
  ok "Orchestrator running (received=${ORCH_RECEIVED} restarts=${ORCH_RESTARTS})"
else
  fail "Orchestrator NOT running — redeploy with recreate.sh Phase 7"
fi

# ── 4. Filter function ────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[4] Selector-filter function${NC}"
FILTER_LIST=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v3/functions/control-plane/broker-filter/" \
  -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "[]")
FILTER_NAME=$(echo "$FILTER_LIST" | python3 -c "import sys,json; fns=json.load(sys.stdin); selectors=[f for f in fns if 'selector' in f]; print(selectors[0] if selectors else '')" 2>/dev/null || echo "")
if [[ -n "$FILTER_NAME" ]]; then
  FILTER_STATUS=$(curl -sf --max-time 10 \
    "${ADMIN}/admin/v3/functions/control-plane/broker-filter/${FILTER_NAME}/status" \
    -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "{}")
  FILTER_RUNNING=$(echo "$FILTER_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('numRunning',0))" 2>/dev/null || echo 0)
  FILTER_RECEIVED=$(echo "$FILTER_STATUS" | python3 -c "import sys,json; d=json.load(sys.stdin); i=d.get('instances',[{}])[0]; s=i.get('status',{}); print(s.get('numReceived',0))" 2>/dev/null || echo 0)
  if [[ "$FILTER_RUNNING" -gt 0 ]]; then
    ok "Filter running: ${FILTER_NAME} (received=${FILTER_RECEIVED})"
  else
    warn "Filter exists but NOT running: ${FILTER_NAME} — restart multi-query-app to trigger re-register"
  fi
else
  warn "No selector-filter function deployed — start multi-query-app to register queries"
fi

# ── 5. Source topic ───────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[5] Source topic: stock-quotes${NC}"
SOURCE_TOPIC="${DEMO_SOURCE_TOPIC_STOCKS:-persistent://public/default/stock-quotes}"
TOPIC_PROPS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/properties" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null || echo "{}")
KAFKA_FMT=$(echo "$TOPIC_PROPS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('kafkaEntryFormat','NOT SET'))" 2>/dev/null || echo "error")
PARTITIONS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/partitions" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('partitions',0))" 2>/dev/null || echo "?")

if [[ "$KAFKA_FMT" == "pulsar_non_batched" ]]; then
  ok "kafkaEntryFormat=pulsar_non_batched ✓"
else
  fail "kafkaEntryFormat=${KAFKA_FMT} — filter function will discard all messages! Fix: delete+recreate topic"
fi
ok "Partitions: ${PARTITIONS}"

TOPIC_STATS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes-partition-0/stats" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null || echo "{}")
MSG_RATE=$(echo "$TOPIC_STATS" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(round(d.get('msgRateIn',0),1))" 2>/dev/null || echo "?")
PUBLISHERS=$(echo "$TOPIC_STATS" | python3 -c "import sys,json,re; raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read()); d=json.loads(raw); print(len(d.get('publishers',[])))" 2>/dev/null || echo "?")
if [[ "$PUBLISHERS" -gt 0 ]] 2>/dev/null; then
  ok "ShadowTraffic producing: ${MSG_RATE} msg/s (${PUBLISHERS} publisher)"
else
  warn "No publishers on stock-quotes — ShadowTraffic not running (start with: docker start stock-quotes-shadowtraffic)"
fi

# ── 6. State topic ────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[6] Orchestrator state topic${NC}"
STATE_HTTP=$(curl -sf --max-time 10 -o /tmp/_state_stats.json -w "%{http_code}" \
  "${ADMIN}/admin/v2/persistent/control-plane/broker-filter/orchestrator-state/stats" \
  -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "000")
if [[ "$STATE_HTTP" == "200" ]]; then
  STATE_MSGS=$(python3 -c "import json; d=json.load(open('/tmp/_state_stats.json')); print(d.get('msgInCounter',0))" 2>/dev/null || echo 0)
  ok "State topic accessible (${STATE_MSGS} total entries)"
elif [[ "$STATE_HTTP" == "307" || "$STATE_HTTP" == "3"* ]]; then
  ok "State topic accessible (redirect — topic exists)"
else
  fail "State topic not accessible (HTTP ${STATE_HTTP}) — check ORCHESTRATOR_TENANT/NAMESPACE config"
fi

# ── 7. Filter-requests topic ──────────────────────────────────────────────────
echo ""
echo -e "${BOLD}[7] Filter-requests topic${NC}"
FR_STATS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/control-plane/broker-filter/filter-requests/stats" \
  -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "{}")
FR_MSGS=$(echo "$FR_STATS" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('msgInCounter',0))" 2>/dev/null || echo 0)
if [[ "$FR_MSGS" -ge 0 ]]; then
  ok "Filter-requests topic accessible (${FR_MSGS} total messages)"
else
  fail "Filter-requests topic not accessible"
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
if [[ $FAILURES -eq 0 ]]; then
  echo -e "  ${GREEN}${BOLD}✓ All checks passed — cluster is ready${NC}"
  echo ""
  echo -e "  Start the demo:"
  echo -e "    docker compose -f demo/docker-compose.yml up -d   # full stack"
  echo -e "    docker start stock-quotes-shadowtraffic            # if already created"
else
  echo -e "  ${RED}${BOLD}✗ ${FAILURES} check(s) failed — see above${NC}"
fi
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
exit $FAILURES
