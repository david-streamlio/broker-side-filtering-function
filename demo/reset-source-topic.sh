#!/usr/bin/env bash
# reset-source-topic.sh — delete stock-quotes (any partition count) and recreate
# with 1 partition + kafkaEntryFormat=pulsar_non_batched on both parent and partition-0.
#
# Use after:
#   - Changing the partition count and wanting to reset to 1
#   - "Discarding corrupted message" errors in the filter function logs
#   - kafkaEntryFormat getting lost (e.g. after topic recreation by Terraform)
#
# Requires: demo/.env + infra/terraform/orchestrator-function.token (admin rights)
#           and infra/terraform/app.token (for partition-level props)
#
# Usage: ./demo/reset-source-topic.sh [--yes]
#   --yes   skip confirmation prompt (for scripted use)

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*"; exit 1; }

# ── Config ────────────────────────────────────────────────────────────────────
ENV_FILE="$DEMO_DIR/.env"
[[ -f "$ENV_FILE" ]] || fail "demo/.env not found"
source "$ENV_FILE"

ADMIN="${PULSAR_ADMIN_URL}"
ORCH_TOKEN=$(cat "$DEMO_DIR/infra/terraform/orchestrator-function.token" 2>/dev/null || "")
APP_TOKEN=$(cat  "$DEMO_DIR/infra/terraform/app.token" 2>/dev/null || "")

# Mint a superuser token — needed for topic deletion (app + orch SAs can't delete)
cd "$DEMO_DIR/infra/terraform"
SUPER_TOKEN=$(snctl -O o-mj3r8 auth get-token stock-demo-instance \
  -f streamnative-key.json 2>/dev/null \
  | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' | tail -1 || echo "")
cd - > /dev/null

if [[ -z "$SUPER_TOKEN" ]]; then
  fail "Could not mint superuser token. Run: snctl -O o-mj3r8 auth get-token stock-demo-instance -f demo/infra/terraform/streamnative-key.json"
fi

TOPIC="persistent://public/default/stock-quotes"
SUB_FILTER="control-plane%2Fbroker-filter%2Fselector-filter-stock-quotes-adcb16d1"

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Reset source topic: stock-quotes${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""

# ── Current state ─────────────────────────────────────────────────────────────
CURRENT=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/partitions" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null || echo '{"partitions":0}')
CURRENT_PARTS=$(echo "$CURRENT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('partitions','?'))" 2>/dev/null || echo "?")
CURRENT_FMT=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/properties" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('kafkaEntryFormat','NOT SET'))" 2>/dev/null || echo "?")

echo -e "  Current state:"
echo -e "    partitions:      ${YELLOW}${CURRENT_PARTS}${NC}"
echo -e "    kafkaEntryFormat: ${YELLOW}${CURRENT_FMT}${NC}"
echo ""
echo -e "  Target state:"
echo -e "    partitions:      ${GREEN}1${NC}"
echo -e "    kafkaEntryFormat: ${GREEN}pulsar_non_batched${NC}"
echo ""
warn "This will DELETE all messages in stock-quotes (synthetic data — safe to drop)"
echo ""

# ── Confirmation ──────────────────────────────────────────────────────────────
if [[ "${1:-}" != "--yes" ]]; then
  read -r -p "  Proceed? [y/N] " confirm
  [[ "${confirm,,}" == "y" ]] || { echo "Aborted."; exit 0; }
fi
echo ""

# ── Step 1: Stop ShadowTraffic ─────────────────────────────────────────────────
echo -e "${BOLD}[1/6] Stopping ShadowTraffic...${NC}"
if docker stop stock-quotes-shadowtraffic > /dev/null 2>&1; then
  ok "ShadowTraffic stopped"
else
  warn "ShadowTraffic not running (OK)"
fi

# ── Step 2: Stop filter function ──────────────────────────────────────────────
echo ""
echo -e "${BOLD}[2/6] Stopping filter function...${NC}"
HTTP=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" \
  -X POST "${ADMIN}/admin/v3/functions/control-plane/broker-filter/selector-filter-stock-quotes-adcb16d1/stop" \
  -H "Authorization: Bearer $ORCH_TOKEN" 2>/dev/null || echo "000")
if [[ "$HTTP" == "204" ]]; then
  ok "Filter function stopped"
else
  warn "Filter function not running or not found (HTTP $HTTP) — continuing"
fi
sleep 3

# ── Step 3: Delete all subscriptions ──────────────────────────────────────────
echo ""
echo -e "${BOLD}[3/6] Deleting subscriptions on all partitions...${NC}"
for p in $(seq 0 "$((CURRENT_PARTS - 1))"); do
  for sub in "$SUB_FILTER" "multiTopicsReader"; do
    # Get actual subscription names for multiTopicsReader (name varies)
    SUBS=$(curl -sf --max-time 10 \
      "${ADMIN}/admin/v2/persistent/public/default/stock-quotes-partition-${p}/subscriptions" \
      -H "Authorization: Bearer $SUPER_TOKEN" 2>/dev/null || echo "[]")
    MATCHING=$(echo "$SUBS" | python3 -c "
import sys,json,urllib.parse
raw=sys.stdin.read()
try:
    subs=json.loads(raw)
    for s in subs:
        if 'selector' in s or 'multiTopics' in s:
            print(urllib.parse.quote(s, safe=''))
except: pass
" 2>/dev/null || true)
    while IFS= read -r encoded_sub; do
      [[ -z "$encoded_sub" ]] && continue
      CODE=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" \
        -X DELETE "${ADMIN}/admin/v2/persistent/public/default/stock-quotes-partition-${p}/subscription/${encoded_sub}?force=true" \
        -H "Authorization: Bearer $SUPER_TOKEN" 2>/dev/null || echo "000")
      [[ "$CODE" == "204" ]] && ok "Deleted sub on partition-${p}" || true
    done <<< "$MATCHING"
  done
done

# ── Step 4: Delete partitions and parent ───────────────────────────────────────
echo ""
echo -e "${BOLD}[4/6] Deleting topic...${NC}"
# Delete each partition individually first
for p in $(seq 0 "$((CURRENT_PARTS - 1))"); do
  CODE=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" \
    -X DELETE "${ADMIN}/admin/v2/persistent/public/default/stock-quotes-partition-${p}?force=true" \
    -H "Authorization: Bearer $SUPER_TOKEN" 2>/dev/null || echo "000")
  [[ "$CODE" == "204" ]] && ok "Deleted partition-${p}" || warn "partition-${p}: HTTP ${CODE}"
done
sleep 2
# Delete partitioned topic metadata
CODE=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" \
  -X DELETE "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/partitions?force=true" \
  -H "Authorization: Bearer $SUPER_TOKEN" 2>/dev/null || echo "000")
[[ "$CODE" == "204" ]] && ok "Deleted parent topic metadata" || warn "Parent delete: HTTP ${CODE} (may already be gone)"

# ── Step 5: Recreate with 1 partition ─────────────────────────────────────────
echo ""
echo -e "${BOLD}[5/6] Recreating stock-quotes with 1 partition...${NC}"
sleep 2
CODE=$(curl -sf --max-time 10 -o /tmp/_create.txt -w "%{http_code}" \
  -X PUT "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/partitions" \
  -H "Authorization: Bearer $SUPER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '1' 2>/dev/null || echo "000")
if [[ "$CODE" == "204" ]]; then
  ok "Created stock-quotes with 1 partition"
else
  fail "Failed to create topic (HTTP ${CODE}): $(cat /tmp/_create.txt 2>/dev/null)"
fi

# ── Step 6: Set kafkaEntryFormat on both parent and partition-0 ────────────────
echo ""
echo -e "${BOLD}[6/6] Setting kafkaEntryFormat=pulsar_non_batched...${NC}"
sleep 1
for target in "stock-quotes" "stock-quotes-partition-0"; do
  CODE=$(curl -sf --max-time 10 -o /dev/null -w "%{http_code}" \
    -X PUT "${ADMIN}/admin/v2/persistent/public/default/${target}/properties" \
    -H "Authorization: Bearer $SUPER_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"kafkaEntryFormat":"pulsar_non_batched"}' 2>/dev/null || echo "000")
  [[ "$CODE" == "204" ]] && ok "${target}: kafkaEntryFormat=pulsar_non_batched" || warn "${target}: HTTP ${CODE}"
done

# ── Verify ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}Verifying...${NC}"
FINAL_PARTS=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/partitions" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('partitions','?'))" 2>/dev/null || echo "?")
FINAL_FMT=$(curl -sf --max-time 10 \
  "${ADMIN}/admin/v2/persistent/public/default/stock-quotes/properties" \
  -H "Authorization: Bearer $APP_TOKEN" 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('kafkaEntryFormat','NOT SET'))" 2>/dev/null || echo "?")
ok "partitions=${FINAL_PARTS}  kafkaEntryFormat=${FINAL_FMT}"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  ${GREEN}${BOLD}✓ Topic reset complete${NC}"
echo ""
echo -e "  Next steps:"
echo -e "    docker start stock-quotes-shadowtraffic   # restart producer"
echo -e "    # multi-query-app will re-register queries automatically"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
