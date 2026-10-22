#!/usr/bin/env bash
# bulk-register-queries.sh — register N queries automatically, one per key
# seen on the source topic (via the observability /api/simulate/symbols peek).
#
# Usage:
#   ./demo/bulk-register-queries.sh -n 5
#   ./demo/bulk-register-queries.sh -n 10 -t persistent://public/default/stock-quotes
#   ./demo/bulk-register-queries.sh -n 3 -p myfilter    # custom output topic prefix
#   ./demo/bulk-register-queries.sh --list              # show available keys, no register
#
# Flags:
#   -n COUNT   number of queries to create (required unless --list)
#   -t TOPIC   source topic (default: auto-detect from observability registry)
#   -p PREFIX  output topic prefix (default: auto-<key>)
#   -u URL     observability URL (default: http://localhost:3200)
#   --list     list available keys and exit without registering

set -euo pipefail

BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*"; exit 1; }

OBS_URL="http://localhost:3200"
COUNT=""
TOPIC=""
PREFIX=""
LIST_ONLY=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n) COUNT="$2"; shift 2 ;;
    -t) TOPIC="$2"; shift 2 ;;
    -p) PREFIX="$2"; shift 2 ;;
    -u) OBS_URL="$2"; shift 2 ;;
    --list) LIST_ONLY=true; shift ;;
    -h|--help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) fail "Unknown flag: $1" ;;
  esac
done

[[ "$LIST_ONLY" == "false" && -z "$COUNT" ]] && fail "Specify number of queries with -n N"
[[ "$LIST_ONLY" == "false" && "$COUNT" -lt 1 ]] 2>/dev/null || true

# ── Fetch available keys from observability ───────────────────────────────────
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Bulk query registration${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""

PARAMS="count=100"
[[ -n "$TOPIC" ]] && PARAMS="topic=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$TOPIC'))")&count=100"

SYMBOLS_RESP=$(python3 -c "
import urllib.request, json
with urllib.request.urlopen('$OBS_URL/api/simulate/symbols?$PARAMS') as r:
    print(r.read().decode())
" 2>/dev/null) || fail "Cannot reach observability at $OBS_URL"

KEYS=$(echo "$SYMBOLS_RESP" | python3 -c "
import sys, json
d = json.load(sys.stdin)
keys = d.get('symbols', [])
for k in keys:
    print(k)
")

SOURCE_TOPIC=$(echo "$SYMBOLS_RESP" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('sourceTopic') or '')
")

KEY_COUNT=$(echo "$KEYS" | grep -c . || echo 0)

echo -e "  Source topic: ${BLUE}${SOURCE_TOPIC:-auto-detect}${NC}"
echo -e "  Available keys: ${YELLOW}${KEY_COUNT}${NC}"
echo ""

if [[ "$LIST_ONLY" == "true" ]]; then
  echo -e "${BOLD}Available keys:${NC}"
  echo "$KEYS" | awk '{printf "  %2d. %s\n", NR, $0}'
  echo ""
  echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
  exit 0
fi

if [[ "$KEY_COUNT" -eq 0 ]]; then
  fail "No keys found on source topic — is ShadowTraffic running?"
fi

# Pick the first COUNT keys (or all if COUNT > available)
TO_CREATE=$(echo "$KEYS" | head -n "$COUNT")
ACTUAL=$(echo "$TO_CREATE" | grep -c . || echo 0)

if [[ "$ACTUAL" -lt "$COUNT" ]]; then
  warn "Only $ACTUAL unique keys available (requested $COUNT) — creating $ACTUAL queries"
fi

echo -e "  Creating ${GREEN}${ACTUAL}${NC} quer$([ "$ACTUAL" -eq 1 ] && echo "y" || echo "ies"):"
echo ""

# ── Register one query per key ────────────────────────────────────────────────
OK=0; FAIL=0

while IFS= read -r KEY; do
  [[ -z "$KEY" ]] && continue
  KEY_LOWER=$(echo "$KEY" | tr '[:upper:]' '[:lower:]')
  OUT_PREFIX="${PREFIX:-auto-${KEY_LOWER}}"

  RESULT=$(OBS="$OBS_URL" SRC="$SOURCE_TOPIC" KEY="$KEY" PFX="$OUT_PREFIX" python3 - <<'PYEOF'
import os, json, urllib.request
key = os.environ["KEY"]
prefix = os.environ["PFX"]
src = os.environ["SRC"]
obs = os.environ["OBS"]
payload = json.dumps({
    "selector": f"__key__ = '{key}'",
    "outputTopicPrefix": prefix,
    "sourceTopic": src if src else None,
}).encode()
req = urllib.request.Request(
    f"{obs}/api/simulate/register-direct",
    data=payload, headers={"Content-Type": "application/json"}, method="POST"
)
try:
    with urllib.request.urlopen(req) as r:
        d = json.loads(r.read())
        print("ok", d.get("queryId","?")[:8], d.get("outputTopic","?").split("/")[-1][:30])
except Exception as e:
    print("err", str(e)[:80])
PYEOF
)

  STATUS=$(echo "$RESULT" | cut -d' ' -f1)
  INFO=$(echo "$RESULT" | cut -d' ' -f2-)

  if [[ "$STATUS" == "ok" ]]; then
    ok "key=${KEY} → queryId=${INFO}"
    OK=$((OK+1))
  else
    warn "key=${KEY} → FAILED: ${INFO}"
    FAIL=$((FAIL+1))
  fi
done <<< "$TO_CREATE"

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
if [[ $FAIL -eq 0 ]]; then
  echo -e "  ${GREEN}${BOLD}✓ ${OK} quer$([ "$OK" -eq 1 ] && echo "y" || echo "ies") registered${NC}"
else
  echo -e "  ${OK} registered, ${RED}${FAIL} failed${NC}"
fi
echo -e "  Open ${BLUE}${OBS_URL}${NC} to see them in the overview"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
