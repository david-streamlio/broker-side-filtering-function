#!/usr/bin/env bash
# check-docker.sh — verify Docker env is ready for the demo
# Checks credsStore, running container versions vs .env, and pulls if needed.
# Usage: ./demo/check-docker.sh [--fix]
#   --fix   automatically fix credsStore and recreate outdated containers

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
BOLD='\033[1m'; GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; NC='\033[0m'

ok()   { echo -e "  ${GREEN}✓${NC} $*"; }
warn() { echo -e "  ${YELLOW}⚠${NC} $*"; }
fail() { echo -e "  ${RED}✗${NC} $*"; FAILURES=$((FAILURES+1)); }

FAILURES=0
FIX="${1:-}"
DOCKER_CONFIG="$HOME/.docker/config.json"
ENV_FILE="$DEMO_DIR/.env"

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Docker env check${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo ""

# ── 1. credsStore ─────────────────────────────────────────────────────────────
echo -e "${BOLD}[1] Docker credsStore${NC}"
if [[ ! -f "$DOCKER_CONFIG" ]]; then
  warn "~/.docker/config.json not found — skipping"
else
  CREDS=$(python3 -c "import json; print(json.load(open('$DOCKER_CONFIG')).get('credsStore',''))" 2>/dev/null || echo "?")
  if [[ "$CREDS" == "desktop" ]]; then
    if [[ "$FIX" == "--fix" ]]; then
      python3 -c "
import json
with open('$DOCKER_CONFIG') as f: cfg = json.load(f)
cfg['credsStore'] = ''
with open('$DOCKER_CONFIG', 'w') as f: json.dump(cfg, f, indent=2)
print('fixed')
"
      ok "credsStore cleared (was 'desktop' — Docker Desktop restart must have reset it)"
    else
      fail "credsStore='desktop' — docker-credential-desktop not in PATH will block pulls"
      echo -e "       Run with --fix to clear it, or: python3 -c \"import json; cfg=json.load(open('$DOCKER_CONFIG')); cfg['credsStore']=''; json.dump(cfg,open('$DOCKER_CONFIG','w'),indent=2)\""
    fi
  elif [[ -z "$CREDS" ]]; then
    ok "credsStore='' (correct)"
  else
    warn "credsStore='${CREDS}' — may cause issues if helper not installed"
  fi
fi

# ── 2. Load expected versions from .env ───────────────────────────────────────
echo ""
echo -e "${BOLD}[2] Container versions (running vs expected)${NC}"

if [[ ! -f "$ENV_FILE" ]]; then
  warn "demo/.env not found — cannot check expected versions"
else
  source "$ENV_FILE"
  EXPECTED_OBS="${OBSERVABILITY_IMAGE_TAG:-unknown}"
  EXPECTED_OBS_IMAGE="${OBSERVABILITY_IMAGE:-ghcr.io/david-streamlio/broker-side-filter-observability}:${EXPECTED_OBS}"

  # Observability
  RUNNING_OBS=$(docker inspect filter-observability --format '{{.Config.Image}}' 2>/dev/null || echo "not running")
  if [[ "$RUNNING_OBS" == "not running" ]]; then
    warn "filter-observability: not running"
  elif [[ "$RUNNING_OBS" == "$EXPECTED_OBS_IMAGE" ]]; then
    ok "filter-observability: ${RUNNING_OBS##*:} ✓"
  else
    fail "filter-observability: running ${RUNNING_OBS##*:}, expected ${EXPECTED_OBS}"
    if [[ "$FIX" == "--fix" ]]; then
      echo -e "       Recreating with ${EXPECTED_OBS}..."
      docker compose -f "$DEMO_DIR/docker-compose.yml" up -d --pull always --force-recreate observability
      ok "filter-observability recreated with ${EXPECTED_OBS}"
    else
      echo -e "       Run with --fix to recreate it, or:"
      echo -e "       docker compose -f demo/docker-compose.yml up -d --pull always --force-recreate observability"
    fi
  fi

  # ShadowTraffic — check container running AND generator not stalled
  RUNNING_ST=$(docker inspect stock-quotes-shadowtraffic --format '{{.Config.Image}}' 2>/dev/null || echo "not running")
  if [[ "$RUNNING_ST" == "not running" ]]; then
    fail "stock-quotes-shadowtraffic: not running (start with: docker start stock-quotes-shadowtraffic)"
  else
    # Sample events_sent_total twice 3s apart — if it didn't move, generator is stalled
    ST_METRICS_1=$(docker exec stock-quotes-shadowtraffic curl -s --max-time 3 localhost:9400/metrics 2>/dev/null \
      | grep "generator_events_sent_total" | grep -oE '[0-9]+\.[0-9]+$' || echo "0")
    sleep 3
    ST_METRICS_2=$(docker exec stock-quotes-shadowtraffic curl -s --max-time 3 localhost:9400/metrics 2>/dev/null \
      | grep "generator_events_sent_total" | grep -oE '[0-9]+\.[0-9]+$' || echo "0")
    ST_ERRORS=$(docker logs stock-quotes-shadowtraffic --since 2m 2>&1 \
      | grep -c "TimeoutException\|Cancelled.*PRODUCE\|REQUEST_TIMED_OUT" 2>/dev/null; true)
    if [[ "$ST_METRICS_1" == "$ST_METRICS_2" && "$ST_METRICS_1" != "0" ]]; then
      fail "stock-quotes-shadowtraffic: generator STALLED (events_sent not incrementing) — restart: docker restart stock-quotes-shadowtraffic"
      if [[ "$FIX" == "--fix" ]]; then
        docker restart stock-quotes-shadowtraffic > /dev/null
        ok "stock-quotes-shadowtraffic: restarted"
      fi
    elif [[ "$ST_ERRORS" -gt 5 ]]; then
      warn "stock-quotes-shadowtraffic: running but ${ST_ERRORS} produce errors in last 2m (may recover)"
    else
      ok "stock-quotes-shadowtraffic: producing (${ST_METRICS_2%.*} events sent)"
    fi
  fi

  # multi-query-app — check running AND no recent Pulsar connection loss
  RUNNING_MQ=$(docker inspect multi-query-demo --format '{{.State.Status}}' 2>/dev/null || echo "not running")
  if [[ "$RUNNING_MQ" == "running" ]]; then
    # Detect zombie: JVM alive but Pulsar connection dead
    UPTIME_S=$(docker inspect multi-query-demo --format '{{.State.StartedAt}}' 2>/dev/null \
      | python3 -c "
import sys,datetime
t=sys.stdin.read().strip().replace('Z','+00:00')
started=datetime.datetime.fromisoformat(t)
now=datetime.datetime.now(datetime.timezone.utc)
print(int((now-started).total_seconds()))
" 2>/dev/null || echo 0)
    CONN_ERR=$(docker logs multi-query-demo --since 2m 2>&1 \
      | grep -c "No available hosts\|keep-alive timeout\|Forcing connection to close" 2>/dev/null; true)
    if [[ "$CONN_ERR" -gt 0 && "$UPTIME_S" -gt 120 ]]; then
      fail "multi-query-demo: running but Pulsar connection lost (${CONN_ERR} errors in last 2m)"
      if [[ "$FIX" == "--fix" ]]; then
        docker restart multi-query-demo > /dev/null
        ok "multi-query-demo: restarted"
      else
        echo -e "       Fix: docker restart multi-query-demo"
      fi
    else
      HEALTH=$(docker inspect multi-query-demo --format '{{.State.Health.Status}}' 2>/dev/null || echo "none")
      if [[ "$HEALTH" == "unhealthy" ]]; then
        fail "multi-query-demo: unhealthy (Docker healthcheck failing) — restart: docker restart multi-query-demo"
      else
        ok "multi-query-demo: running${HEALTH:+ (health: $HEALTH)}"
      fi
    fi
  else
    fail "multi-query-demo: ${RUNNING_MQ} — restart: docker compose -f demo/docker-compose.yml up -d multi-query-app"
    if [[ "$FIX" == "--fix" ]]; then
      docker compose -f "$DEMO_DIR/docker-compose.yml" up -d multi-query-app > /dev/null
      ok "multi-query-demo: started"
    fi
  fi
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
if [[ $FAILURES -eq 0 ]]; then
  echo -e "  ${GREEN}${BOLD}✓ Docker env is ready${NC}"
else
  echo -e "  ${RED}${BOLD}✗ ${FAILURES} issue(s) found${NC}"
  echo ""
  echo -e "  Re-run with --fix to auto-correct:"
  echo -e "    ./demo/check-docker.sh --fix"
fi
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
exit $FAILURES
