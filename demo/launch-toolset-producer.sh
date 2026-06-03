#!/usr/bin/env bash
# Launch the in-cluster Kafka producer (kafka-producer-pod.py) on a toolset pod.
#
# Picks the target pod (auto-detect / interactive / -p), makes sure the
# kafka-python lib + script are present, then runs the producer. Records are
# keyed (default LOCAL) so a `__key__ = '<KEY>'` broker-side filter isolates them.
#
# Usage:
#   ./launch-toolset-producer.sh                       # auto-pick toolset pod, 10 msgs, key=LOCAL
#   ./launch-toolset-producer.sh -n 0 -t 200           # continuous @ 5 msg/s
#   ./launch-toolset-producer.sh -k DESK7 -n 0 -t 50   # custom key, faster
#   ./launch-toolset-producer.sh -p stock-demo-toolset-0
#   ./launch-toolset-producer.sh -d -n 0               # detached in-pod (survives this shell)
#
# Choosing the pod (highest precedence first):
#   1. -p POD flag
#   2. $TOOLSET_POD env var          (e.g. export TOOLSET_POD=stock-demo-toolset-0)
#   3. auto-detect pods matching $POD_PATTERN (default: "toolset")
#
# Flags:
#   -p POD        target pod (overrides $TOOLSET_POD / auto-detect)
#   -c CONTAINER  container name (default: $TOOLSET_CONTAINER or pulsar)
#   -n COUNT      message count, 0 = forever (default: 10)
#   -k KEY        Kafka message key for every record (default: LOCAL)
#   -t MS         throttle between messages in ms (default: 100)
#   -d            detached: nohup inside the pod, logs to /tmp/producer-<KEY>.log
#   -h            help

set -euo pipefail

DEMO_DIR="$(cd "$(dirname "$0")" && pwd)"
SCRIPT_LOCAL="$DEMO_DIR/kafka-producer-pod.py"
SCRIPT_REMOTE="/tmp/kafka-producer-pod.py"
POD_PATTERN="${POD_PATTERN:-toolset}"

CONTAINER="${TOOLSET_CONTAINER:-pulsar}"
POD="${TOOLSET_POD:-}"
COUNT=10
KEY="LOCAL"
THROTTLE=100
DETACH=false

BOLD='\033[1m'; GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

usage() { sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while getopts ":p:c:n:k:t:dh" opt; do
  case "$opt" in
    p) POD="$OPTARG" ;;
    c) CONTAINER="$OPTARG" ;;
    n) COUNT="$OPTARG" ;;
    k) KEY="$OPTARG" ;;
    t) THROTTLE="$OPTARG" ;;
    d) DETACH=true ;;
    h) usage 0 ;;
    \?) echo -e "${RED}Unknown flag: -$OPTARG${NC}"; usage 1 ;;
    :)  echo -e "${RED}-$OPTARG needs a value${NC}"; usage 1 ;;
  esac
done

[[ -f "$SCRIPT_LOCAL" ]] || { echo -e "${RED}Missing $SCRIPT_LOCAL${NC}"; exit 1; }
command -v kubectl >/dev/null || { echo -e "${RED}kubectl not found${NC}"; exit 1; }

# ── Pick the pod ────────────────────────────────────────────────────────────────
# ($POD may already be set via -p or $TOOLSET_POD; otherwise auto-detect.)
if [[ -z "$POD" ]]; then
  # bash 3.2 (macOS) has no `mapfile`, so build the array portably.
  PODS=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && PODS+=("$line")
  done < <(kubectl get pods -o name 2>/dev/null | sed 's#^pod/##' | grep -i "$POD_PATTERN" || true)
  if   [[ ${#PODS[@]} -eq 0 ]]; then
    echo -e "${RED}No pods matching '$POD_PATTERN'. Pass one with -p.${NC}"; exit 1
  elif [[ ${#PODS[@]} -eq 1 ]]; then
    POD="${PODS[0]}"
    echo -e "${BLUE}Auto-selected pod:${NC} ${BOLD}$POD${NC}"
  else
    echo -e "${BOLD}Select a toolset pod:${NC}"
    select choice in "${PODS[@]}"; do
      [[ -n "${choice:-}" ]] && { POD="$choice"; break; }
    done
  fi
fi

echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "${BOLD} Launch toolset Kafka producer${NC}"
echo -e "${BOLD}═══════════════════════════════════════════════════${NC}"
echo -e "  Pod:       ${BOLD}$POD${NC}  (container: $CONTAINER)"
echo -e "  Key:       ${YELLOW}$KEY${NC}"
echo -e "  Count:     $([[ "$COUNT" == "0" ]] && echo '∞ (forever)' || echo "$COUNT")"
echo -e "  Throttle:  ${THROTTLE}ms"
echo -e "  Detached:  $DETACH"
echo ""

# ── Ensure deps + script on the pod ──────────────────────────────────────────────
echo -e "${BLUE}[1/3] Ensuring kafka-python on pod...${NC}"
if ! kubectl exec "$POD" -c "$CONTAINER" -- python3 -c 'import kafka' >/dev/null 2>&1; then
  kubectl exec "$POD" -c "$CONTAINER" -- pip3 install -q --break-system-packages kafka-python-ng
fi
echo -e "  ${GREEN}✓${NC} kafka-python present"

echo -e "${BLUE}[2/3] Copying producer script...${NC}"
kubectl cp "$SCRIPT_LOCAL" "$POD:$SCRIPT_REMOTE" -c "$CONTAINER"
echo -e "  ${GREEN}✓${NC} $SCRIPT_REMOTE"

# ── Run ──────────────────────────────────────────────────────────────────────────
echo -e "${BLUE}[3/3] Starting producer...${NC}"
if $DETACH; then
  LOG="/tmp/producer-${KEY}.log"
  kubectl exec "$POD" -c "$CONTAINER" -- bash -c \
    "nohup python3 $SCRIPT_REMOTE -n $COUNT --key '$KEY' --throttle-ms $THROTTLE > $LOG 2>&1 & echo \"pid \$!\""
  echo ""
  echo -e "  ${GREEN}✓ detached${NC} on $POD, logging to ${BOLD}$LOG${NC}"
  echo -e "  tail:  kubectl exec $POD -c $CONTAINER -- tail -f $LOG"
  echo -e "  stop:  kubectl exec $POD -c $CONTAINER -- pkill -f kafka-producer-pod"
else
  echo -e "  ${YELLOW}(streaming — Ctrl-C to stop)${NC}\n"
  exec kubectl exec -i "$POD" -c "$CONTAINER" -- \
    python3 "$SCRIPT_REMOTE" -n "$COUNT" --key "$KEY" --throttle-ms "$THROTTLE"
fi
