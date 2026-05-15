#!/bin/bash
#
# One-shot setup for the multi-query demo. Run inside a pulsar-admin-capable
# container (apachepulsar/pulsar:4.x); creates the source + control topics and
# grants the demo SA the permissions ShadowTraffic + the SDK need.
#
# Idempotent: "already exists" / "already granted" responses are treated as
# success, so re-running after a partial failure is safe.
#
# Required env (the docker-compose service sets these from .env):
#   PULSAR_ADMIN_URL      — https://pc-<id>.<region>.snio.cloud
#   PULSAR_TOKEN_FILE     — path inside the container to the SA's JWT
#   DEMO_SOURCE_TOPIC     — full FQN, e.g. persistent://public/default/stock-quotes
#   DEMO_CONTROL_TOPIC    — full FQN, e.g. persistent://public/default/filter-requests
#   DEMO_SA_PRINCIPAL     — the SASL/JWT principal that ShadowTraffic + the demo
#                           app authenticate as (e.g.
#                           tradewise-demo-tradewise-backend@o-mj3r8.auth.streamnative.cloud)
#   DEMO_SOURCE_PARTITIONS — number of partitions for the source topic (default 1)

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:?required}"
PULSAR_TOKEN_FILE="${PULSAR_TOKEN_FILE:?required}"
DEMO_SOURCE_TOPIC="${DEMO_SOURCE_TOPIC:?required}"
DEMO_CONTROL_TOPIC="${DEMO_CONTROL_TOPIC:?required}"
DEMO_SA_PRINCIPAL="${DEMO_SA_PRINCIPAL:?required}"
DEMO_SOURCE_PARTITIONS="${DEMO_SOURCE_PARTITIONS:-1}"

[[ -f "$PULSAR_TOKEN_FILE" ]] || { echo -e "${RED}❌ token file not found at $PULSAR_TOKEN_FILE${NC}"; exit 1; }
TOKEN=$(cat "$PULSAR_TOKEN_FILE")

# Shared client config — auth-plugin must be AuthenticationToken explicitly to
# bypass pulsar-admin's default OAuth2 client-credentials path, which fails 401
# even with a valid JWT (the same gotcha we hit on the orchestrator deploy).
PA() {
    pulsar-admin \
        --admin-url "$PULSAR_ADMIN_URL" \
        --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken \
        --auth-params "token:$TOKEN" \
        "$@"
}

# Extract tenant/namespace from a topic FQN like persistent://t/ns/topic
parse_namespace() {
    local fqn="$1"
    # strip scheme + leading slashes
    local rest="${fqn#persistent://}"
    rest="${rest#non-persistent://}"
    # tenant/ns/topic → tenant/ns
    echo "$rest" | awk -F'/' '{print $1"/"$2}'
}

SOURCE_NS=$(parse_namespace "$DEMO_SOURCE_TOPIC")
CONTROL_NS=$(parse_namespace "$DEMO_CONTROL_TOPIC")

echo -e "${BLUE}🚀 Multi-query demo bootstrap${NC}"
echo "  Admin URL:       $PULSAR_ADMIN_URL"
echo "  Source topic:    $DEMO_SOURCE_TOPIC  (ns=$SOURCE_NS, partitions=$DEMO_SOURCE_PARTITIONS)"
echo "  Control topic:   $DEMO_CONTROL_TOPIC  (ns=$CONTROL_NS)"
echo "  Demo SA:         $DEMO_SA_PRINCIPAL"
echo ""

# ── Step 1: ensure the source topic exists as a partitioned topic ────────────
echo -e "${BLUE}📦 Step 1/4 — ensure source topic exists${NC}"
if OUT=$(PA topics create-partitioned-topic "$DEMO_SOURCE_TOPIC" -p "$DEMO_SOURCE_PARTITIONS" 2>&1); then
    echo -e "${GREEN}  ✅ created partitioned source topic${NC}"
elif echo "$OUT" | grep -qE 'already exists|409'; then
    echo -e "${YELLOW}  ↻ source topic already exists${NC}"
else
    echo -e "${RED}  ❌ failed:${NC}"; echo "$OUT"; exit 1
fi

# ── Step 2: ensure the control topic exists (non-partitioned) ────────────────
echo -e "${BLUE}📦 Step 2/4 — ensure control topic exists${NC}"
if OUT=$(PA topics create "$DEMO_CONTROL_TOPIC" 2>&1); then
    echo -e "${GREEN}  ✅ created control topic${NC}"
elif echo "$OUT" | grep -qE 'already exists|409'; then
    echo -e "${YELLOW}  ↻ control topic already exists${NC}"
else
    echo -e "${RED}  ❌ failed:${NC}"; echo "$OUT"; exit 1
fi

# ── Step 3: grant produce+consume on the source namespace ────────────────────
# Covers ShadowTraffic publishing to stock-quotes + the SDK reading from any
# private filtered-* topics the orchestrator creates inside the same namespace.
echo -e "${BLUE}🔐 Step 3/4 — grant produce+consume on $SOURCE_NS to $DEMO_SA_PRINCIPAL${NC}"
PA namespaces grant-permission "$SOURCE_NS" \
    --role "$DEMO_SA_PRINCIPAL" \
    --actions produce,consume 2>&1 | grep -vE '^$' || true
echo -e "${GREEN}  ✅ grant applied${NC}"

# ── Step 3b: same on the control namespace (typically same as source) ────────
if [[ "$CONTROL_NS" != "$SOURCE_NS" ]]; then
    echo -e "${BLUE}🔐 Step 3b/4 — grant produce+consume on $CONTROL_NS to $DEMO_SA_PRINCIPAL${NC}"
    PA namespaces grant-permission "$CONTROL_NS" \
        --role "$DEMO_SA_PRINCIPAL" \
        --actions produce,consume 2>&1 | grep -vE '^$' || true
    echo -e "${GREEN}  ✅ grant applied${NC}"
fi

# ── Step 4: enable auto-topic-creation on the source namespace ───────────────
# Belt-and-suspenders: the orchestrator's spawned filters publish to
# filtered-<uuid> private topics in this namespace by default. Some SN Cloud
# clusters disable broker-default auto-create; setting partitioned/1 here makes
# the override explicit and matches the deploy-connector pattern that fixed the
# Coinbase pipeline.
echo -e "${BLUE}⚙️  Step 4/4 — enable auto-topic-creation on $SOURCE_NS${NC}"
PA namespaces set-topic-auto-creation "$SOURCE_NS" \
    --type partitioned --partitions 1 2>&1 | grep -vE '^$' || true
echo -e "${GREEN}  ✅ auto-topic-creation enabled${NC}"

echo ""
echo -e "${GREEN}🎉 Bootstrap complete. Ready for shadowtraffic + multi-query-app.${NC}"
