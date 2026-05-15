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
#   PULSAR_ADMIN_URL      — https://pc-<id>.<region>.snio.cloud (or http://pulsar:8080 for local)
#   DEMO_SOURCE_TOPIC     — full FQN, e.g. persistent://public/default/stock-quotes
#   DEMO_CONTROL_TOPIC    — full FQN, e.g. persistent://public/default/filter-requests
#   DEMO_SOURCE_PARTITIONS — number of partitions for the source topic (default 1)
#
# Optional env (skipped in local-mode):
#   PULSAR_TOKEN_FILE     — path inside the container to the SA's JWT. When unset
#                           or empty, the script runs in no-auth mode (local
#                           Pulsar standalone with anonymous access).
#   DEMO_SA_PRINCIPAL     — the SASL/JWT principal used as grant target. Required
#                           in cloud mode, ignored in local mode.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:?required}"
PULSAR_TOKEN_FILE="${PULSAR_TOKEN_FILE:-}"
DEMO_SOURCE_TOPIC="${DEMO_SOURCE_TOPIC:?required}"
DEMO_CONTROL_TOPIC="${DEMO_CONTROL_TOPIC:?required}"
DEMO_SA_PRINCIPAL="${DEMO_SA_PRINCIPAL:-}"
DEMO_SOURCE_PARTITIONS="${DEMO_SOURCE_PARTITIONS:-1}"

# Auth mode auto-detect: if a token file path was supplied and the file exists,
# run authenticated (cloud / production). Otherwise assume the broker accepts
# anonymous (local Pulsar standalone or any cluster with auth disabled).
AUTH_MODE="none"
if [[ -n "$PULSAR_TOKEN_FILE" && -f "$PULSAR_TOKEN_FILE" ]]; then
    AUTH_MODE="token"
    TOKEN=$(cat "$PULSAR_TOKEN_FILE")
fi

# Shared client wrapper. In token mode, --auth-plugin must be AuthenticationToken
# explicitly to bypass pulsar-admin's default OAuth2 client-credentials path,
# which fails 401 even with a valid JWT.
PA() {
    if [[ "$AUTH_MODE" == "token" ]]; then
        pulsar-admin \
            --admin-url "$PULSAR_ADMIN_URL" \
            --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken \
            --auth-params "token:$TOKEN" \
            "$@"
    else
        pulsar-admin --admin-url "$PULSAR_ADMIN_URL" "$@"
    fi
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
echo "  Auth mode:       $AUTH_MODE"
echo "  Source topic:    $DEMO_SOURCE_TOPIC  (ns=$SOURCE_NS, partitions=$DEMO_SOURCE_PARTITIONS)"
echo "  Control topic:   $DEMO_CONTROL_TOPIC  (ns=$CONTROL_NS)"
[[ "$AUTH_MODE" == "token" ]] && echo "  Demo SA:         $DEMO_SA_PRINCIPAL"
echo ""

# ── Step 1: set kafkaEntryFormat=pulsar_non_batched on the source namespace ──
# MUST run before the source topic is created. KoP defaults to a batched
# entry format; the orchestrator's spawned filter functions are regular
# Pulsar consumers that can't decode KoP-batched entries (broker logs
# `SingleMessageMetadata: Some required fields are missing` and discards
# the ledger entry). This property is recognized only at the NAMESPACE
# level — it is NOT a valid broker-config value — so it must be set here,
# and BEFORE topic creation so the topic inherits it at create time
# (changing it after a topic exists doesn't retroactively re-encode, and
# KoP may have cached the topic's effective format).
echo -e "${BLUE}🔧 Step 1/4 — set kafkaEntryFormat=pulsar_non_batched on $SOURCE_NS${NC}"
PA namespaces set-property "$SOURCE_NS" \
    --key kafkaEntryFormat \
    --value pulsar_non_batched 2>&1 | grep -vE '^$' || true
echo -e "${GREEN}  ✅ namespace property set${NC}"

# ── Step 1b: ensure the source topic exists as a partitioned topic ───────────
# Created AFTER the namespace property above so it inherits pulsar_non_batched.
echo -e "${BLUE}📦 Step 1b — ensure source topic exists${NC}"
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
# Skipped in no-auth mode — anonymous already has all access on standalone.
if [[ "$AUTH_MODE" == "token" && -n "$DEMO_SA_PRINCIPAL" ]]; then
    echo -e "${BLUE}🔐 Step 3/4 — grant produce+consume on $SOURCE_NS to $DEMO_SA_PRINCIPAL${NC}"
    PA namespaces grant-permission "$SOURCE_NS" \
        --role "$DEMO_SA_PRINCIPAL" \
        --actions produce,consume 2>&1 | grep -vE '^$' || true
    echo -e "${GREEN}  ✅ grant applied${NC}"

    # ── Step 3b: same on the control namespace (typically same as source) ────
    if [[ "$CONTROL_NS" != "$SOURCE_NS" ]]; then
        echo -e "${BLUE}🔐 Step 3b/4 — grant produce+consume on $CONTROL_NS to $DEMO_SA_PRINCIPAL${NC}"
        PA namespaces grant-permission "$CONTROL_NS" \
            --role "$DEMO_SA_PRINCIPAL" \
            --actions produce,consume 2>&1 | grep -vE '^$' || true
        echo -e "${GREEN}  ✅ grant applied${NC}"
    fi
else
    echo -e "${YELLOW}⏭  Step 3/4 — skipping permission grants (no-auth mode)${NC}"
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
