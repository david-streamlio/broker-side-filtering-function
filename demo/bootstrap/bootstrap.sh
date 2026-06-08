#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
#   DEMO_CONTROL_TOPIC    — full FQN, e.g. persistent://public/default/filter-requests
#   DEMO_SOURCE_PARTITIONS — number of partitions per source topic (default 1)
#
# Source topics — one of:
#   DEMO_SOURCE_TOPICS    — comma-separated list of FQNs (multi-topic mode), or
#   DEMO_SOURCE_TOPIC     — single FQN, e.g. persistent://public/default/stock-quotes
# When DEMO_SOURCE_TOPICS is set it takes precedence; DEMO_SOURCE_TOPIC is then
# ignored. Namespace-level ops (kafkaEntryFormat, grants, auto-topic-creation)
# are deduped — running the parcel demo's three topics in public/default
# applies each namespace op exactly once.
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
DEMO_CONTROL_TOPIC="${DEMO_CONTROL_TOPIC:?required}"
DEMO_SA_PRINCIPAL="${DEMO_SA_PRINCIPAL:-}"
DEMO_SOURCE_PARTITIONS="${DEMO_SOURCE_PARTITIONS:-1}"

# Resolve the source-topic list: DEMO_SOURCE_TOPICS wins if set, otherwise
# fall back to the single-topic DEMO_SOURCE_TOPIC. Exactly one must be present.
if [[ -n "${DEMO_SOURCE_TOPICS:-}" ]]; then
    IFS=',' read -r -a _RAW_SOURCE_TOPICS <<< "$DEMO_SOURCE_TOPICS"
elif [[ -n "${DEMO_SOURCE_TOPIC:-}" ]]; then
    _RAW_SOURCE_TOPICS=("$DEMO_SOURCE_TOPIC")
else
    echo "Set either DEMO_SOURCE_TOPICS (comma-separated) or DEMO_SOURCE_TOPIC (single FQN)" >&2
    exit 1
fi

# Trim whitespace around each entry; reject empty entries (stray commas).
SOURCE_TOPICS=()
for t in "${_RAW_SOURCE_TOPICS[@]}"; do
    t="${t#"${t%%[![:space:]]*}"}"   # ltrim
    t="${t%"${t##*[![:space:]]}"}"   # rtrim
    if [[ -n "$t" ]]; then
        SOURCE_TOPICS+=("$t")
    fi
done
if [[ ${#SOURCE_TOPICS[@]} -eq 0 ]]; then
    echo "DEMO_SOURCE_TOPICS resolved to an empty list — at least one topic FQN required" >&2
    exit 1
fi

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

# Build the deduped list of unique source namespaces. Namespace-level admin
# ops (kafkaEntryFormat, grants, auto-topic-creation) only need to run once
# per unique namespace — the parcel demo's three topics all share public/default.
declare -A _SEEN_NS
SOURCE_NAMESPACES=()
for t in "${SOURCE_TOPICS[@]}"; do
    ns=$(parse_namespace "$t")
    if [[ -z "${_SEEN_NS[$ns]:-}" ]]; then
        _SEEN_NS[$ns]=1
        SOURCE_NAMESPACES+=("$ns")
    fi
done
CONTROL_NS=$(parse_namespace "$DEMO_CONTROL_TOPIC")

echo -e "${BLUE}🚀 Multi-query demo bootstrap${NC}"
echo "  Admin URL:       $PULSAR_ADMIN_URL"
echo "  Auth mode:       $AUTH_MODE"
echo "  Source topics ($DEMO_SOURCE_PARTITIONS partition(s) each):"
for t in "${SOURCE_TOPICS[@]}"; do
    echo "    - $t"
done
echo "  Source namespaces (deduped): ${SOURCE_NAMESPACES[*]}"
echo "  Control topic:   $DEMO_CONTROL_TOPIC  (ns=$CONTROL_NS)"
[[ "$AUTH_MODE" == "token" ]] && echo "  Demo SA:         $DEMO_SA_PRINCIPAL"
echo ""

# ── Step 1: set kafkaEntryFormat=pulsar_non_batched on each source namespace ─
# MUST run before the source topics are created. KoP defaults to a batched
# entry format; the orchestrator's spawned filter functions are regular
# Pulsar consumers that can't decode KoP-batched entries (broker logs
# `SingleMessageMetadata: Some required fields are missing` and discards
# the ledger entry). This property is recognized only at the NAMESPACE
# level — it is NOT a valid broker-config value — so it must be set here,
# and BEFORE topic creation so the topic inherits it at create time
# (changing it after a topic exists doesn't retroactively re-encode, and
# KoP may have cached the topic's effective format).
for ns in "${SOURCE_NAMESPACES[@]}"; do
    echo -e "${BLUE}🔧 Step 1 — set kafkaEntryFormat=pulsar_non_batched on $ns${NC}"
    PA namespaces set-property "$ns" \
        --key kafkaEntryFormat \
        --value pulsar_non_batched 2>&1 | grep -vE '^$' || true
    # In cloud-mode the demo SA is not tenant-admin and 401s here — that's fine,
    # recreate.sh already set this property with the superuser token before the
    # compose stack starts. Non-fatal either way.
    echo -e "${GREEN}  ✅ namespace property set (or already set out-of-band)${NC}"
done

# ── Step 1b: ensure each source topic exists as a partitioned topic ──────────
# Created AFTER the namespace property above so each one inherits
# pulsar_non_batched.
for t in "${SOURCE_TOPICS[@]}"; do
    echo -e "${BLUE}📦 Step 1b — ensure source topic exists: $t${NC}"
    # Check existence first via a metadata read (the demo SA can do this with its
    # produce/consume grant). In cloud-mode recreate.sh's Step B may already have
    # created the topic with the superuser token; the demo SA then 403s on
    # create-partitioned-topic ("Unauthorized to validateTopicPolicyOperation …
    # PARTITION") because it lacks partition-policy write — that is NOT a failure,
    # the topic is already there. Only hard-fail if it's genuinely absent and
    # creatable but the create errored for another reason.
    if PA topics get-partitioned-topic-metadata "$t" >/dev/null 2>&1; then
        echo -e "${YELLOW}  ↻ source topic already exists (managed out-of-band)${NC}"
    elif OUT=$(PA topics create-partitioned-topic "$t" -p "$DEMO_SOURCE_PARTITIONS" 2>&1); then
        echo -e "${GREEN}  ✅ created partitioned source topic${NC}"
    elif echo "$OUT" | grep -qE 'already exists|409|Unauthorized to validateTopicPolicyOperation|Don.t have permission to administrate'; then
        echo -e "${YELLOW}  ↻ source topic exists / managed out-of-band — skipping create${NC}"
    else
        echo -e "${RED}  ❌ failed:${NC}"; echo "$OUT"; exit 1
    fi
done

# ── Step 1c: set kafkaEntryFormat=pulsar_non_batched on PARENT partitioned topic
# KoP source code (PartitionLogManager.java) reads kafkaEntryFormat from the
# PARENT topic's partitioned-topic metadata for partitioned topics, NOT from the
# namespace property set in Step 1 (which is read only for non-partitioned topics).
# Without this, KoP stores Kafka records in batch format (x-pulsar-num-batch-message)
# that the Pulsar function consumer cannot decode, logging "Discarding corrupted
# message" for every entry and driving availablePermits negative — permanently
# deadlocking message dispatch to the selector-filter function.
for t in "${SOURCE_TOPICS[@]}"; do
    echo -e "${BLUE}🔧 Step 1c — set kafkaEntryFormat=pulsar_non_batched on parent topic $t${NC}"
    PA topics set-property "$t" \
        --key kafkaEntryFormat \
        --value pulsar_non_batched 2>&1 | grep -vE '^$' || true
    echo -e "${GREEN}  ✅ parent topic property set (kafkaEntryFormat=pulsar_non_batched)${NC}"
done

# ── Step 2: ensure the control topic exists (non-partitioned) ────────────────
echo -e "${BLUE}📦 Step 2 — ensure control topic exists${NC}"
# Same idempotency as Step 1b: recreate.sh's Step B pre-creates the control
# topic in cloud mode; the demo SA can read its stats but can't admin-create
# it, so tolerate the out-of-band-managed case.
if PA topics stats "$DEMO_CONTROL_TOPIC" >/dev/null 2>&1; then
    echo -e "${YELLOW}  ↻ control topic already exists (managed out-of-band)${NC}"
elif OUT=$(PA topics create "$DEMO_CONTROL_TOPIC" 2>&1); then
    echo -e "${GREEN}  ✅ created control topic${NC}"
elif echo "$OUT" | grep -qE 'already exists|409|Unauthorized to validateTopicPolicyOperation|Don.t have permission to administrate'; then
    echo -e "${YELLOW}  ↻ control topic exists / managed out-of-band — skipping create${NC}"
else
    echo -e "${RED}  ❌ failed:${NC}"; echo "$OUT"; exit 1
fi

# ── Step 3: grant produce+consume on every source namespace + control ns ─────
# Covers ShadowTraffic publishing to the source topics + the SDK reading from
# any private filtered-* topics the orchestrator creates inside the same
# namespace. Skipped in no-auth mode — anonymous already has all access on
# standalone. The control-namespace grant only fires when it differs from
# the source namespaces (it usually doesn't — they all default to public/default).
if [[ "$AUTH_MODE" == "token" && -n "$DEMO_SA_PRINCIPAL" ]]; then
    for ns in "${SOURCE_NAMESPACES[@]}"; do
        echo -e "${BLUE}🔐 Step 3 — grant produce+consume on $ns to $DEMO_SA_PRINCIPAL${NC}"
        PA namespaces grant-permission "$ns" \
            --role "$DEMO_SA_PRINCIPAL" \
            --actions produce,consume 2>&1 | grep -vE '^$' || true
        echo -e "${GREEN}  ✅ grant applied${NC}"
    done

    # Control namespace: grant only if it isn't already covered above.
    CONTROL_NS_COVERED=""
    for ns in "${SOURCE_NAMESPACES[@]}"; do
        if [[ "$ns" == "$CONTROL_NS" ]]; then CONTROL_NS_COVERED=1; break; fi
    done
    if [[ -z "$CONTROL_NS_COVERED" ]]; then
        echo -e "${BLUE}🔐 Step 3b — grant produce+consume on $CONTROL_NS to $DEMO_SA_PRINCIPAL${NC}"
        PA namespaces grant-permission "$CONTROL_NS" \
            --role "$DEMO_SA_PRINCIPAL" \
            --actions produce,consume 2>&1 | grep -vE '^$' || true
        echo -e "${GREEN}  ✅ grant applied${NC}"
    fi
else
    echo -e "${YELLOW}⏭  Step 3 — skipping permission grants (no-auth mode)${NC}"
fi

# ── Step 4: enable auto-topic-creation on each source namespace ──────────────
# Belt-and-suspenders: the orchestrator's spawned filters publish to
# filtered-<uuid> private topics in this namespace by default. Some SN Cloud
# clusters disable broker-default auto-create; setting partitioned/1 here makes
# the override explicit and matches the deploy-connector pattern that fixed the
# Coinbase pipeline.
for ns in "${SOURCE_NAMESPACES[@]}"; do
    echo -e "${BLUE}⚙️  Step 4 — enable auto-topic-creation on $ns${NC}"
    PA namespaces set-auto-topic-creation "$ns" \
        --enable --type partitioned --num-partitions 1 2>&1 | grep -vE '^$' || true
    echo -e "${GREEN}  ✅ auto-topic-creation enabled${NC}"
done

echo ""
echo -e "${GREEN}🎉 Bootstrap complete. Ready for shadowtraffic + multi-query-app.${NC}"
