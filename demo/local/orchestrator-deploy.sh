#!/usr/bin/env bash
#
# Local-mode orchestrator + selector-filter deploy.
#
# Equivalent of recreate.sh Phase 7 for the standalone Pulsar broker:
#   1. download selector-filter + orchestrator NARs from the GitHub release
#   2. upload selector-filter as a Pulsar package
#   3. deploy the orchestrator function (if not already deployed)
#
# No auth — the standalone broker accepts anonymous access. Idempotent.

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:?required}"
SELECTOR_REPO="${SELECTOR_REPO:-david-streamlio/broker-side-filtering-function}"
SELECTOR_REL="${SELECTOR_REL:-v1.0.0-rc3}"
SF_VER="${SELECTOR_REL#v}"

echo -e "${BLUE}🚀 Local orchestrator deploy${NC}"
echo "  Admin URL:    $PULSAR_ADMIN_URL"
echo "  Release:      $SELECTOR_REPO @ $SELECTOR_REL"
echo ""

# ── Step 1: Download NARs ────────────────────────────────────────────────────
echo -e "${BLUE}📦 Step 1/3 — Download orchestrator + selector-filter NARs${NC}"
mkdir -p /tmp/selector-filter-release
ORCH_NAR="/tmp/selector-filter-release/selector-filter-orchestrator-${SF_VER}.nar"
FILT_NAR="/tmp/selector-filter-release/selector-filter-function-${SF_VER}.nar"

if [[ ! -f "$ORCH_NAR" ]]; then
    curl -sfL -o "$ORCH_NAR" \
        "https://github.com/${SELECTOR_REPO}/releases/download/${SELECTOR_REL}/selector-filter-orchestrator-${SF_VER}.nar" \
        || { echo -e "${RED}❌ orchestrator NAR download failed${NC}"; exit 1; }
fi
if [[ ! -f "$FILT_NAR" ]]; then
    curl -sfL -o "$FILT_NAR" \
        "https://github.com/${SELECTOR_REPO}/releases/download/${SELECTOR_REL}/selector-filter-function-${SF_VER}.nar" \
        || { echo -e "${RED}❌ selector-filter NAR download failed${NC}"; exit 1; }
fi
echo -e "${GREEN}  ✅ NARs ready${NC}"

# ── Step 2: Upload selector-filter as a Pulsar package ───────────────────────
# The orchestrator's userConfig references function://public/default/selector-filter@<ver>;
# Pulsar pulls this NAR per spawn instead of cloning a deploy each time.
PKG_URL="function://public/default/selector-filter@${SF_VER}"
echo -e "${BLUE}📤 Step 2/3 — Upload selector-filter to $PKG_URL${NC}"
if pulsar-admin --admin-url "$PULSAR_ADMIN_URL" \
        packages upload "$PKG_URL" \
        --path "$FILT_NAR" \
        --description "selector-filter ${SELECTOR_REL} (local-mode)" 2>&1 | tee /tmp/pkg-upload.log; then
    echo -e "${GREEN}  ✅ uploaded${NC}"
elif grep -qE 'already exists|409' /tmp/pkg-upload.log; then
    echo -e "${YELLOW}  ↻ package version already uploaded${NC}"
else
    echo -e "${RED}  ❌ upload failed${NC}"
    cat /tmp/pkg-upload.log
    exit 1
fi

# ── Step 3: Deploy orchestrator function ─────────────────────────────────────
echo -e "${BLUE}⚙️  Step 3/3 — Deploy orchestrator function${NC}"

FUNCS_LIST=$(pulsar-admin --admin-url "$PULSAR_ADMIN_URL" \
    functions list --tenant public --namespace default 2>&1 || true)

if echo "$FUNCS_LIST" | grep -q "^orchestrator$"; then
    echo -e "${YELLOW}  ↻ orchestrator already deployed — skipping${NC}"
else
    # Rewrite the @VERSION marker and the PULSAR_ADMIN_URL placeholder in the
    # function config template. Local mode has no admin token (anonymous auth).
    TMP_CFG=$(mktemp -t orchestrator-XXXXXX.yaml)
    sed -e "s|@VERSION|@${SF_VER}|g" \
        -e "s|\${PULSAR_ADMIN_URL}|${PULSAR_ADMIN_URL}|g" \
        /etc/orchestrator-function-config.yaml > "$TMP_CFG"

    pulsar-admin --admin-url "$PULSAR_ADMIN_URL" \
        functions create \
        --function-config-file "$TMP_CFG" \
        --jar "$ORCH_NAR" \
        || { echo -e "${RED}  ❌ functions create failed${NC}"; exit 1; }

    rm -f "$TMP_CFG"
    echo -e "${GREEN}  ✅ orchestrator deployed${NC}"
fi

echo ""
echo -e "${GREEN}🎉 Local orchestrator ready. Bootstrap container starts next.${NC}"
