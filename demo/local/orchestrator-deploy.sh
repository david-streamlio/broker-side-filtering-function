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

# Private repos need an auth token. The compose file forwards GITHUB_TOKEN
# from the host shell:
#   export GITHUB_TOKEN=$(gh auth token)
#   docker compose -f docker-compose.local.yml up --build
# When unset, the direct download URL is used (works for public repos).
download_asset() {
    local asset_name="$1" dest="$2"
    if [[ -n "${GITHUB_TOKEN:-}" ]]; then
        # Look up the asset's API URL by name (the API URL accepts auth headers
        # — the browser download URL doesn't, because GitHub's CDN strips them
        # on redirect). Then fetch with Accept: application/octet-stream so the
        # response is the raw NAR, not the JSON metadata.
        local api_url
        api_url=$(curl -sfL -H "Authorization: token ${GITHUB_TOKEN}" \
            "https://api.github.com/repos/${SELECTOR_REPO}/releases/tags/${SELECTOR_REL}" \
            | python3 -c "import sys,json; r=json.load(sys.stdin); print(next(a['url'] for a in r['assets'] if a['name']=='$asset_name'))")
        [[ -z "$api_url" ]] && { echo "  ❌ asset not found: $asset_name"; return 1; }
        curl -sfL \
            -H "Authorization: token ${GITHUB_TOKEN}" \
            -H "Accept: application/octet-stream" \
            -o "$dest" "$api_url"
    else
        curl -sfL -o "$dest" \
            "https://github.com/${SELECTOR_REPO}/releases/download/${SELECTOR_REL}/${asset_name}"
    fi
}

if [[ ! -f "$ORCH_NAR" ]]; then
    download_asset "selector-filter-orchestrator-${SF_VER}.nar" "$ORCH_NAR" \
        || { echo -e "${RED}❌ orchestrator NAR download failed${NC}"; exit 1; }
fi
if [[ ! -f "$FILT_NAR" ]]; then
    download_asset "selector-filter-function-${SF_VER}.nar" "$FILT_NAR" \
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
