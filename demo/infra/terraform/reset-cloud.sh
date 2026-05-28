#!/usr/bin/env bash
#
# reset-cloud.sh — wipe stale demo state on SN Cloud so the next recreate.sh
# (and the next `make demo-cloud[-parcels]`) starts from a clean baseline.
#
# WHAT IT DELETES (in $ORCHESTRATOR_TENANT/$ORCHESTRATOR_NAMESPACE unless noted):
#   - the `orchestrator` function (recreate.sh RESET=1 redeploys it)
#   - spawned `selector-filter-*` functions
#   - every uploaded `selector-filter@<ver>` package version
#   - demo data topics — source (public/default/stock-quotes + parcel-*),
#     control (filter-requests), state (orchestrator-state)
#   - SDK private output topics (`filtered-*` in public/default)
#
# WHAT IT KEEPS:
#   - the cluster, tenant, namespaces, SAs, role bindings, permission grants
#
# The orchestrator is undeployed FIRST so it stops reading filter-requests and
# writing orchestrator-state — otherwise it would auto-recreate those topics
# (and possibly re-spawn filters) mid-reset. Local producers (shadowtraffic,
# multi-query-app) must also be stopped first; the `make reset-demo-cloud`
# target depends on `make demo-down` to guarantee that.
#
# After deleting topics, the script also `terraform state rm`s the topic and
# permission-grant resources so the next `recreate.sh` Step B sees a clean
# state and CREATEs them fresh, instead of trying to UPDATE a topic that no
# longer exists (which fails with ERROR_UPDATE_TOPIC_PARTITIONS) or hitting a
# 409 "topic already exists" against state it doesn't know about.
#
# After running, redeploy:
#   1. SELECTOR_REL=v1.1.0 make demo-nars        # refresh the local NAR cache
#   2. cd demo/infra/terraform && SELECTOR_REL=v1.1.0 RESET=1 ./recreate.sh
#   3. make demo-cloud-parcels                   # (or demo-cloud)
#
# Required env (auto-loaded from demo/.env if present):
#   PULSAR_ADMIN_URL              SN Cloud admin URL (recreate.sh writes this)
#   ORCHESTRATOR_TENANT           e.g. stock-demo
#   ORCHESTRATOR_NAMESPACE        e.g. filtering
#   ORCHESTRATOR_FUNCTION         orchestrator (default)
#
# Token (in priority order):
#   PULSAR_TOKEN env              superuser JWT (e.g. snctl auth get-token <inst> --login)
#   orchestrator-function.token   uploaded the package, can delete it (and functions)
#   app.token                     last-resort fallback (may have limited admin)
#
# Usage:
#   make reset-demo-cloud
#   # or directly:  bash demo/infra/terraform/reset-cloud.sh

set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
section() { echo -e "\n${BLUE}=== $* ===${NC}"; }
ok()      { echo -e "  ${GREEN}✓${NC} $*"; }
warn()    { echo -e "  ${YELLOW}!${NC} $*"; }
fail()    { echo -e "  ${RED}✗${NC} $*"; exit 1; }

TF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "$TF_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$DEMO_DIR/.." && pwd)"

# Load demo/.env (cluster URL + orchestrator coordinates) if present.
if [[ -f "$DEMO_DIR/.env" ]]; then
    set -a; . "$DEMO_DIR/.env"; set +a
fi

PULSAR_ADMIN_URL="${PULSAR_ADMIN_URL:?required (set in env, or run recreate.sh first so it writes demo/.env)}"
TENANT="${ORCHESTRATOR_TENANT:-stock-demo}"
NS="${ORCHESTRATOR_NAMESPACE:-filtering}"
ORCH_FN="${ORCHESTRATOR_FUNCTION:-orchestrator}"
PULSAR_IMAGE="${PULSAR_IMAGE:-apachepulsar/pulsar:4.2.1}"

# Pick the strongest token available.
if [[ -n "${PULSAR_TOKEN:-}" ]]; then
    # PULSAR_TOKEN may contain extra prose from `snctl auth get-token --login`; extract the JWT.
    TOKEN="$(printf '%s' "$PULSAR_TOKEN" | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' | tail -1)"
    [[ -n "$TOKEN" ]] || fail "PULSAR_TOKEN is set but no JWT found in it."
    TOKEN_SRC="env PULSAR_TOKEN (superuser)"
elif [[ -f "$TF_DIR/orchestrator-function.token" ]]; then
    TOKEN="$(cat "$TF_DIR/orchestrator-function.token")"
    TOKEN_SRC="orchestrator-function.token"
elif [[ -f "$TF_DIR/app.token" ]]; then
    TOKEN="$(cat "$TF_DIR/app.token")"
    TOKEN_SRC="app.token (limited admin)"
else
    fail "No token — set PULSAR_TOKEN env or run recreate.sh first to generate $TF_DIR/*.token"
fi

# pulsar-admin via docker, mirroring recreate.sh Phase 7 (no host pulsar-admin needed).
PA() {
    docker run --rm "$PULSAR_IMAGE" pulsar-admin \
        --admin-url "$PULSAR_ADMIN_URL" \
        --auth-plugin org.apache.pulsar.client.impl.auth.AuthenticationToken \
        --auth-params "token:$TOKEN" \
        "$@"
}

echo -e "${YELLOW}⚠  Reset will delete spawned functions, package versions, and demo topics on:${NC}"
echo "    Admin URL:    $PULSAR_ADMIN_URL"
echo "    Orchestrator: $TENANT/$NS/$ORCH_FN  (KEPT; only the spawned filters are removed)"
echo "    Token:        $TOKEN_SRC"
echo "    pulsar-admin: $PULSAR_IMAGE"
echo

# pulsar-admin emits lists in slightly different shapes across versions. Normalize
# to whitespace-separated names: strip [ ] " , and explode on whitespace.
_normalize_list() {
    printf '%s' "$1" | tr -d '[]",' | tr ' ' '\n' | sed '/^$/d'
}

# ── Step 1/5: undeploy the orchestrator ───────────────────────────────────────
# Done first so the orchestrator stops reading filter-requests and writing
# orchestrator-state — otherwise it would auto-recreate them mid-reset.
section "Step 1/5 — Undeploy the orchestrator function ($ORCH_FN)"
if PA functions status --tenant "$TENANT" --namespace "$NS" --name "$ORCH_FN" >/dev/null 2>&1; then
    if PA functions delete --tenant "$TENANT" --namespace "$NS" --name "$ORCH_FN" >/dev/null 2>&1; then
        ok "undeployed orchestrator: $TENANT/$NS/$ORCH_FN"
    else
        warn "could not undeploy $ORCH_FN (continuing — recreate.sh RESET=1 will retry)"
    fi
else
    ok "orchestrator not running ($TENANT/$NS/$ORCH_FN) — nothing to undeploy"
fi

# ── Step 2/5: spawned filter functions ────────────────────────────────────────
section "Step 2/5 — Delete spawned selector-filter-* functions in $TENANT/$NS"
FN_OUT=$(PA functions list --tenant "$TENANT" --namespace "$NS" 2>&1 || true)
FN_NAMES=$(_normalize_list "$FN_OUT")
if [[ -z "$FN_NAMES" ]]; then
    warn "no functions listed (or list call failed): $FN_OUT"
else
    for fn in $FN_NAMES; do
        if [[ "$fn" == "$ORCH_FN" ]]; then
            ok "kept orchestrator: $fn"
            continue
        fi
        if [[ "$fn" =~ ^selector-filter- ]]; then
            if PA functions delete --tenant "$TENANT" --namespace "$NS" --name "$fn" >/dev/null 2>&1; then
                ok "deleted function: $fn"
            else
                warn "could not delete function: $fn (continuing)"
            fi
        fi
    done
fi

# ── Step 3/5: uploaded selector-filter package versions ──────────────────────
section "Step 3/5 — Delete selector-filter package versions"
PKG_PATH="function://$TENANT/$NS/selector-filter"
VERS_OUT=$(PA packages list-package-versions "$PKG_PATH" 2>&1 || true)
VERS=$(_normalize_list "$VERS_OUT")
if [[ -z "$VERS" ]]; then
    warn "no package versions listed for $PKG_PATH (or none exist)"
else
    for v in $VERS; do
        pkg="${PKG_PATH}@${v}"
        if PA packages delete "$pkg" >/dev/null 2>&1; then
            ok "deleted package: $pkg"
        else
            warn "could not delete package: $pkg (continuing)"
        fi
    done
fi

# ── Step 4/5: demo topics ─────────────────────────────────────────────────────
section "Step 4/5 — Delete demo topics (recreated by bootstrap on next run)"

# Source topics — partitioned per bootstrap.sh; -f tolerates active producers/consumers.
for t in \
        "persistent://public/default/stock-quotes" \
        "persistent://public/default/parcel-shipments" \
        "persistent://public/default/parcel-scans" \
        "persistent://public/default/parcel-exceptions" ; do
    if PA topics delete-partitioned-topic "$t" -f >/dev/null 2>&1; then
        ok "deleted partitioned topic: $t"
    else
        warn "skipped (not found or no permission): $t"
    fi
done

# Control + state — non-partitioned.
for t in \
        "persistent://$TENANT/$NS/filter-requests" \
        "persistent://$TENANT/$NS/orchestrator-state" ; do
    if PA topics delete "$t" -f >/dev/null 2>&1; then
        ok "deleted topic: $t"
    else
        warn "skipped (not found or no permission): $t"
    fi
done

# SDK private topics (filtered-<uuid>) in public/default — accumulate per query registration.
PRIV_OUT=$(PA topics list public/default 2>/dev/null || true)
PRIV_TOPICS=$(printf '%s' "$PRIV_OUT" | grep -E '/filtered-' || true)
if [[ -n "$PRIV_TOPICS" ]]; then
    for t in $PRIV_TOPICS; do
        if PA topics delete "$t" -f >/dev/null 2>&1; then
            ok "deleted private topic: $t"
        else
            warn "skipped private topic: $t"
        fi
    done
else
    ok "no filtered-* private topics to delete"
fi

# ── Step 5/5: clear terraform state for the just-deleted resources ───────────
# Without this, the next `recreate.sh` Step B sees stale state and either:
#   - tries to UPDATE a topic that no longer exists → ERROR_UPDATE_TOPIC_PARTITIONS
#   - half-applies, leaves orphans on the cluster, then on retry CREATEs into
#     existing topics → 409 "topic already exists"
# Same list `recreate.sh` Phase 4 clears when it detects no cluster. Tenant +
# namespace are NOT removed (they still exist on the cluster).
section "Step 5/5 — Clear Terraform state for deleted topic + grant resources"
if ! command -v terraform >/dev/null 2>&1; then
    warn "terraform not found on PATH — skipping state cleanup. The next recreate.sh may fail with stale-state errors; clear manually with: cd $TF_DIR && terraform state rm <resource>"
elif [[ ! -f "$TF_DIR/terraform.tfstate" && ! -f "$TF_DIR/.terraform/terraform.tfstate" ]]; then
    ok "no terraform state file present — nothing to clean"
else
    TF_RESOURCES=(
        pulsar_topic.stock_quotes
        pulsar_topic.parcel_shipments
        pulsar_topic.parcel_scans
        pulsar_topic.parcel_exceptions
        pulsar_topic.filter_requests
        pulsar_topic.orchestrator_state
        pulsar_permission_grant.app_public_default
        pulsar_permission_grant.app_filter_requests
        pulsar_permission_grant.orchestrator_filter_deploy
        pulsar_permission_grant.orchestrator_consume_control
        pulsar_permission_grant.orchestrator_state_consume
        pulsar_permission_grant.orchestrator_state_produce
        pulsar_permission_grant.orchestrator_consume_source
        pulsar_permission_grant.orchestrator_produce_output
    )
    # Intersect with what's actually in state so we don't `state rm` missing
    # resources (which exits non-zero and would abort under `set -e`).
    IN_STATE=$(cd "$TF_DIR" && terraform state list 2>/dev/null || true)
    for r in "${TF_RESOURCES[@]}"; do
        if printf '%s\n' "$IN_STATE" | grep -qxF "$r"; then
            if (cd "$TF_DIR" && terraform state rm "$r" >/dev/null 2>&1); then
                ok "removed from state: $r"
            else
                warn "could not remove from state: $r (continuing)"
            fi
        else
            ok "not in state, skipping: $r"
        fi
    done
fi

echo
echo -e "${GREEN}🎉 Reset complete.${NC}"
echo "Next steps — copy/paste each block (paths are absolute, no matter your CWD):"
echo
echo "  # 1. refresh local NAR cache"
echo "  cd $REPO_ROOT && SELECTOR_REL=v1.1.0 make demo-nars"
echo
echo "  # 2. re-upload selector-filter package + redeploy orchestrator"
echo "  cd $TF_DIR && SELECTOR_REL=v1.1.0 RESET=1 ./recreate.sh"
echo
echo "  # 3. bring the demo back up (or 'make demo-cloud' for the stock-quotes scenario)"
echo "  cd $REPO_ROOT && make demo-cloud-parcels"
