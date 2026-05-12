#!/usr/bin/env bash
# Pulsar Helm smoke test: brings up a kind cluster, installs Pulsar with KoP enabled via
# the greenfield example, then drives a Python SDK round-trip against the orchestrator.
#
# Idempotent in the sense that re-running tears down the kind cluster first. Intended
# for local validation + CI; not for production use.
#
# Requires: kind, helm, terraform, kubectl, pulsarctl, python3.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HELM_TF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$HELM_TF_DIR/../../.." && pwd)"
GREENFIELD_DIR="$HELM_TF_DIR/examples/greenfield"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-selector-filter-smoke}"

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "ERROR: $1 not on PATH." >&2
    exit 1
  }
}
for cmd in kind helm terraform kubectl pulsarctl python3; do require "$cmd"; done

ORCH_NAR=$(find "$REPO_ROOT/functions/orchestrator/target" -maxdepth 1 -name 'selector-filter-orchestrator-*.nar' | head -n 1)
FILTER_NAR=$(find "$REPO_ROOT/functions/selector-filter/target" -maxdepth 1 -name 'selector-filter-function-*.nar' | head -n 1)
if [[ -z "$ORCH_NAR" || -z "$FILTER_NAR" ]]; then
  echo "ERROR: NARs not built. Run: mvn -pl functions/selector-filter,functions/orchestrator package -DskipTests" >&2
  exit 1
fi

echo "===> Recreating kind cluster $KIND_CLUSTER_NAME"
kind delete cluster --name "$KIND_CLUSTER_NAME" >/dev/null 2>&1 || true
kind create cluster --name "$KIND_CLUSTER_NAME" --wait 60s
trap 'echo "===> Tearing down kind cluster"; kind delete cluster --name "$KIND_CLUSTER_NAME" || true' EXIT

echo "===> terraform init + apply (greenfield example)"
cd "$GREENFIELD_DIR"
terraform init -upgrade
terraform apply -auto-approve \
  -var "orchestrator_nar_path=$ORCH_NAR" \
  -var "filter_nar_path=$FILTER_NAR"

echo "===> Port-forwarding broker 6650 + 8080"
kubectl -n pulsar port-forward svc/pulsar-broker 6650:6650 8080:8080 >/tmp/pf.log 2>&1 &
PF_PID=$!
trap 'echo "===> killing port-forward"; kill $PF_PID 2>/dev/null || true; echo "===> Tearing down kind cluster"; kind delete cluster --name "$KIND_CLUSTER_NAME" || true' EXIT
sleep 5

CONTROL_TOPIC=$(terraform output -raw control_topic_fqdn)
echo "===> Driving Python SDK round-trip on $CONTROL_TOPIC"

PY_SDK_DIR="$REPO_ROOT/sdk/python"
if [[ ! -x "$PY_SDK_DIR/.venv/bin/python" ]]; then
  echo "ERROR: Python venv missing at $PY_SDK_DIR/.venv. Run setup first." >&2
  exit 1
fi

export SMOKE_SERVICE_URL="pulsar://localhost:6650"
export SMOKE_CONTROL_TOPIC="$CONTROL_TOPIC"
"$PY_SDK_DIR/.venv/bin/python" - <<'PYEOF'
import os
import pulsar
from selector_filter import Session

client = pulsar.Client(os.environ["SMOKE_SERVICE_URL"])
session = (
    Session.builder()
    .pulsar_client(client)
    .control_topic(os.environ["SMOKE_CONTROL_TOPIC"])
    .disable_heartbeat()
    .build()
)
pq = session.register_private_query(
    source_topics=["persistent://pulsar-tenant/filters/__smoke__"],
    user_selector=None,
)
print(f"Smoke: registered queryId={pq.query_id}")
session.retire_private_query(pq)
print("Smoke: retract OK")
session.close()
client.close()
print("Smoke: PASS")
PYEOF

echo "===> Smoke test PASSED"
