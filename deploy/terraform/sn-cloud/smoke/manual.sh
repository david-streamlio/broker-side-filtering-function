#!/usr/bin/env bash
# SN Cloud smoke test. Reads the most recent `terraform apply` outputs from
# deploy/terraform/sn-cloud/, then drives a Python SDK round-trip against the deployed
# orchestrator.
#
# Gated on $TF_VAR_streamnative_admin_token being set — refuses to run otherwise so a
# stale env doesn't accidentally hit the wrong cluster.
#
# Requires: terraform, python3, the Python SDK installed (cd sdk/python && pip
# install -e . from a venv).
set -euo pipefail

: "${TF_VAR_streamnative_admin_token:?Set TF_VAR_streamnative_admin_token to a valid SN Cloud OAuth2 token before running this smoke test.}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$TF_DIR/../../.." && pwd)"

cd "$TF_DIR"

if [[ ! -f terraform.tfstate && ! -f .terraform/terraform.tfstate ]]; then
  echo "No Terraform state in $TF_DIR. Run 'terraform apply' first." >&2
  exit 1
fi

SERVICE_URL=$(terraform output -raw service_url)
CONTROL_TOPIC=$(terraform output -raw control_topic_fqdn)
# Extract tenant/namespace from the control topic FQDN: persistent://tenant/ns/topic
TENANT_NS=${CONTROL_TOPIC#persistent://}    # strip prefix
TENANT=${TENANT_NS%%/*}
REST=${TENANT_NS#*/}
NAMESPACE=${REST%%/*}

echo "SN Cloud smoke test"
echo "  service_url   = $SERVICE_URL"
echo "  control_topic = $CONTROL_TOPIC"
echo "  tenant/ns     = $TENANT/$NAMESPACE"

PY_SDK_DIR="$REPO_ROOT/sdk/python"
if [[ ! -d "$PY_SDK_DIR" ]]; then
  echo "Python SDK not found at $PY_SDK_DIR." >&2
  exit 1
fi

cd "$PY_SDK_DIR"
if [[ ! -x .venv/bin/python ]]; then
  echo "Python venv missing. Run: cd sdk/python && python3 -m venv .venv && .venv/bin/pip install -e .[dev]" >&2
  exit 1
fi

export SMOKE_SERVICE_URL="$SERVICE_URL"
export SMOKE_CONTROL_TOPIC="$CONTROL_TOPIC"
export SMOKE_TENANT="$TENANT"
export SMOKE_NAMESPACE="$NAMESPACE"

# Quoted heredoc — bash does NOT expand inside Python. We pass values via env.
.venv/bin/python - <<'PYEOF'
import os
import pulsar
from selector_filter import Session

service_url   = os.environ["SMOKE_SERVICE_URL"]
control_topic = os.environ["SMOKE_CONTROL_TOPIC"]
tenant        = os.environ["SMOKE_TENANT"]
namespace     = os.environ["SMOKE_NAMESPACE"]
token         = os.environ["TF_VAR_streamnative_admin_token"]

client = pulsar.Client(
    service_url,
    authentication=pulsar.AuthenticationToken(token),
)

session = (
    Session.builder()
    .pulsar_client(client)
    .control_topic(control_topic)
    .private_topic_tenant(tenant)
    .private_topic_namespace(namespace)
    .disable_heartbeat()
    .build()
)

print("Smoke: registering a no-op query…")
pq = session.register_private_query(
    source_topics=[f"persistent://{tenant}/{namespace}/__smoke__"],
    user_selector=None,
)
print(f"Smoke: registered queryId={pq.query_id}")

print("Smoke: retracting…")
session.retire_private_query(pq)
print("Smoke: retract OK.")

session.close()
client.close()
print("Smoke: PASS")
PYEOF
