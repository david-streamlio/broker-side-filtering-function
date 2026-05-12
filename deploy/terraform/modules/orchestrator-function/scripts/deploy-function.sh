#!/usr/bin/env bash
# Deploy (or update) the orchestrator Pulsar function. Idempotent: probes status first,
# then chooses create vs update.
#
# Required env (passed by Terraform local-exec):
#   PULSAR_ADMIN_URL     — http://broker:8080
#   PULSAR_ADMIN_TOKEN   — empty for no-auth
#   TENANT, NAMESPACE, FUNCTION_NAME
#   ORCHESTRATOR_NAR     — absolute path on the Terraform host
#   USER_CONFIG_JSON     — single-line JSON dict; mirrors the 14 OrchestratorConfig keys
#   FUNCTION_CLASSNAME   — io.streamnative.filter.orchestrator.OrchestratorFunction
#   INPUTS               — comma-separated input topic FQDNs (the control topic)
#   PARALLELISM          — integer (MUST be 1 per the v1 invariant)
set -euo pipefail

: "${PULSAR_ADMIN_URL:?PULSAR_ADMIN_URL is required}"
: "${TENANT:?TENANT is required}"
: "${NAMESPACE:?NAMESPACE is required}"
: "${FUNCTION_NAME:?FUNCTION_NAME is required}"
: "${ORCHESTRATOR_NAR:?ORCHESTRATOR_NAR is required}"
: "${USER_CONFIG_JSON:?USER_CONFIG_JSON is required}"
: "${FUNCTION_CLASSNAME:?FUNCTION_CLASSNAME is required}"
: "${INPUTS:?INPUTS is required}"
: "${PARALLELISM:?PARALLELISM is required}"
PULSAR_ADMIN_TOKEN="${PULSAR_ADMIN_TOKEN:-}"

if [[ ! -f "$ORCHESTRATOR_NAR" ]]; then
  echo "ERROR: orchestrator NAR not found at $ORCHESTRATOR_NAR" >&2
  echo "Hint: build with 'mvn -pl functions/orchestrator package -DskipTests'" >&2
  exit 1
fi

if ! command -v pulsarctl >/dev/null 2>&1; then
  echo "ERROR: pulsarctl not on PATH." >&2
  exit 1
fi

PULSARCTL_AUTH=()
if [[ -n "$PULSAR_ADMIN_TOKEN" ]]; then
  PULSARCTL_AUTH+=(--token "$PULSAR_ADMIN_TOKEN")
fi

# Decide: create or update.
if pulsarctl --admin-service-url "$PULSAR_ADMIN_URL" "${PULSARCTL_AUTH[@]}" \
     functions status \
     --tenant "$TENANT" --namespace "$NAMESPACE" --name "$FUNCTION_NAME" \
     >/dev/null 2>&1; then
  ACTION="update"
else
  ACTION="create"
fi

echo "Function ${TENANT}/${NAMESPACE}/${FUNCTION_NAME}: $ACTION"

pulsarctl --admin-service-url "$PULSAR_ADMIN_URL" "${PULSARCTL_AUTH[@]}" \
  functions "$ACTION" \
  --tenant "$TENANT" \
  --namespace "$NAMESPACE" \
  --name "$FUNCTION_NAME" \
  --jar "$ORCHESTRATOR_NAR" \
  --classname "$FUNCTION_CLASSNAME" \
  --inputs "$INPUTS" \
  --parallelism "$PARALLELISM" \
  --user-config "$USER_CONFIG_JSON" \
  --processing-guarantees ATLEAST_ONCE \
  --subs-position Earliest

echo "Function $ACTION complete. Polling for RUNNING…"

DEADLINE=$(($(date +%s) + 90))
while [[ $(date +%s) -lt $DEADLINE ]]; do
  STATUS=$(pulsarctl --admin-service-url "$PULSAR_ADMIN_URL" "${PULSARCTL_AUTH[@]}" \
    functions status \
    --tenant "$TENANT" --namespace "$NAMESPACE" --name "$FUNCTION_NAME" 2>/dev/null || true)
  # Look for numRunning matching numInstances. Pulsarctl prints JSON.
  if echo "$STATUS" | grep -q '"numRunning"' && \
     echo "$STATUS" | python3 -c 'import sys,json; d=json.load(sys.stdin); sys.exit(0 if d.get("numRunning",0)>0 and d.get("numRunning")==d.get("numInstances") else 1)' 2>/dev/null; then
    echo "Function $FUNCTION_NAME is RUNNING."
    exit 0
  fi
  sleep 2
done

echo "ERROR: function $FUNCTION_NAME did not reach RUNNING within 90s." >&2
echo "Last status:" >&2
echo "$STATUS" >&2
exit 1
