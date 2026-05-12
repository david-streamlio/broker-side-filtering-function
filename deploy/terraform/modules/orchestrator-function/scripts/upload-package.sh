#!/usr/bin/env bash
# Upload a NAR as a Pulsar package. Idempotent: a duplicate upload of the same
# (package URL, content sha) is treated as success.
#
# Required env (passed by Terraform local-exec):
#   PULSAR_ADMIN_URL     — http://broker:8080
#   PULSAR_ADMIN_TOKEN   — empty for no-auth
#   PACKAGE_URL          — function://tenant/namespace/selector-filter@1.0.0
#   NAR_PATH             — absolute path to the NAR on the Terraform host
#   PACKAGE_DESCRIPTION  — short human description
set -euo pipefail

: "${PULSAR_ADMIN_URL:?PULSAR_ADMIN_URL is required}"
: "${PACKAGE_URL:?PACKAGE_URL is required}"
: "${NAR_PATH:?NAR_PATH is required}"
PACKAGE_DESCRIPTION="${PACKAGE_DESCRIPTION:-selector-filter NAR}"
PULSAR_ADMIN_TOKEN="${PULSAR_ADMIN_TOKEN:-}"

if [[ ! -f "$NAR_PATH" ]]; then
  echo "ERROR: NAR not found at $NAR_PATH" >&2
  echo "Hint: build with 'mvn -pl functions/selector-filter package -DskipTests'" >&2
  exit 1
fi

if ! command -v pulsarctl >/dev/null 2>&1; then
  echo "ERROR: pulsarctl not on PATH." >&2
  echo "Install: https://github.com/streamnative/pulsarctl/releases (pin to v3.4.x)" >&2
  exit 1
fi

# Build the pulsarctl auth flags array — keeps quoting clean.
PULSARCTL_AUTH=()
if [[ -n "$PULSAR_ADMIN_TOKEN" ]]; then
  PULSARCTL_AUTH+=(--token "$PULSAR_ADMIN_TOKEN")
fi

# If the package already exists at this URL, the upload is a no-op (Pulsar rejects
# duplicate uploads as "package already exists" — we treat that as success).
if pulsarctl --admin-service-url "$PULSAR_ADMIN_URL" "${PULSARCTL_AUTH[@]}" \
     packages get-metadata "$PACKAGE_URL" >/dev/null 2>&1; then
  echo "Package $PACKAGE_URL already exists; skipping upload."
  exit 0
fi

echo "Uploading $NAR_PATH -> $PACKAGE_URL"
pulsarctl --admin-service-url "$PULSAR_ADMIN_URL" "${PULSARCTL_AUTH[@]}" \
  packages upload "$PACKAGE_URL" \
  --path "$NAR_PATH" \
  --description "$PACKAGE_DESCRIPTION"

echo "Upload complete."
