# SN Cloud deployment

Terraform wrapper that deploys the broker-side selector filter orchestrator onto a
StreamNative Cloud instance.

## Prerequisites

- A SN Cloud **Kafka-enabled instance** (cross-format SDK support depends on KoP being
  enabled at the cluster level — this Terraform does NOT configure the broker, only the
  topology + function on top).
- A SN Cloud tenant already created via the console.
- The `streamnative/pulsar` Terraform provider available (>= 0.9.0).
- `pulsarctl` v3.4.x or newer on the host running `terraform apply`.
- `python3` on PATH (used by the function-deploy idempotency probe).
- Pre-built NARs:
  ```bash
  mvn -pl functions/selector-filter,functions/orchestrator package -DskipTests
  ```
- A SN Cloud service account with admin role on the target tenant/namespace + functions
  admin + packages admin. Generate the OAuth2 token via `snctl auth login` and pass it
  through `TF_VAR_streamnative_admin_token`.

## Usage

```bash
cd deploy/terraform/sn-cloud
cp terraform.tfvars.example terraform.tfvars   # fill in instance + cluster + tenant
export TF_VAR_streamnative_admin_token=$(snctl auth current-context --output token)

terraform init
terraform plan
terraform apply
```

Outputs include `control_topic_fqdn` (configure your SDKs to point at this), the
deployed `function_fqn`, and the uploaded `filter_package_url`.

## What this provisions

- Pulsar **state topic** for the orchestrator's registry TableView (compacted, ~10 MB
  threshold).
- Pulsar **SDK control topic** — both Pulsar SDK and Kafka SDK clients publish
  FilterRequest messages here. The cross-format property comes from the SN Cloud
  instance's KoP config; this Terraform does not touch broker settings.
- **Filter NAR package upload** (`pulsarctl packages upload`), referenced by the
  orchestrator's runtime spawn logic.
- **Orchestrator function deployment** (`pulsarctl functions create | update`) with the
  full userConfig matching `OrchestratorConfig.java`.

The shared modules under `../modules/` handle the actual work; this wrapper just
selects sensible SN Cloud defaults (no namespace creation, OAuth2 auth flow).

## What this DOES NOT provision

- The SN Cloud instance, cluster, or tenant — bring your own.
- The namespace (default `create_namespace = false` — make it in the console first).
- Broker config — KoP enablement is per-instance in SN Cloud (Kafka-enabled vs
  Pulsar-only instance type). Cross-format SDK support requires the Kafka-enabled
  instance type.

## Upgrading

To deploy a new filter NAR:

1. Build the new NAR: `mvn -pl functions/selector-filter package`.
2. Bump `filter_package_version` in `terraform.tfvars`.
3. `terraform apply`.

The package upload re-runs (triggered by the version + content hash) and the
orchestrator's `function-package-url` in userConfig changes, which causes the
function to be redeployed.

## Smoke test

`smoke/manual.sh` runs an end-to-end round-trip via the Pulsar SDK against the
deployed orchestrator. It is gated on `TF_VAR_streamnative_admin_token` being set so it
cannot fire accidentally in CI.

```bash
TF_VAR_streamnative_admin_token=... bash smoke/manual.sh
```

The script reads the Terraform outputs of the most recent apply (`terraform output -json`)
and uses them to configure the Python SDK at `sdk/python/`.

## Risks documented

- **Kafka-enabled instance required.** Non-Kafka instances do not have KoP and the
  cross-format property breaks. Terraform does not detect this — the smoke test does.
- **Namespace must match `kafkaNamespace`.** SN Cloud Kafka-enabled instances expose
  the `kafkaNamespace` via console; pass that same `tenant/namespace` into this
  Terraform's `tenant` + `namespace` vars.
- **Service account permissions.** The token must have functions admin + packages admin
  on the target tenant. A 403 from `pulsarctl` during apply usually means scope is missing.
