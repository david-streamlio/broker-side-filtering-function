# `deploy/` — Infrastructure-as-Code for the orchestrator

This directory holds Terraform modules + wrappers for deploying the broker-side selector
filter orchestrator into a Pulsar cluster.

## Pick a path

| Where Pulsar runs | What you do | Where the docs live |
|---|---|---|
| **StreamNative Cloud** (managed) | Apply the SN Cloud Terraform wrapper. Bring your own tenant + Kafka-enabled instance. | [`terraform/sn-cloud/README.md`](terraform/sn-cloud/README.md) |
| **Self-hosted Pulsar Helm chart** (Kubernetes) | Merge our KoP broker values into your existing release, then apply the Helm Terraform wrapper. Greenfield: use the `examples/greenfield/` one-shot. | [`terraform/pulsar-helm/README.md`](terraform/pulsar-helm/README.md) |
| **Bare-metal / VM / something custom** | Roll your own. The shared modules under [`terraform/modules/`](terraform/modules/) are reusable; just point the provider at your admin URL. | [`terraform/modules/`](terraform/modules/) |

## What gets provisioned (all paths)

- **State topic** (`persistent://<tenant>/<namespace>/orchestrator-state`) — compacted, 10 MB threshold by default.
- **SDK control topic** (`persistent://<tenant>/<namespace>/filter-requests`) — non-partitioned. Accepts both Pulsar AND Kafka SDK clients when the broker has KoP enabled.
- **Filter NAR package upload** — `function://<tenant>/<namespace>/selector-filter@<version>`.
- **Orchestrator function deploy** — parallelism 1; full userConfig matching `OrchestratorConfig.java`.
- **Namespace policies** — auto-topic-creation enabled (required for orchestrator's runtime spawn behavior).

## What you provide

| Path | Required inputs |
|---|---|
| **SN Cloud** | Admin URL + service URL + OAuth2 token + tenant + cluster name + NARs |
| **Helm** | Admin URL + cluster name + NARs (plus a KoP-enabled Pulsar release already running) |

## Cross-format control topic

The orchestrator's SDK control topic accepts both Pulsar producers (from the Java/Python
SDK) and Kafka producers (from the Kafka SDK via KoP) on the same Pulsar topic. The
property comes from broker-level KoP config — `messagingProtocols=kafka`,
`kafkaEntryFormat=pulsar_non_batched`, `kafkaNamespace=<this-topic's-namespace>`. The
Terraform provisions the topic itself; the broker config is:

- **SN Cloud**: on by default in Kafka-enabled instances; nothing to configure.
- **Helm**: see [`terraform/pulsar-helm/values.broker.kop.yaml`](terraform/pulsar-helm/values.broker.kop.yaml) — merge into your Pulsar Helm release values.

## Module decomposition

```
terraform/
  modules/
    orchestrator-topology/    # tenant + namespace + topics + policies
    orchestrator-function/    # filter NAR upload + orchestrator function deploy
  sn-cloud/                   # SN Cloud wrapper composing both modules
  pulsar-helm/                # Helm wrapper composing both modules + KoP snippet
    examples/greenfield/      # one-shot: Helm release + orchestrator together
```

Both wrappers compose the same two shared modules. The wrappers differ only in:
- **Auth + admin URL discovery** (SN Cloud uses OAuth2 tokens; Helm uses release-issued tokens or no-auth)
- **Tenant + namespace lifecycle** (SN Cloud expects them pre-created; Helm creates them)
- **Broker-config delivery** (none for SN Cloud; values snippet for Helm)

## Why `null_resource` + `local-exec` for some resources

The `streamnative/pulsar` Terraform provider's `pulsar_function` resource only accepts
local jar paths, not `function://` package URLs — but the orchestrator must reference
a package URL at runtime so it can spawn child filter functions. There is also no
`pulsar_package` resource. We fall back to `pulsarctl` via `null_resource` +
`local-exec` for both the package upload and the function deploy. Idempotency comes from
content shas in the `triggers` block + status-probe-then-create-or-update logic in the
scripts.

See [`terraform/modules/orchestrator-function/README.md`](terraform/modules/orchestrator-function/README.md) for details.

## Smoke tests

| Target | Where | What it does |
|---|---|---|
| `make deploy-smoke-helm` | top-level Makefile | Brings up kind, installs Pulsar Helm with KoP, applies the greenfield Terraform, drives a Python SDK round-trip. ~5 min. |
| `make deploy-smoke-sn-cloud` | top-level Makefile | Requires `TF_VAR_streamnative_admin_token`. Drives a Python SDK round-trip against your most recent SN Cloud `terraform apply`. |

## Versioning + upgrades

To deploy a new filter NAR:
1. Rebuild: `make package` (or `mvn -pl functions/selector-filter package -DskipTests`).
2. Bump `filter_package_version` in your `terraform.tfvars`.
3. `terraform apply`.

The package upload re-runs on the version + content sha change, and the function
redeploys because `function-package-url` in its userConfig changed.

## Risks + known gaps

See each wrapper's README for target-specific risks. Cross-cutting:

- **Auth scope.** Whatever principal `terraform apply` runs as must have functions admin
  + packages admin on the configured tenant. A 403 from `pulsarctl` during apply usually
  means scope is missing.
- **NAR file paths in Terraform state.** TF stores the absolute paths; CI runs that
  produce NARs at different paths break re-apply unless `triggers` keys catch the
  content sha (they do). For shared-state pipelines, build the NAR before TF runs.
- **`pulsarctl` version drift.** Pin a tested version (v3.4.x). The scripts probe
  `pulsarctl` on PATH and fail with a clear message if missing.
