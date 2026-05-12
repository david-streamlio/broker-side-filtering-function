# `orchestrator-function` module

Uploads the selector-filter NAR as a Pulsar package and deploys the orchestrator function
that references it. This module is paired with [`orchestrator-topology`](../orchestrator-topology/)
— the topology provisions the state + control topics; this one provisions the function that
consumes from them.

## How it works

The `streamnative/pulsar` Terraform provider's `pulsar_function` resource only accepts
**local jar paths**, but the orchestrator needs to spawn child filter functions from a
`function://` package URL at runtime. There is also no `pulsar_package` resource at all.
So this module shells out to `pulsarctl` via `null_resource` + `local-exec` for both
operations. Idempotency comes from:

- **Package upload**: triggers on `filesha256(filter_nar_path)` + `filter_package_version`;
  the script also probes `packages get-metadata` and treats already-uploaded as success.
- **Function deploy**: triggers on a `sha256` of the materialized userConfig JSON +
  `filesha256(orchestrator_nar_path)`; the script probes `functions status` and does
  `update` instead of `create` when the function already exists.

## Inputs

### Required

| Name | Purpose |
|---|---|
| `tenant`, `namespace` | Where the orchestrator function runs |
| `orchestrator_nar_path` | Local path to `selector-filter-orchestrator-*.nar` |
| `filter_nar_path` | Local path to `selector-filter-function-*.nar` |
| `pulsar_admin_url` | HTTP admin URL for `pulsarctl` |
| `pulsar_admin_token` | Admin token (sensitive); empty string = no-auth |
| `state_topic_fqdn` | Output of `orchestrator-topology` |
| `control_topic_fqdn` | Output of `orchestrator-topology`; used as the function's input |

### Defaulted (mirrors `OrchestratorConfig.KEY_*`)

| Name | Default | OrchestratorConfig key |
|---|---|---|
| `function_name` | `selector-filter-orchestrator` | (function name, not userConfig) |
| `function_classname` | `io.streamnative.filter.orchestrator.OrchestratorFunction` | — |
| `function_parallelism` | `1` (locked) | `function-parallelism` |
| `filter_package_version` | `1.0.0` | — (composed into `function-package-url`) |
| `filter_package_url_override` | `""` | direct `function-package-url` override |
| `function_control_topic_tenant` | `tenant` | `function-control-topic-tenant` |
| `function_control_topic_namespace` | `namespace` | `function-control-topic-namespace` |
| `apply_compaction` | `true` | `apply-compaction` |
| `compaction_threshold_bytes` | `10485760` | `compaction-threshold-bytes` |
| `idle_gc_enabled` | `false` | `idle-gc-enabled` |
| `idle_gc_ttl_seconds` | `300` | `idle-gc-ttl-seconds` |
| `idle_gc_sweep_interval_seconds` | `30` | `idle-gc-sweep-interval-seconds` |
| `heartbeat_max_stale_seconds` | `90` | `heartbeat-max-stale-seconds` |

## Outputs

| Name | Purpose |
|---|---|
| `function_fqn` | `tenant/namespace/function-name` |
| `filter_package_url` | The composed `function://...` URL passed in userConfig |
| `user_config` | The full materialized userConfig dict — debugging |

## Prerequisites

- `pulsarctl` v3.4.x on PATH on the host running `terraform apply`.
- `python3` on PATH (used by `deploy-function.sh` to parse status JSON).
- The orchestrator's runtime identity must have **functions admin** on the configured
  function tenant/namespace AND **packages admin** for the filter package URL.

## Upgrading the filter NAR

Bump `filter_package_version`, rebuild the NAR, and re-apply. The package upload
triggers on the version change; the orchestrator function's `function-package-url`
in userConfig changes, which causes the function deploy to update. New child filter
functions spawn from the new package; existing ones keep running until they are GC'd
or you manually delete and let the orchestrator respawn.
