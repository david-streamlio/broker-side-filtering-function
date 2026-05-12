# `orchestrator-topology` module

Provisions the Pulsar topology the broker-side selector filter orchestrator needs:

- Namespace (optional — skipped when SN Cloud pre-provisions it)
- Orchestrator state topic (TableView, compacted)
- SDK -> orchestrator control topic (cross-format: Pulsar + Kafka producers)
- Namespace auto-topic-creation policy (required so the orchestrator can spawn per-filter
  control topics on demand and the SDK can subscribe to private output topics)

The module does NOT deploy the orchestrator function or upload the filter NAR; see the
[`orchestrator-function`](../orchestrator-function/) module for that.

## Inputs

| Name | Required | Default | Purpose |
|---|---|---|---|
| `tenant` | yes | — | Pulsar tenant |
| `namespace` | yes | — | Namespace within tenant |
| `pulsar_admin_url` | yes | — | HTTP admin URL for `pulsarctl` local-execs |
| `pulsar_admin_token` | no | `""` | Admin token (sensitive); empty = no-auth |
| `create_namespace` | no | `true` | False when caller pre-provisions it |
| `state_topic_name` | no | `orchestrator-state` | Short topic name |
| `control_topic_name` | no | `filter-requests` | Short topic name |
| `state_topic_compaction_threshold_bytes` | no | `10485760` (10 MB) | Per-topic compaction policy |
| `state_topic_retention_minutes` | no | `1440` | Per-topic retention |
| `state_topic_retention_size_mb` | no | `1024` | Per-topic retention size |
| `control_topic_retention_minutes` | no | `60` | Short — control plane processes immediately |
| `control_topic_retention_size_mb` | no | `128` | — |
| `allow_auto_topic_creation` | no | `true` | MUST be true for orchestrator to function |
| `auto_topic_creation_type` | no | `non-partitioned` | `non-partitioned` or `partitioned` |
| `namespace_retention_minutes` | no | `60` | Namespace default; per-topic overrides |
| `namespace_retention_size_mb` | no | `1024` | — |

## Outputs

| Name | Purpose |
|---|---|
| `namespace_fqdn` | `tenant/namespace` |
| `state_topic_fqdn` | `persistent://tenant/namespace/orchestrator-state` |
| `control_topic_fqdn` | `persistent://tenant/namespace/filter-requests` |

## Prerequisites

- `pulsarctl` on PATH (the module shells out for per-topic compaction policy because
  the provider's coverage varies by version). Pin a recent release; we test against v3.4.x.
- The `streamnative/pulsar` Terraform provider configured by the calling wrapper.

## Cross-format note

This module does NOT configure the broker for cross-format (Pulsar + Kafka via KoP)
operation. That is broker-level config (`messagingProtocols=kafka`, `kafkaEntryFormat=
pulsar_non_batched`, `kafkaNamespace=<this-module's-namespace>`). See:

- **SN Cloud**: Kafka-enabled instances have this on by default.
- **Pulsar Helm**: apply [`deploy/terraform/pulsar-helm/values.broker.kop.yaml`](../../pulsar-helm/values.broker.kop.yaml)
  to your existing Pulsar release.
