# ── Tenant ────────────────────────────────────────────────────────────────────
# Requires the pulsar provider (var.pulsar_web_service_url + var.pulsar_token set).
#
# One tenant:
#   control-plane  — Owns the filtering infrastructure (orchestrator, filter-
#                    requests, orchestrator-state, spawned selector-filter
#                    functions, selector-filter package). admin_roles is just
#                    the orchestrator-function SA so SDK clients can't reach
#                    in via tenant-admin shortcuts; SDK access to filter-requests
#                    is granted explicitly per-topic in permissions.tf.
#
# Why not a per-demo `stock-demo` / `parcel-demo` tenant for the SDK SAs?
# Considered and rejected (PR after #35). They'd be empty by construction
# because:
#   - Data topics MUST live in public/default — KoP's default kafkaNamespace
#     is the only place Kafka SDK clients can address topics by short name
#     without FQN gymnastics that break kafka-console-consumer/kafkacat/Kafka
#     Connect (kafka-clients accepts `persistent://tenant/ns/topic` literally
#     but the rest of the ecosystem doesn't).
#   - Per-query filtered-<uuid> outputs are created by Session.registerPrivateQuery
#     and live alongside the source topics for the same kafkaNamespace reason.
#   - The orchestrator + state + filter-requests + spawned filters live in
#     control-plane/broker-filter (this file, immediately below) — locked down
#     to the orchestrator-function SA.
# That left two cosmetic tenants with admin_roles pointing at SAs that didn't
# need tenant-admin anywhere; the SDK SAs work fine off namespace-level grants
# on public/default + per-topic grants on filter-requests. Removed for
# minimum-surface-area.

resource "pulsar_tenant" "control_plane" {
  tenant           = var.control_plane_tenant
  allowed_clusters = [var.cluster_name]
  admin_roles = [
    "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}",
  ]
}

# ── Namespaces ────────────────────────────────────────────────────────────────
# Note: data topics (stock-quotes, parcel-*, filtered-<uuid>) live in
# `public/default` per KoP's default `kafkaNamespace` setting — that's the only
# place Kafka clients connecting via SASL_SSL on :9093 can read/write without
# explicit FQN routing. The control plane (orchestrator + filter-requests +
# orchestrator-state + spawned filter functions) lives in
# `control-plane/broker-filter` so SDK clients have explicit per-topic grants
# only and can't accidentally interact with orchestrator internals.

resource "pulsar_namespace" "broker_filter" {
  tenant    = pulsar_tenant.control_plane.tenant
  namespace = var.control_plane_namespace

  namespace_config {
    replication_clusters = [var.cluster_name]
    message_ttl_seconds  = 86400
  }

  retention_policies {
    retention_minutes    = "1440"
    retention_size_in_mb = "256"
  }
}

# ── Topics ────────────────────────────────────────────────────────────────────

# Source topic — partitioned so ShadowTraffic's Kafka producer auto-partitions
# evenly across symbols, and so the spawned selector-filter functions can scale
# parallelism > 1 if needed.
resource "pulsar_topic" "stock_quotes" {
  tenant     = "public"
  namespace  = "default"
  topic_type = "persistent"
  topic_name = "stock-quotes"
  partitions = 1
}

# Parcels-demo source topics. Pre-created (rather than letting ShadowTraffic
# auto-create them on first produce) so recreate.sh can set
# kafkaEntryFormat=pulsar_non_batched on each topic deterministically. Without
# the per-topic property, KoP keeps Kafka entries in the default batched
# format and headers (carrier/origin_region/customer_tier/…) are not surfaced
# as Pulsar message properties — every JMS selector then sees empty props,
# evaluates to UNKNOWN, and zero messages match (the parcels-demo failure
# mode observed 2026-05-28; namespace-default kafkaEntryFormat alone did not
# propagate to the auto-created topics).
resource "pulsar_topic" "parcel_shipments" {
  tenant     = "public"
  namespace  = "default"
  topic_type = "persistent"
  topic_name = "parcel-shipments"
  partitions = 1
}

resource "pulsar_topic" "parcel_scans" {
  tenant     = "public"
  namespace  = "default"
  topic_type = "persistent"
  topic_name = "parcel-scans"
  partitions = 1
}

resource "pulsar_topic" "parcel_exceptions" {
  tenant     = "public"
  namespace  = "default"
  topic_type = "persistent"
  topic_name = "parcel-exceptions"
  partitions = 1
}

# Control topic — SDK-facing input topic for the orchestrator function.
# multi-query-app publishes FilterRequest{REGISTER,MODIFY,UNREGISTER} records here;
# the orchestrator routes them to per-input-topic selector-filter function instances.
# Both stock-demo-app and parcel-demo-app SAs have explicit produce+consume grants
# on this single topic (see permissions.tf:{app,parcel_app}_filter_requests).
resource "pulsar_topic" "filter_requests" {
  tenant     = pulsar_namespace.broker_filter.tenant
  namespace  = pulsar_namespace.broker_filter.namespace
  topic_type = "persistent"
  topic_name = "filter-requests"
  partitions = 0
}

# Orchestrator state topic — TableView-backed registry of input topics + active queries.
resource "pulsar_topic" "orchestrator_state" {
  tenant     = pulsar_namespace.broker_filter.tenant
  namespace  = pulsar_namespace.broker_filter.namespace
  topic_type = "persistent"
  topic_name = "orchestrator-state"
  partitions = 0
}

# Per-query private output topics are created dynamically by the SDK
# (Session.registerPrivateQuery → admin.topics().createNonPartitionedTopic). They land in
# public/default. The orchestrator + stock_demo_app SAs hold the necessary grants
# via pulsar_permission_grant resources in permissions.tf.
