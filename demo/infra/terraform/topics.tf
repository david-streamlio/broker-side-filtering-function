# ── Tenant ────────────────────────────────────────────────────────────────────
# Requires the pulsar provider (var.pulsar_web_service_url + var.pulsar_token set).

resource "pulsar_tenant" "stock_demo" {
  tenant           = var.tenant
  allowed_clusters = [var.cluster_name]
  # Admin roles — must use full principal: <sa-name>@<sa_domain>
  # stock_demo_app needs admin so the SDK's Session.registerPrivateQuery can create
  # the per-query private output topic. Spawned filter functions run as the
  # orchestrator SA and need admin to publish into the same private topics.
  admin_roles = [
    "${streamnative_service_account.stock_demo_app.name}@${local.sa_domain}",
    "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}",
  ]
}

# ── Namespaces ────────────────────────────────────────────────────────────────
# Note: the source topic (stock-quotes) lives in `public/default` per KoP's
# default `kafkaNamespace` setting — that's the only place Kafka clients connecting
# via SASL_SSL on :9093 can read/write without explicit FQN routing. The private
# filtered-* topics also land there for the same reason. Only the orchestrator's
# own state + control topics + per-filter ctrl topics + spawned filter functions
# go under stock-demo/filtering, where they can be managed with project-scoped
# permissions distinct from the broker's shared `public/default`.

resource "pulsar_namespace" "filtering" {
  tenant    = pulsar_tenant.stock_demo.tenant
  namespace = "filtering"

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
resource "pulsar_topic" "filter_requests" {
  tenant     = pulsar_namespace.filtering.tenant
  namespace  = pulsar_namespace.filtering.namespace
  topic_type = "persistent"
  topic_name = "filter-requests"
  partitions = 0
}

# Orchestrator state topic — TableView-backed registry of input topics + active queries.
resource "pulsar_topic" "orchestrator_state" {
  tenant     = pulsar_namespace.filtering.tenant
  namespace  = pulsar_namespace.filtering.namespace
  topic_type = "persistent"
  topic_name = "orchestrator-state"
  partitions = 0
}

# Per-query private output topics are created dynamically by the SDK
# (Session.registerPrivateQuery → admin.topics().createNonPartitionedTopic). They land in
# public/default. The orchestrator + stock_demo_app SAs hold the necessary grants
# via pulsar_permission_grant resources in permissions.tf.
