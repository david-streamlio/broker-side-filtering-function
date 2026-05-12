locals {
  namespace_fqdn     = "${var.tenant}/${var.namespace}"
  state_topic_fqdn   = "persistent://${var.tenant}/${var.namespace}/${var.state_topic_name}"
  control_topic_fqdn = "persistent://${var.tenant}/${var.namespace}/${var.control_topic_name}"
}

# Namespace — created only when the operator hasn't pre-provisioned it (SN Cloud often has).
resource "pulsar_namespace" "this" {
  count     = var.create_namespace ? 1 : 0
  tenant    = var.tenant
  namespace = var.namespace

  # Defaults; per-topic policies on the state topic override these where they differ.
  namespace_config {
    anti_affinity                  = ""
    max_consumers_per_subscription = 50
    max_consumers_per_topic        = 100
    max_producers_per_topic        = 100
    replication_clusters           = var.replication_clusters
  }

  retention_policies {
    retention_minutes    = tostring(var.namespace_retention_minutes)
    retention_size_in_mb = tostring(var.namespace_retention_size_mb)
  }

  topic_auto_creation {
    enable     = var.allow_auto_topic_creation
    type       = var.auto_topic_creation_type
    partitions = 1
  }
}

# State topic — TableView for the orchestrator's registry. Compaction is essential.
resource "pulsar_topic" "state" {
  tenant     = var.tenant
  namespace  = var.namespace
  topic_name = var.state_topic_name
  topic_type = "persistent"
  partitions = 0 # non-partitioned

  depends_on = [pulsar_namespace.this]
}

# SDK -> orchestrator control topic. Cross-format: Pulsar SDK and Kafka SDK both publish here.
# Broker-level KoP config (kafkaEntryFormat=pulsar_non_batched, kafkaNamespace=this namespace)
# is what makes the cross-format work — see deploy/terraform/pulsar-helm/values.broker.kop.yaml.
resource "pulsar_topic" "control" {
  tenant     = var.tenant
  namespace  = var.namespace
  topic_name = var.control_topic_name
  topic_type = "persistent"
  partitions = 0 # non-partitioned — orchestrator parallelism is locked at 1 in v1

  depends_on = [pulsar_namespace.this]
}

# Per-topic policies. The streamnative/pulsar provider exposes setCompactionThreshold via
# pulsar_topic.permission_grant in some versions, but for portability across provider
# revisions we use a local-exec that calls pulsarctl. Idempotent: re-applying the same
# threshold is a no-op server-side.
resource "null_resource" "state_topic_compaction" {
  triggers = {
    topic_fqdn = local.state_topic_fqdn
    threshold  = var.state_topic_compaction_threshold_bytes
    admin_url  = var.pulsar_admin_url
  }

  provisioner "local-exec" {
    command = <<-EOT
      pulsarctl --admin-service-url "${var.pulsar_admin_url}" \
        ${var.pulsar_admin_token != "" ? format("--token \"%s\"", var.pulsar_admin_token) : ""} \
        topics set-compaction-threshold \
        --threshold ${var.state_topic_compaction_threshold_bytes} \
        "${local.state_topic_fqdn}"
    EOT
  }

  depends_on = [pulsar_topic.state]
}

variable "pulsar_admin_url" {
  description = "Pulsar HTTP admin URL (http://broker:8080). Used by pulsarctl-backed local-execs for resources the provider doesn't cover."
  type        = string
}

variable "pulsar_admin_token" {
  description = "Pulsar admin token. Empty string means no-auth (acceptable for dev / self-hosted with disabled auth)."
  type        = string
  default     = ""
  sensitive   = true
}
