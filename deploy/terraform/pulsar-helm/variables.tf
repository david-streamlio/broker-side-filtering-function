# ---------------------------------------------------------------- Pulsar connection

variable "pulsar_admin_url" {
  description = "Pulsar HTTP admin URL of the cluster. For an in-cluster Helm release: http://<release>-broker.<namespace>.svc.cluster.local:8080. For a port-forward smoke test: http://localhost:8080."
  type        = string
}

variable "pulsar_admin_token" {
  description = "Pulsar admin token. Empty string = no-auth (typical for dev Helm releases without authentication enabled)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "cluster_name" {
  description = "Pulsar cluster name as configured in the Helm release. Used in namespace replication_clusters. Default 'pulsar' matches the apachepulsar/pulsar Helm chart default."
  type        = string
  default     = "pulsar"
}

# ---------------------------------------------------------------- topology

variable "tenant" {
  description = "Pulsar tenant. Created by this Terraform unless one already exists."
  type        = string
}

variable "namespace" {
  description = "Pulsar namespace within tenant. MUST match the broker's kafkaNamespace (see values.broker.kop.yaml) for cross-format SDK support."
  type        = string
  default     = "filters"
}

variable "create_namespace" {
  description = "Whether this Terraform creates the namespace."
  type        = bool
  default     = true
}

# ---------------------------------------------------------------- function

variable "orchestrator_nar_path" {
  description = "Path to the orchestrator NAR on the Terraform host."
  type        = string
}

variable "filter_nar_path" {
  description = "Path to the selector-filter NAR on the Terraform host."
  type        = string
}

variable "filter_package_version" {
  description = "Filter NAR package version tag. Bump to force a re-upload."
  type        = string
  default     = "1.0.0"
}

variable "function_name" {
  description = "Pulsar function name for the orchestrator."
  type        = string
  default     = "selector-filter-orchestrator"
}

variable "idle_gc_enabled" {
  description = "Enable idle-filter GC."
  type        = bool
  default     = false
}

variable "idle_gc_ttl_seconds" {
  description = "Idle-GC TTL when enabled."
  type        = number
  default     = 300
}

variable "heartbeat_max_stale_seconds" {
  description = "Heartbeat staleness threshold when idle-GC is enabled."
  type        = number
  default     = 90
}
