# ---------------------------------------------------------------- SN Cloud connection

variable "streamnative_admin_url" {
  description = "Pulsar admin URL of the SN Cloud cluster. Find via `snctl get clusters -o yaml` or the SN Cloud console: instance -> cluster details -> Admin URL."
  type        = string
}

variable "streamnative_service_url" {
  description = "Pulsar service URL (pulsar+ssl://...) of the SN Cloud cluster. Not used by Terraform directly; included in outputs for downstream consumers."
  type        = string
}

variable "streamnative_admin_token" {
  description = "OAuth2 service-account token with admin scope. Generate via `snctl auth login --output-credentials`. Sensitive; pass through TF_VAR_streamnative_admin_token env."
  type        = string
  sensitive   = true
}

# ---------------------------------------------------------------- topology

variable "tenant" {
  description = "Pulsar tenant. SN Cloud usually pre-creates the tenant — pass the existing one."
  type        = string
}

variable "namespace" {
  description = "Pulsar namespace within tenant. For cross-format the SN Cloud instance MUST be Kafka-enabled and its kafkaNamespace must match this namespace."
  type        = string
  default     = "filters"
}

variable "create_namespace" {
  description = "Whether to create the namespace. SN Cloud usually requires the operator to create namespaces via the console first, so default false."
  type        = bool
  default     = false
}

variable "replication_clusters" {
  description = "SN Cloud cluster name(s) the namespace replicates to. Find via `snctl get clusters` — typically a single cluster name like 'use1-prod'."
  type        = list(string)
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
  description = "Filter NAR package version tag."
  type        = string
  default     = "1.0.0"
}

variable "function_name" {
  description = "Pulsar function name for the orchestrator."
  type        = string
  default     = "selector-filter-orchestrator"
}

variable "idle_gc_enabled" {
  description = "Enable idle-filter GC. Default false; turn on when you know your SDK clients close cleanly OR you've validated heartbeat detection in your environment."
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
