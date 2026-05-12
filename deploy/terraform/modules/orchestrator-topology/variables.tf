variable "tenant" {
  description = "Pulsar tenant for the orchestrator's topics and namespace."
  type        = string
}

variable "namespace" {
  description = "Pulsar namespace (within tenant) for the orchestrator's state topic and SDK control topic. For cross-format (Pulsar + Kafka) the broker's kafkaNamespace MUST match this value."
  type        = string
}

variable "create_namespace" {
  description = "Whether to create the namespace. Set to false when SN Cloud or the operator pre-provisioned it."
  type        = bool
  default     = true
}

variable "state_topic_name" {
  description = "Short topic name (without persistent:// prefix) for the orchestrator's TableView state topic."
  type        = string
  default     = "orchestrator-state"
}

variable "control_topic_name" {
  description = "Short topic name for the SDK -> orchestrator control topic. SDK clients (Pulsar and Kafka SDK) publish FilterRequest messages here."
  type        = string
  default     = "filter-requests"
}

variable "state_topic_compaction_threshold_bytes" {
  description = "Compaction threshold for the state topic in bytes. Recommended ~10 MB; matches the orchestrator's apply-compaction default."
  type        = number
  default     = 10485760
}

variable "state_topic_retention_minutes" {
  description = "Retention time in minutes on the state topic. 24h default keeps a generous replay window for orchestrator restarts."
  type        = number
  default     = 1440
}

variable "state_topic_retention_size_mb" {
  description = "Retention size in MB on the state topic. -1 = unbounded; both retention dimensions apply."
  type        = number
  default     = 1024
}

variable "control_topic_retention_minutes" {
  description = "Retention time in minutes on the SDK control topic. Short by design — orchestrator processes messages immediately."
  type        = number
  default     = 60
}

variable "control_topic_retention_size_mb" {
  description = "Retention size in MB on the SDK control topic."
  type        = number
  default     = 128
}

variable "allow_auto_topic_creation" {
  description = "Whether the namespace allows auto-topic creation. MUST be true so the orchestrator can spawn per-filter control topics and the SDK can subscribe to private output topics on demand."
  type        = bool
  default     = true
}

variable "auto_topic_creation_type" {
  description = "Auto-topic-creation topic type. Non-partitioned matches the orchestrator's spawn behavior."
  type        = string
  default     = "non-partitioned"
  validation {
    condition     = contains(["non-partitioned", "partitioned"], var.auto_topic_creation_type)
    error_message = "auto_topic_creation_type must be one of: non-partitioned, partitioned."
  }
}

variable "namespace_retention_minutes" {
  description = "Default retention in minutes applied to the namespace. Per-topic policies on the state topic override this."
  type        = number
  default     = 60
}

variable "namespace_retention_size_mb" {
  description = "Default retention in MB applied to the namespace."
  type        = number
  default     = 1024
}

variable "replication_clusters" {
  description = "Pulsar clusters to replicate the namespace to. For single-cluster deployments this is just [cluster_name]. SN Cloud: the SN cluster name. Helm: the cluster name configured in the Pulsar release (usually 'pulsar'). Self-hosted standalone: 'standalone'."
  type        = list(string)
  default     = ["standalone"]
  validation {
    condition     = length(var.replication_clusters) >= 1
    error_message = "replication_clusters must contain at least one cluster name."
  }
}
