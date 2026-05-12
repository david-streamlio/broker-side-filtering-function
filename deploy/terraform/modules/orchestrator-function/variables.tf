# ---------------------------------------------------------------- required

variable "tenant" {
  description = "Pulsar tenant the orchestrator is deployed under."
  type        = string
}

variable "namespace" {
  description = "Pulsar namespace within tenant for the orchestrator function instance."
  type        = string
}

variable "orchestrator_nar_path" {
  description = "Absolute or relative path to the orchestrator NAR on the Terraform host. Built by `mvn -pl functions/orchestrator package`."
  type        = string
}

variable "filter_nar_path" {
  description = "Absolute or relative path to the selector-filter NAR. Built by `mvn -pl functions/selector-filter package`. Uploaded to Pulsar as a package the orchestrator references via filter_package_url."
  type        = string
}

variable "pulsar_admin_url" {
  description = "Pulsar HTTP admin URL (http://broker:8080). Used by pulsarctl-backed local-execs for package upload and function deploy."
  type        = string
}

variable "pulsar_admin_token" {
  description = "Pulsar admin token. Empty string means no-auth (dev only)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "state_topic_fqdn" {
  description = "Output of the orchestrator-topology module: persistent://... state topic URI."
  type        = string
}

variable "control_topic_fqdn" {
  description = "Output of the orchestrator-topology module: persistent://... SDK control topic URI. Used as the orchestrator function's input."
  type        = string
}

# ---------------------------------------------------------------- defaulted (mirrors OrchestratorConfig.java)

variable "function_name" {
  description = "Pulsar function name for the orchestrator. Choose something stable; spawned filter function names are independent."
  type        = string
  default     = "selector-filter-orchestrator"
}

variable "function_classname" {
  description = "Java class implementing the orchestrator. Do not change unless you're forking the code."
  type        = string
  default     = "io.streamnative.filter.orchestrator.OrchestratorFunction"
}

variable "function_parallelism" {
  description = "Orchestrator parallelism. MUST be 1 in v1 — multiple instances race on spawn decisions for the same input topic. See README R2."
  type        = number
  default     = 1
  validation {
    condition     = var.function_parallelism == 1
    error_message = "function_parallelism must be 1 in v1. Sharding via KeyShared or leader election is the v2 path."
  }
}

variable "filter_package_version" {
  description = "Version tag appended to the filter NAR package URL. Bump to force a re-upload of a new filter NAR."
  type        = string
  default     = "1.0.0"
}

variable "filter_package_url_override" {
  description = "Override the computed filter package URL. Use only when you need to point at a NAR uploaded under a different tenant/namespace than the orchestrator runs in."
  type        = string
  default     = ""
}

variable "function_control_topic_tenant" {
  description = "Tenant for per-filter-function control topics. Defaults to the orchestrator's tenant."
  type        = string
  default     = ""
}

variable "function_control_topic_namespace" {
  description = "Namespace for per-filter-function control topics. Defaults to the orchestrator's namespace."
  type        = string
  default     = ""
}

variable "apply_compaction" {
  description = "Whether the orchestrator applies per-topic compaction policies on per-function control topics at spawn time. Requires the orchestrator principal to have topic-policies admin."
  type        = bool
  default     = true
}

variable "compaction_threshold_bytes" {
  description = "Compaction threshold (bytes) the orchestrator sets on each per-function control topic."
  type        = number
  default     = 10485760
}

variable "idle_gc_enabled" {
  description = "Master switch for idle-filter GC. When true the orchestrator sweeps for idle filter functions whose queryIds have all UNREGISTERed."
  type        = bool
  default     = false
}

variable "idle_gc_ttl_seconds" {
  description = "Continuous-idle time in seconds before a filter function becomes deletion-eligible."
  type        = number
  default     = 300
}

variable "idle_gc_sweep_interval_seconds" {
  description = "How often the sweeper evaluates the registry, in seconds."
  type        = number
  default     = 30
}

variable "heartbeat_max_stale_seconds" {
  description = "Max allowed gap between SDK heartbeats before orchestrator synthesizes an UNREGISTER for a queryId. Default 90s = three missed heartbeats at the SDK's default 30s interval."
  type        = number
  default     = 90
}
