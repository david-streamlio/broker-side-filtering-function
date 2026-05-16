variable "key_file_path" {
  description = "Path to StreamNative service account key JSON — never commit this file"
  type        = string
  default     = "./streamnative-key.json"
}

variable "organization" {
  description = "StreamNative organization id (the namespace on SN Cloud). To find yours: `jq -r '.client_email' streamnative-key.json` and read the org id between '@' and '.auth.streamnative.cloud'."
  type        = string
  # No default — forces the user to supply their own org so the streamnative provider
  # targets the namespace their SA key actually has rights in.
}

variable "cluster_name" {
  description = "Cluster name — used as prefix for instance and all resources"
  type        = string
  default     = "stock-demo"
}

variable "location" {
  description = "GCP region for the serverless cluster"
  type        = string
  default     = "us-central1"
}

variable "tenant" {
  description = "Pulsar tenant for the demo's filtering namespace (orchestrator state + control topics)"
  type        = string
  default     = "stock-demo"
}

variable "pulsar_web_service_url" {
  description = "HTTP TLS web service URL — set after first apply: terraform output -raw pulsar_web_service_url"
  type        = string
  default     = ""
}

variable "pulsar_token" {
  description = "JWT token for the pulsar provider — from: snctl auth get-token stock-demo-instance -f streamnative-key.json"
  type        = string
  default     = ""
  sensitive   = true
}

variable "provisioner_sa_name" {
  description = "Name of the SA in streamnative-key.json (the part before '@' in client_email). Bound to the cluster so it gets an Auth0 client-grant for the instance — required for Phase 5's `snctl auth get-token` to mint the Step B pulsar-provider token on a fresh instance. recreate.sh auto-derives and exports this; empty disables the binding."
  type        = string
  default     = ""
}

variable "sa_domain" {
  description = "Auth domain for Pulsar SA principals — <sa-name>@<sa_domain> is the role used by every pulsar_permission_grant. Leave empty to auto-derive as <organization>.auth.streamnative.cloud (production). Override for staging (typically <organization>.auth.sncloud-stg.dev)."
  type        = string
  default     = ""
}
