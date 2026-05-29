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

# NOTE: there's no `var.tenant` and no `var.parcel_tenant` here. Earlier
# revisions had cosmetic `stock-demo` / `parcel-demo` Pulsar tenants for
# project organization, but they ended up empty by construction (data
# topics MUST live in public/default for KoP's default kafkaNamespace;
# control plane lives in control-plane/broker-filter). The SAs work fine
# off namespace-level grants on public/default + per-topic grants on
# filter-requests — no tenant-admin scope needed anywhere. See topics.tf
# header comment for the full reasoning.

variable "parcel_app_prefix" {
  description = "Name prefix for the parcels-demo SDK service account + apikey + rolebindings. Produces 'parcel-demo-app' as the SA name, mirroring the stock-demo SA which inherits its name from var.cluster_name. Pure label — not a Pulsar tenant."
  type        = string
  default     = "parcel-demo"
}

variable "control_plane_tenant" {
  description = "Pulsar tenant that owns the filtering control plane (orchestrator, filter-requests, orchestrator-state, spawned filter functions, selector-filter package). Locked down — only the orchestrator-function SA is in admin_roles."
  type        = string
  default     = "control-plane"
}

variable "control_plane_namespace" {
  description = "Pulsar namespace under control_plane_tenant. The orchestrator function and its spawned children, plus the SDK-facing filter-requests + orchestrator-state topics, all live here."
  type        = string
  default     = "broker-filter"
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

variable "sa_domain" {
  description = "Auth domain for Pulsar SA principals — <sa-name>@<sa_domain> is the role used by every pulsar_permission_grant. Leave empty to auto-derive as <organization>.auth.streamnative.cloud (production). Override for staging (typically <organization>.auth.sncloud-stg.dev)."
  type        = string
  default     = ""
}
