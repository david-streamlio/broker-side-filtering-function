provider "pulsar" {
  web_service_url = var.pulsar_admin_url
  token           = var.pulsar_admin_token
}

# Create the tenant if it doesn't already exist. Self-hosted Helm clusters don't preprovision
# anything beyond the default 'public' tenant. The provider's pulsar_tenant resource is
# idempotent in the sense that re-applying with the same allowed_clusters set is a no-op.
resource "pulsar_tenant" "this" {
  tenant           = var.tenant
  allowed_clusters = [var.cluster_name]
  admin_roles      = []
}

module "topology" {
  source = "../modules/orchestrator-topology"

  tenant               = var.tenant
  namespace            = var.namespace
  create_namespace     = var.create_namespace
  replication_clusters = [var.cluster_name]
  pulsar_admin_url     = var.pulsar_admin_url
  pulsar_admin_token   = var.pulsar_admin_token

  # The orchestrator needs to spawn per-filter control topics + the SDK auto-subscribes
  # to private output topics. Without auto-topic-creation, spawn fails.
  allow_auto_topic_creation = true
  auto_topic_creation_type  = "non-partitioned"

  depends_on = [pulsar_tenant.this]
}

module "function" {
  source = "../modules/orchestrator-function"

  tenant                      = var.tenant
  namespace                   = var.namespace
  orchestrator_nar_path       = var.orchestrator_nar_path
  filter_nar_path             = var.filter_nar_path
  filter_package_version      = var.filter_package_version
  function_name               = var.function_name
  pulsar_admin_url            = var.pulsar_admin_url
  pulsar_admin_token          = var.pulsar_admin_token
  state_topic_fqdn            = module.topology.state_topic_fqdn
  control_topic_fqdn          = module.topology.control_topic_fqdn
  idle_gc_enabled             = var.idle_gc_enabled
  idle_gc_ttl_seconds         = var.idle_gc_ttl_seconds
  heartbeat_max_stale_seconds = var.heartbeat_max_stale_seconds
}
