provider "pulsar" {
  web_service_url = var.streamnative_admin_url
  # SN Cloud uses OAuth2 service-account tokens for admin. The streamnative/pulsar provider
  # accepts a long-lived token via the `token` field.
  token = var.streamnative_admin_token
}

module "topology" {
  source = "../modules/orchestrator-topology"

  tenant               = var.tenant
  namespace            = var.namespace
  create_namespace     = var.create_namespace
  replication_clusters = var.replication_clusters
  pulsar_admin_url     = var.streamnative_admin_url
  pulsar_admin_token   = var.streamnative_admin_token

  # SN Cloud Kafka-enabled instances apply auto-topic-creation namespace-wide already, but
  # we still pass true so per-filter control topics + private output topics can auto-create
  # under our specific namespace.
  allow_auto_topic_creation = true
  auto_topic_creation_type  = "non-partitioned"
}

module "function" {
  source = "../modules/orchestrator-function"

  tenant                      = var.tenant
  namespace                   = var.namespace
  orchestrator_nar_path       = var.orchestrator_nar_path
  filter_nar_path             = var.filter_nar_path
  filter_package_version      = var.filter_package_version
  function_name               = var.function_name
  pulsar_admin_url            = var.streamnative_admin_url
  pulsar_admin_token          = var.streamnative_admin_token
  state_topic_fqdn            = module.topology.state_topic_fqdn
  control_topic_fqdn          = module.topology.control_topic_fqdn
  idle_gc_enabled             = var.idle_gc_enabled
  idle_gc_ttl_seconds         = var.idle_gc_ttl_seconds
  heartbeat_max_stale_seconds = var.heartbeat_max_stale_seconds
}
