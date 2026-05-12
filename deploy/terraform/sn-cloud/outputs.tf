output "control_topic_fqdn" {
  description = "SDK -> orchestrator control topic. Configure your Pulsar SDK Session with this as controlTopic; configure your Kafka SDK with the same topic via KoP."
  value       = module.topology.control_topic_fqdn
}

output "state_topic_fqdn" {
  description = "Orchestrator state topic. Provided for visibility/debugging — operators usually don't interact with it directly."
  value       = module.topology.state_topic_fqdn
}

output "function_fqn" {
  description = "Deployed orchestrator function FQN (tenant/namespace/function-name)."
  value       = module.function.function_fqn
}

output "filter_package_url" {
  description = "Uploaded selector-filter NAR package URL."
  value       = module.function.filter_package_url
}

output "service_url" {
  description = "SN Cloud Pulsar service URL — pass to SDK clients."
  value       = var.streamnative_service_url
}

output "admin_url" {
  description = "SN Cloud Pulsar admin URL — pass to SDK clients that need topic policies (e.g., Java SDK PrivateTopicPolicy)."
  value       = var.streamnative_admin_url
}
