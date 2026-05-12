output "control_topic_fqdn" {
  description = "SDK -> orchestrator control topic. Configure your Pulsar SDK Session AND your Kafka SDK with this — both reach the orchestrator via this single topic when KoP is configured (see values.broker.kop.yaml)."
  value       = module.topology.control_topic_fqdn
}

output "state_topic_fqdn" {
  description = "Orchestrator state topic. Visibility only."
  value       = module.topology.state_topic_fqdn
}

output "function_fqn" {
  description = "Deployed orchestrator function FQN."
  value       = module.function.function_fqn
}

output "filter_package_url" {
  description = "Uploaded selector-filter NAR package URL."
  value       = module.function.filter_package_url
}

output "admin_url" {
  description = "Pulsar admin URL the orchestrator + SDK use."
  value       = var.pulsar_admin_url
}
