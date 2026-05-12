output "namespace_fqdn" {
  description = "Fully-qualified namespace (tenant/namespace) the orchestrator operates in."
  value       = local.namespace_fqdn
}

output "state_topic_fqdn" {
  description = "persistent:// URI of the orchestrator's TableView state topic. Pass into orchestrator-function module as state_topic_fqdn."
  value       = local.state_topic_fqdn
}

output "control_topic_fqdn" {
  description = "persistent:// URI of the SDK -> orchestrator control topic. Pass into orchestrator-function module as control_topic_fqdn AND configure SDK clients with this value."
  value       = local.control_topic_fqdn
}
