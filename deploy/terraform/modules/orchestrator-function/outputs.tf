output "function_fqn" {
  description = "tenant/namespace/function-name — useful for follow-on pulsarctl commands."
  value       = local.function_fqn
}

output "filter_package_url" {
  description = "Pulsar package URL the orchestrator uses to spawn filter functions. Operators upgrading the filter NAR should bump filter_package_version, NOT replace this URL."
  value       = local.filter_package_url_computed
}

output "user_config" {
  description = "The materialized userConfig passed to the orchestrator function. Helpful for debugging."
  value       = local.user_config
  sensitive   = false
}
