# ── API Keys ──────────────────────────────────────────────────────────────────
# JWT tokens for each SA. Use these directly with Pulsar/Kafka clients — avoids
# the OAuth2 client-credential exchange that fails on some staging clusters.

resource "streamnative_apikey" "stock_demo_app" {
  organization         = var.organization
  name                 = "${var.cluster_name}-app"
  service_account_name = streamnative_service_account.stock_demo_app.name
  instance_name        = streamnative_pulsar_instance.stock_demo.name
  expiration_time      = "90d"
}

resource "streamnative_apikey" "orchestrator_function" {
  organization         = var.organization
  name                 = "${var.cluster_name}-orchestrator-function"
  service_account_name = streamnative_service_account.orchestrator_function.name
  instance_name        = streamnative_pulsar_instance.stock_demo.name
  expiration_time      = "90d"
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "apikey_tokens" {
  description = "JWT tokens for each SA — use directly with Pulsar/Kafka (token auth)"
  sensitive   = true
  value = {
    app                   = streamnative_apikey.stock_demo_app.token
    orchestrator_function = streamnative_apikey.orchestrator_function.token
  }
}
