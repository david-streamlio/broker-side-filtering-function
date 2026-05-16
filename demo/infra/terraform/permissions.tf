# ── Service Accounts (streamnative provider) ──────────────────────────────────
# Two SAs:
#   stock_demo_app          — ShadowTraffic produces stock-quotes here; multi-query-app
#                              also uses this SA as the SDK Session principal + consumer.
#   orchestrator_function   — runs the orchestrator function; spawned selector-filter
#                              instances inherit the orchestrator's identity.

resource "streamnative_service_account" "stock_demo_app" {
  organization = var.organization
  name         = "${var.cluster_name}-app"
}

resource "streamnative_service_account" "orchestrator_function" {
  organization = var.organization
  name         = "${var.cluster_name}-orchestrator-function"
}

# ── Service Account Bindings ───────────────────────────────────────────────────
# Bind each SA to the cluster. Without this, pulsar-admin returns
# "No Service Account Binding found for role: <sa-name>".

resource "streamnative_service_account_binding" "stock_demo_app" {
  organization         = var.organization
  service_account_name = streamnative_service_account.stock_demo_app.name
  cluster_name         = var.cluster_name
}

resource "streamnative_service_account_binding" "orchestrator_function" {
  organization         = var.organization
  service_account_name = streamnative_service_account.orchestrator_function.name
  cluster_name         = var.cluster_name
}

# ── Locals ────────────────────────────────────────────────────────────────────
# Pulsar principal = <sa-name>@<sa_domain>
#   Production:  <org-id>.auth.streamnative.cloud         (auto-derived)
#   Staging:     <org-id>.auth.sncloud-stg.dev            (override via var.sa_domain)
locals {
  sa_domain = var.sa_domain != "" ? var.sa_domain : "${var.organization}.auth.streamnative.cloud"
}

# ── ClusterRole Bindings (SN Cloud control-plane RBAC) ────────────────────────
# Separate from Pulsar's data-plane permission_grants below. Without these,
# snctl / KoP operations return "not authorized" or "JoinGroup failed: Group
# authorization failed" even when Pulsar grants are correct.

# multi-query-app's Kafka consumer (in the SDK) needs `kafka.groups.{read,describe}`
# to JoinGroup against KoP. consumer-group-reader is the narrowest ClusterRole
# with those verbs.
resource "streamnative_rolebinding" "stock_demo_app_consumer_group_reader" {
  organization          = var.organization
  name                  = "${var.cluster_name}-app-consumer-group-reader"
  cluster_role_name     = "consumer-group-reader"
  service_account_names = [streamnative_service_account.stock_demo_app.name]
}

# ── Permission Grants (pulsar provider) ───────────────────────────────────────

# stock_demo_app — produce + consume + packages on public/default. Covers:
#   - ShadowTraffic publishing into persistent://public/default/stock-quotes
#   - SDK consumers reading from per-query private filtered-* topics (which the
#     orchestrator's spawned filters auto-create in this namespace)
resource "pulsar_permission_grant" "app_public_default" {
  namespace = "public/default"
  role      = "${streamnative_service_account.stock_demo_app.name}@${local.sa_domain}"
  actions   = ["produce", "consume", "packages"]
}

# stock_demo_app — produce + consume on filter-requests so the SDK can publish
# REGISTER/MODIFY/UNREGISTER records (orchestrator consumes them). consume is for
# the SDK's heartbeat thread responses.
resource "pulsar_permission_grant" "app_filter_requests" {
  depends_on = [pulsar_topic.filter_requests]
  topic      = "persistent://${var.tenant}/filtering/filter-requests"
  role       = "${streamnative_service_account.stock_demo_app.name}@${local.sa_domain}"
  actions    = ["produce", "consume"]
}

# orchestrator_function — deploy spawned filter functions + upload NAR packages
# in the filtering namespace. Required for the orchestrator's
# functions().createFunction call when spawning a selector-filter instance.
resource "pulsar_permission_grant" "orchestrator_filter_deploy" {
  depends_on = [pulsar_namespace.filtering]
  namespace  = "${var.tenant}/filtering"
  role       = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions    = ["functions", "packages"]
}

# orchestrator_function — consume the SDK-facing FilterRequest topic
resource "pulsar_permission_grant" "orchestrator_consume_control" {
  depends_on = [pulsar_topic.filter_requests]
  topic      = "persistent://${var.tenant}/filtering/filter-requests"
  role       = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions    = ["consume"]
}

# orchestrator_function — read + write its TableView state topic
resource "pulsar_permission_grant" "orchestrator_state_consume" {
  depends_on = [pulsar_topic.orchestrator_state]
  topic      = "persistent://${var.tenant}/filtering/orchestrator-state"
  role       = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions    = ["consume"]
}

resource "pulsar_permission_grant" "orchestrator_state_produce" {
  depends_on = [pulsar_topic.orchestrator_state]
  topic      = "persistent://${var.tenant}/filtering/orchestrator-state"
  role       = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions    = ["produce"]
}

# orchestrator_function — consume the source topic. Required so spawned
# selector-filter functions (which inherit the orchestrator SA) can read
# stock-quotes from public/default.
resource "pulsar_permission_grant" "orchestrator_consume_source" {
  depends_on = [pulsar_topic.stock_quotes]
  topic      = "persistent://public/default/stock-quotes"
  role       = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions    = ["consume"]
}

# orchestrator_function — produce to all per-query private topics in public/default.
# Spawned selector-filter function instances write filtered records here.
resource "pulsar_permission_grant" "orchestrator_produce_output" {
  namespace = "public/default"
  role      = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions   = ["produce"]
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "service_accounts" {
  description = "SA names — apikeys.tf produces the corresponding JWT tokens"
  value = {
    app                   = streamnative_service_account.stock_demo_app.name
    orchestrator_function = streamnative_service_account.orchestrator_function.name
  }
}
