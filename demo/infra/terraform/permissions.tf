# ── Service Accounts (streamnative provider) ──────────────────────────────────
# Three SAs:
#   stock_demo_app          — ShadowTraffic produces stock-quotes here; multi-query-app
#                              uses this SA as the SDK Session principal + consumer
#                              when running the stock demo.
#   parcel_demo_app         — Same role, but for the parcels demo (ShadowTraffic +
#                              SDK Session). Parallel SA so each demo's SDK access
#                              is independently auditable / revocable.
#   orchestrator_function   — Runs the orchestrator function; spawned selector-filter
#                              instances inherit this identity. Owns the
#                              control-plane tenant (admin_roles in topics.tf).

resource "streamnative_service_account" "stock_demo_app" {
  organization = var.organization
  name         = "${var.cluster_name}-app"
}

resource "streamnative_service_account" "parcel_demo_app" {
  organization = var.organization
  name         = "${var.parcel_app_prefix}-app"
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

resource "streamnative_service_account_binding" "parcel_demo_app" {
  organization         = var.organization
  service_account_name = streamnative_service_account.parcel_demo_app.name
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

# Producing over KoP needs the SN Cloud Kafka-layer authorization, separate
# from the Pulsar produce grant in pulsar_permission_grant.app_public_default
# (which alone yields Kafka TopicAuthorizationException — the Pulsar ACL is
# satisfied but the KoP authorizer isn't). namespace-operator is the
# ClusterRole that confers Kafka topic produce/consume, mirroring TradeWise's
# coinbase_connector_namespace_operator. ShadowTraffic produces stock-quotes
# as this SA; consumer-group-reader above only covers Kafka group ops.
resource "streamnative_rolebinding" "stock_demo_app_namespace_operator" {
  organization          = var.organization
  name                  = "${var.cluster_name}-app-namespace-operator"
  cluster_role_name     = "namespace-operator"
  service_account_names = [streamnative_service_account.stock_demo_app.name]
}

# parcel_demo_app — same two ClusterRoles as stock_demo_app (consumer-group-reader +
# namespace-operator). Mirrors the KoP-side authorization the SDK + ShadowTraffic need.
resource "streamnative_rolebinding" "parcel_demo_app_consumer_group_reader" {
  organization          = var.organization
  name                  = "${var.parcel_app_prefix}-app-consumer-group-reader"
  cluster_role_name     = "consumer-group-reader"
  service_account_names = [streamnative_service_account.parcel_demo_app.name]
}

resource "streamnative_rolebinding" "parcel_demo_app_namespace_operator" {
  organization          = var.organization
  name                  = "${var.parcel_app_prefix}-app-namespace-operator"
  cluster_role_name     = "namespace-operator"
  service_account_names = [streamnative_service_account.parcel_demo_app.name]
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

# parcel_demo_app — namespace-level grant on public/default, mirroring
# app_public_default. Same three actions: ShadowTraffic produces parcel-*,
# SDK consumes private filtered-* outputs, packages action covers any
# package-API use by the demo client.
resource "pulsar_permission_grant" "parcel_app_public_default" {
  namespace = "public/default"
  role      = "${streamnative_service_account.parcel_demo_app.name}@${local.sa_domain}"
  actions   = ["produce", "consume", "packages"]
}

# stock_demo_app — produce + consume on filter-requests so the SDK can publish
# REGISTER/MODIFY/UNREGISTER records (orchestrator consumes them). consume is for
# the SDK's heartbeat thread responses. The control topic now lives in
# control-plane/broker-filter (locked-down tenant), so the SDK SA needs explicit
# per-topic access here — it has no tenant-admin shortcut.
resource "pulsar_permission_grant" "app_filter_requests" {
  depends_on = [pulsar_topic.filter_requests]
  topic      = "persistent://${var.control_plane_tenant}/${var.control_plane_namespace}/filter-requests"
  role       = "${streamnative_service_account.stock_demo_app.name}@${local.sa_domain}"
  actions    = ["produce", "consume"]
}

# parcel_demo_app — same grant on filter-requests as stock_demo_app. Both demos'
# SDK clients publish REGISTER/MODIFY/UNREGISTER to the single shared control
# topic; the orchestrator routes by queryId so per-demo isolation is at the
# query-id level, not the topic level.
resource "pulsar_permission_grant" "parcel_app_filter_requests" {
  depends_on = [pulsar_topic.filter_requests]
  topic      = "persistent://${var.control_plane_tenant}/${var.control_plane_namespace}/filter-requests"
  role       = "${streamnative_service_account.parcel_demo_app.name}@${local.sa_domain}"
  actions    = ["produce", "consume"]
}

# NOTE: the orchestrator-function SA used to have four explicit grants in the
# old stock-demo/filtering namespace (functions+packages on the namespace; consume
# on filter-requests; consume+produce on orchestrator-state). All four are now
# redundant — the SA is admin_roles of the new control-plane tenant (see
# topics.tf:pulsar_tenant.control_plane), and tenant-admin grants all of those
# operations implicitly across the entire tenant. Removing them eliminates four
# (namespace, role) id-collision opportunities of the PR #32 class.

# orchestrator_function — consume any source topic and produce to any per-query
# private filtered-* output topic in public/default. Required so spawned
# selector-filter functions (which inherit the orchestrator SA) can both
# subscribe to whichever input topics the SDK registers a query against —
# stock-quotes for the stock-cloud demo, parcel-{shipments,scans,exceptions}
# for the parcels demo, plus any future source topic — AND write filtered
# matches to the SDK-managed filtered-<uuid> output topics.
#
# Consume and produce MUST be in the same resource. The pulsar provider keys
# pulsar_permission_grant by (namespace, role), not by (namespace, role, action),
# and Pulsar's namespaces/{ns}/permissions/{role} API REPLACES (not merges) the
# role's actions on each call. Two separate resources for the same (namespace,
# role) tuple share an id, race in apply, and only the last-applied wins on the
# broker — silently leaving TF state with both grants intact. That bug
# manifested as "filter receives messages but cannot create output producer"
# (verified 2026-05-28, AuthorizationException at producer create on filtered-*).
# Same shape as app_public_default above.
resource "pulsar_permission_grant" "orchestrator_consume_source" {
  namespace = "public/default"
  role      = "${streamnative_service_account.orchestrator_function.name}@${local.sa_domain}"
  actions   = ["consume", "produce"]
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "service_accounts" {
  description = "SA names — apikeys.tf produces the corresponding JWT tokens"
  value = {
    app                   = streamnative_service_account.stock_demo_app.name
    parcel_app            = streamnative_service_account.parcel_demo_app.name
    orchestrator_function = streamnative_service_account.orchestrator_function.name
  }
}
