locals {
  filter_package_url_computed = (
    var.filter_package_url_override != ""
    ? var.filter_package_url_override
    : "function://${var.tenant}/${var.namespace}/selector-filter@${var.filter_package_version}"
  )

  control_topic_tenant_effective    = var.function_control_topic_tenant != "" ? var.function_control_topic_tenant : var.tenant
  control_topic_namespace_effective = var.function_control_topic_namespace != "" ? var.function_control_topic_namespace : var.namespace

  # Orchestrator userConfig — keys mirror OrchestratorConfig.KEY_* one-for-one. Do NOT
  # rename keys; they are wire contract between this module and the orchestrator's
  # OrchestratorConfig.fromContext parser.
  user_config = {
    "state-topic"                      = var.state_topic_fqdn
    "function-package-url"             = local.filter_package_url_computed
    "function-tenant"                  = var.tenant
    "function-namespace"               = var.namespace
    "function-control-topic-tenant"    = local.control_topic_tenant_effective
    "function-control-topic-namespace" = local.control_topic_namespace_effective
    "function-parallelism"             = 1
    "apply-compaction"                 = var.apply_compaction
    "compaction-threshold-bytes"       = var.compaction_threshold_bytes
    "pulsar-admin-url"                 = var.pulsar_admin_url
    "idle-gc-enabled"                  = var.idle_gc_enabled
    "idle-gc-ttl-seconds"              = var.idle_gc_ttl_seconds
    "idle-gc-sweep-interval-seconds"   = var.idle_gc_sweep_interval_seconds
    "heartbeat-max-stale-seconds"      = var.heartbeat_max_stale_seconds
  }

  user_config_json   = jsonencode(local.user_config)
  user_config_sha256 = sha256(local.user_config_json)

  function_fqn = "${var.tenant}/${var.namespace}/${var.function_name}"
}

# Step 1: upload the selector-filter NAR as a Pulsar package. Re-uploads on content sha
# change OR package-version bump; the script handles the "already exists" no-op case.
resource "null_resource" "filter_package_upload" {
  triggers = {
    package_url     = local.filter_package_url_computed
    nar_sha256      = filesha256(var.filter_nar_path)
    package_version = var.filter_package_version
    admin_url       = var.pulsar_admin_url
  }

  provisioner "local-exec" {
    command     = "${path.module}/scripts/upload-package.sh"
    interpreter = ["/usr/bin/env", "bash"]
    environment = {
      PULSAR_ADMIN_URL    = var.pulsar_admin_url
      PULSAR_ADMIN_TOKEN  = var.pulsar_admin_token
      PACKAGE_URL         = local.filter_package_url_computed
      NAR_PATH            = abspath(var.filter_nar_path)
      PACKAGE_DESCRIPTION = "selector-filter (uploaded by terraform; version ${var.filter_package_version})"
    }
  }
}

# Step 2: deploy or update the orchestrator function. Re-runs on:
#   - userConfig change (anything from variables that flows into local.user_config)
#   - orchestrator NAR content change
#   - admin URL change (sanity)
# Depends on the filter NAR being uploaded first; the orchestrator reads the package URL
# at runtime when spawning child filter functions.
resource "null_resource" "orchestrator_function_deploy" {
  triggers = {
    user_config_sha256      = local.user_config_sha256
    orchestrator_nar_sha256 = filesha256(var.orchestrator_nar_path)
    function_fqn            = local.function_fqn
    admin_url               = var.pulsar_admin_url
    inputs                  = var.control_topic_fqdn
    function_classname      = var.function_classname
  }

  provisioner "local-exec" {
    command     = "${path.module}/scripts/deploy-function.sh"
    interpreter = ["/usr/bin/env", "bash"]
    environment = {
      PULSAR_ADMIN_URL   = var.pulsar_admin_url
      PULSAR_ADMIN_TOKEN = var.pulsar_admin_token
      TENANT             = var.tenant
      NAMESPACE          = var.namespace
      FUNCTION_NAME      = var.function_name
      FUNCTION_CLASSNAME = var.function_classname
      ORCHESTRATOR_NAR   = abspath(var.orchestrator_nar_path)
      USER_CONFIG_JSON   = local.user_config_json
      INPUTS             = var.control_topic_fqdn
      PARALLELISM        = tostring(var.function_parallelism)
    }
  }

  depends_on = [null_resource.filter_package_upload]
}
