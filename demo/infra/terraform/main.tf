# Multi-query stock-quote demo — StreamNative Serverless Cluster
#
# Two-phase apply:
#   1. terraform init && terraform apply  (creates instance + cluster)
#      -> capture `pulsar_web_service_url` from the output
#   2. export TF_VAR_pulsar_web_service_url=<url>
#      export TF_VAR_pulsar_token=$(snctl auth get-token stock-demo-instance -f streamnative-key.json)
#      terraform apply  (creates tenant, namespaces, topics, SAs, grants, apikeys)
#
# The recreate.sh helper script orchestrates both phases + orchestrator deploy.

terraform {
  required_providers {
    streamnative = {
      version = "0.13.0"
      source  = "streamnative/streamnative"
    }
    # terraform-provider-pulsar manages Pulsar-level resources (tenants, namespaces,
    # topics, permissions). Requires a second apply after the cluster is ready.
    pulsar = {
      version = "~> 0.3"
      source  = "streamnative/pulsar"
    }
  }
}

provider "streamnative" {
  key_file_path = var.key_file_path
}
# Staging override (export before terraform commands):
#   GLOBAL_DEFAULT_API_SERVER=https://api.sncloud-stg.dev
#   GLOBAL_DEFAULT_AUDIENCE=https://api.sncloud-stg.dev
#   GLOBAL_DEFAULT_ISSUER=https://auth.sncloud-stg.dev/

# Pulsar-level provider — JWT token auth. Empty defaults so the first-phase apply
# can run without the cluster existing yet.
provider "pulsar" {
  web_service_url = var.pulsar_web_service_url
  token           = var.pulsar_token
  api_version     = "3"
}

# ── Instance (serverless) ────────────────────────────────────────────────────

resource "streamnative_pulsar_instance" "stock_demo" {
  organization      = var.organization
  name              = "${var.cluster_name}-instance"
  availability_mode = "zonal"
  pool_name         = "shared-gcp"
  pool_namespace    = "streamnative"
  type              = "dedicated"
}

data "streamnative_pulsar_instance" "stock_demo" {
  depends_on   = [streamnative_pulsar_instance.stock_demo]
  name         = streamnative_pulsar_instance.stock_demo.name
  organization = streamnative_pulsar_instance.stock_demo.organization
}

# ── Cluster ───────────────────────────────────────────────────────────────────

resource "streamnative_pulsar_cluster" "stock_demo" {
  organization    = var.organization
  name            = var.cluster_name
  instance_name   = streamnative_pulsar_instance.stock_demo.name
  location        = var.location
  release_channel = "rapid"

  bookie_replicas         = 3
  broker_replicas         = 2
  compute_unit_per_broker = 0.5
  storage_unit_per_bookie = 0.5

  maintenance_window {
    recurrence = "3"
    window {
      start_time = "22:00:00"
      duration   = "2h0m0s"
    }
  }

  config {
    websocket_enabled   = true
    function_enabled    = true # required: orchestrator + selector-filter functions
    transaction_enabled = false

    protocols {
      kafka = {
        enabled = "true" # required: ShadowTraffic + Kafka SDK on KoP :9093
      }
      mqtt = {
        enabled = "false"
      }
    }

    audit_log {
      categories = ["Management", "Describe", "Produce", "Consume"]
    }

    custom = {
      allowAutoTopicCreation = "true"
    }
  }
}

locals {
  cluster_ref = split("/", streamnative_pulsar_cluster.stock_demo.id)
}

data "streamnative_pulsar_cluster" "stock_demo" {
  depends_on   = [streamnative_pulsar_cluster.stock_demo]
  organization = streamnative_pulsar_cluster.stock_demo.organization
  name         = local.cluster_ref[1]
}

# ── Outputs ───────────────────────────────────────────────────────────────────

output "pulsar_instance" {
  value = data.streamnative_pulsar_instance.stock_demo
}

output "pulsar_cluster" {
  value = data.streamnative_pulsar_cluster.stock_demo
}

output "pulsar_web_service_url" {
  description = "Set this as var.pulsar_web_service_url for the second apply (topics + permissions)"
  value       = data.streamnative_pulsar_cluster.stock_demo.http_tls_service_url
}

output "kafka_bootstrap_url" {
  description = "KoP bootstrap server for Kafka clients (SASL_SSL on :9093) — consumed by ShadowTraffic + multi-query-app"
  value = try(
    data.streamnative_pulsar_cluster.stock_demo.kafka_service_urls,
    data.streamnative_pulsar_cluster.stock_demo.kafka_service_url,
    "unknown — check the pulsar_cluster output for the kafka_service_url(s) field name"
  )
}
