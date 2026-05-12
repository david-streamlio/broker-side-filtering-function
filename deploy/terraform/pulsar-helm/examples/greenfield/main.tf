# Greenfield example — bring up a fresh Pulsar Helm release on k8s and provision the
# orchestrator topology + function on top in one terraform apply.
#
# Operators with an EXISTING Pulsar cluster should use ../../main.tf directly instead.

terraform {
  required_version = ">= 1.4.0"
  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.14.0"
    }
    pulsar = {
      source  = "streamnative/pulsar"
      version = ">= 0.9.0"
    }
  }
}

variable "k8s_namespace" {
  description = "Kubernetes namespace to install Pulsar into."
  type        = string
  default     = "pulsar"
}

variable "release_name" {
  description = "Helm release name."
  type        = string
  default     = "pulsar"
}

variable "orchestrator_nar_path" {
  type = string
}

variable "filter_nar_path" {
  type = string
}

# Pulsar Helm release with KoP enabled. The values overlay below mirrors
# ../../values.broker.kop.yaml but expands the templated fields at terraform plan time.
locals {
  kafka_namespace_value     = "${var.release_name}-tenant/filters"
  kop_advertised_host_value = "localhost" # for kind / port-forward smoke — override in production

  broker_values = yamlencode({
    broker = {
      configData = {
        messagingProtocols                  = "kafka"
        kafkaListeners                      = "PLAINTEXT://0.0.0.0:9092"
        kafkaAdvertisedListeners            = "PLAINTEXT://${local.kop_advertised_host_value}:9092"
        kafkaNamespace                      = local.kafka_namespace_value
        kafkaEntryFormat                    = "pulsar_non_batched"
        kafkaTransactionCoordinatorEnabled  = "false"
        brokerEntryMetadataInterceptors     = "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor"
        allowAutoTopicCreation              = "true"
        allowAutoTopicCreationType          = "non-partitioned"
        brokerDeleteInactiveTopicsEnabled   = "false"
      }
    }
  })
}

resource "helm_release" "pulsar" {
  name             = var.release_name
  namespace        = var.k8s_namespace
  create_namespace = true
  repository       = "https://pulsar.apache.org/charts"
  chart            = "pulsar"
  version          = "3.9.0" # pin a tested version

  values = [local.broker_values]

  wait    = true
  timeout = 600
}

# Apply the orchestrator topology + function on top of the freshly-installed release.
module "orchestrator" {
  source = "../.."

  pulsar_admin_url = "http://${var.release_name}-broker.${var.k8s_namespace}.svc.cluster.local:8080"
  cluster_name     = var.release_name
  tenant           = "${var.release_name}-tenant"
  namespace        = "filters"

  orchestrator_nar_path = var.orchestrator_nar_path
  filter_nar_path       = var.filter_nar_path

  depends_on = [helm_release.pulsar]
}

output "control_topic_fqdn" {
  value = module.orchestrator.control_topic_fqdn
}
