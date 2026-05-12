# Pulsar Helm deployment

Terraform wrapper for self-hosted Pulsar (deployed via the
[apachepulsar/pulsar Helm chart](https://github.com/apache/pulsar-helm-chart) or any
equivalent) plus KoP. This wrapper assumes the Pulsar cluster ALREADY EXISTS and is
reachable from the host running `terraform apply`. For a one-shot greenfield install,
see [`examples/greenfield/`](examples/greenfield/).

## Prerequisites

- A reachable Pulsar broker with the Kafka protocol handler loaded. Apache Pulsar's
  default images do NOT bundle KoP — either use a `streamnative/sn-platform:*` image OR
  sideload the KoP plugin (see Caveats below).
- Broker configured with KoP cross-format settings. See
  [`values.broker.kop.yaml`](values.broker.kop.yaml) — merge into your existing Helm
  values BEFORE running this Terraform.
- `terraform` >= 1.4, `pulsarctl` v3.4.x, `python3` on PATH.
- Pre-built NARs:
  ```bash
  mvn -pl functions/selector-filter,functions/orchestrator package -DskipTests
  ```

## Usage

### Step 1: enable KoP on your Pulsar release

```bash
# Edit values.broker.kop.yaml: replace ${KAFKA_NAMESPACE} with your tenant/namespace
# (e.g., "public/filters") and ${KOP_ADVERTISED_HOST} with the host external Kafka
# clients reach. Then:
envsubst < deploy/terraform/pulsar-helm/values.broker.kop.yaml > /tmp/kop.yaml
helm upgrade my-pulsar apachepulsar/pulsar -f my-existing-values.yaml -f /tmp/kop.yaml
```

Or just edit the file in place.

### Step 2: provision the orchestrator topology + function

```bash
cd deploy/terraform/pulsar-helm
cp terraform.tfvars.example terraform.tfvars   # fill in pulsar_admin_url + paths
terraform init
terraform plan
terraform apply
```

## Outputs

| Output | Purpose |
|---|---|
| `control_topic_fqdn` | Configure your Pulsar SDK Session AND your Kafka SDK with this — both reach the orchestrator via this same topic when KoP is wired correctly. |
| `state_topic_fqdn` | Orchestrator state topic; visibility only. |
| `function_fqn` | Deployed orchestrator function. |
| `filter_package_url` | Uploaded filter NAR. |

## Smoke test (kind cluster)

`smoke/kind-bootstrap.sh` brings up a kind cluster, installs the Pulsar Helm chart with
the KoP overlay, applies this Terraform, and drives a round-trip via the Python SDK.

```bash
make -C ../../.. deploy-smoke-helm
# or directly:
bash smoke/kind-bootstrap.sh
```

Wall time: ~5 min on a warm Docker host (the Pulsar Helm chart is the long pole).

## Caveats

- **KoP image requirement.** The vanilla `apachepulsar/pulsar` images do not bundle
  `pulsar-protocol-handler-kafka-*.nar`. For self-hosted KoP you need one of:
  - `streamnative/sn-platform:*` (recommended for ease — KoP is built in).
  - A custom broker image with the KoP NAR placed at `/pulsar/protocols/`.
  See the SN Platform docs for image tags.
- **kafkaNamespace alignment.** The broker's `kafkaNamespace` MUST equal
  `<tenant>/<namespace>` from this Terraform. A mismatch makes Kafka clients see
  "topic not found" while Pulsar clients work fine.
- **Helm release dependency.** `pulsar_admin_url` must be reachable when `terraform
  apply` runs. For in-cluster Helm releases, that usually means `kubectl port-forward`
  or running Terraform from a pod with cluster network access.
- **Token auth.** When your Helm release has token auth enabled, the same token must
  have functions admin + packages admin on the configured tenant. The wrapper does NOT
  create roles or grant permissions.

## Upgrading the filter NAR

Same flow as SN Cloud: bump `filter_package_version`, rebuild, `terraform apply`. The
filter package upload re-runs; existing filter function instances keep their old NAR
until they're GC'd or manually deleted.
