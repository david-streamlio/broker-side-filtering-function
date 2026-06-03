# Greenfield Pulsar Helm deployment

One-shot Terraform: bring up Apache Pulsar via Helm AND provision the orchestrator
topology + function on top. For operators with no existing cluster.

Operators with an **existing** Pulsar Helm release should NOT use this. Call the
[`pulsar-helm`](../..) module from your own root, **configuring a `provider "pulsar"` there** — the
module no longer embeds one (so it can be called with `depends_on`); see the root-level provider in
this example's `main.tf`. Merge [`../../values.broker.kop.yaml`](../../values.broker.kop.yaml) into
your existing chart values.

## What it does

1. `helm_release` installs `apachepulsar/pulsar` (chart version pinned to `3.9.0`) into
   the chosen Kubernetes namespace, with the KoP broker config baked in via inline values.
2. Once the release is healthy, instantiates the parent `pulsar-helm` module which
   provisions the orchestrator topology + function via the `streamnative/pulsar`
   provider + `pulsarctl` local-execs.

## Prerequisites

- A reachable Kubernetes cluster with `kubectl` context set (kind, k3d, or real).
- `helm` + `terraform` + `pulsarctl` + `python3` on the host running `terraform apply`.
- Pre-built NARs in `functions/{orchestrator,selector-filter}/target/`.

## Usage

```bash
cd deploy/terraform/pulsar-helm/examples/greenfield

terraform init
terraform apply -var "orchestrator_nar_path=$(realpath ../../../../../functions/orchestrator/target/selector-filter-orchestrator-1.0.0-SNAPSHOT.nar)" \
                -var "filter_nar_path=$(realpath ../../../../../functions/selector-filter/target/selector-filter-function-1.0.0-SNAPSHOT.nar)"

# Wire up port-forward for SDK clients
kubectl -n pulsar port-forward svc/pulsar-broker 6650:6650 8080:8080 9092:9092 &
```

## Caveats

- **Image bundles KoP.** The default `apachepulsar/pulsar` images do NOT include the KoP
  protocol handler. To make `messagingProtocols=kafka` actually load you need either:
  - `streamnative/sn-platform:*` images (recommended for greenfield), OR
  - a custom broker image with `pulsar-protocol-handler-kafka-*.nar` baked into `/pulsar/protocols/`.
  Override via `broker.image.repository` + `broker.image.tag` in the chart values.
- **Storage class / persistence.** This example uses chart defaults (typically PVCs). For
  a kind smoke test, override storage to `emptyDir`.
- **Port advertisement.** `kafkaAdvertisedListeners` is hardcoded to `localhost` for kind
  smoke testing. For real deployments, set it to a cluster-resolvable host.
