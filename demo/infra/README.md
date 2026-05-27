# Demo Infrastructure

Terraform + a one-shot `recreate.sh` that stands up the entire SN Cloud cluster
the multi-query stock-quote demo needs — instance, cluster, tenant, namespaces,
service accounts, role bindings, permission grants, and the orchestrator function.

After this is up, [`../docker-compose.yml`](../docker-compose.yml) brings up
ShadowTraffic + multi-query-app + the observability UI against the cluster.

## Layout

```
infra/
  pulsar-functions/
    orchestrator-function-config.yaml      ← deployed by recreate.sh Phase 7
  terraform/
    main.tf                                ← instance + cluster + outputs
    variables.tf                           ← org, cluster_name, tenant, region
    topics.tf                              ← tenant, namespaces, source topic
    permissions.tf                         ← SAs, rolebindings, permission grants
    apikeys.tf                             ← JWT tokens per SA
    recreate.sh                            ← orchestration (8 phases)
    .gitignore                             ← excludes *.token + tfstate
```

## What it creates

| Resource | Name |
|----------|------|
| Pulsar instance | `stock-demo-instance` (serverless, GCP shared) |
| Pulsar cluster | `stock-demo` (KoP enabled on :9093, functions enabled, auto-topic-creation on) |
| Tenant | `stock-demo` |
| Namespace | `stock-demo/filtering` (orchestrator state + control topics) |
| Topics | `public/default/stock-quotes` (partitioned), `stock-demo/filtering/filter-requests`, `stock-demo/filtering/orchestrator-state` |
| Service accounts | `stock-demo-app` (ShadowTraffic + SDK), `stock-demo-orchestrator-function` |
| ClusterRoleBinding | `stock-demo-app-consumer-group-reader` (lets the SDK's Kafka consumer JoinGroup on KoP) |
| Permission grants | Mix of namespace- and topic-level grants for produce/consume/functions/packages (full list in `permissions.tf`) |
| Function | `orchestrator` deployed into `stock-demo/filtering` (downloads NARs from `$SELECTOR_REPO @ $SELECTOR_REL` and uploads as Pulsar packages) |

The source topic lives in `public/default` because that's KoP's default
`kafkaNamespace` — Kafka clients connecting via SASL_SSL on :9093 can't
read/write topics outside it without explicit routing. The orchestrator's
state + per-filter control topics go under `stock-demo/filtering` for clean
isolation.

## Prereqs

- [terraform](https://developer.hashicorp.com/terraform/downloads) ≥ 1.5
- [snctl](https://docs.streamnative.io/snctl) authenticated to your SN Cloud org (`snctl auth login -O <your-org>`)
- [pulsar-admin](https://pulsar.apache.org/docs/en/admin-api-overview/) on `$PATH` (or set `$PULSAR_ADMIN`)
- A StreamNative service account key JSON downloaded from the SN Cloud console — drop it at `terraform/streamnative-key.json`.

## Running

From the demo root:

```bash
cd infra/terraform
export ORG=<your-sn-cloud-org-id>          # e.g. o-mj3r8
export SELECTOR_REL=v1.0.0              # broker-side-filtering-function release
./recreate.sh
```

Total runtime: ~10–15 minutes on a first run (cluster provisioning dominates).
Re-runs against an existing cluster skip Phases 2–5 and complete in ~30 seconds.
The script prints the .env values you'll need at the end.

`export RESET=1 ./recreate.sh` redeploys the orchestrator function in-place
without touching anything else.

## What lives outside this directory

- `../docker-compose.yml` — runtime services. Read `../README.md` for the
  full bring-up after `recreate.sh` finishes.
- `../bootstrap/bootstrap.sh` — runtime topic-creation safety net. Mostly a
  no-op after terraform has already provisioned everything, but kept as a
  defensive layer for re-deploys.

## Cleanup

```bash
cd infra/terraform
terraform destroy
```

`destroy` removes the cluster + everything in it (~10 minutes). The `*.token`
files on disk are no longer valid after destroy — delete them.
