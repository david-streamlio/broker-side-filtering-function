# Cloud Demo Runbook

End-to-end setup for running the broker-side filtering demos against a fresh StreamNative Cloud cluster. Two flavors share the same bootstrap path:

- **Stock demo** (`make demo-cloud`) — single source topic (`stock-quotes`), spawns one filter function.
- **Parcels demo** (`make demo-cloud-parcels`) — three source topics (`parcel-shipments`, `parcel-scans`, `parcel-exceptions`), spawns three filter functions; exercises multi-topic queries.

Both demos run against the **same** cluster, tenant, and orchestrator — you only swap the docker-compose stack on top.

---

## 1. Prerequisites

| Tool | Purpose | Install |
| --- | --- | --- |
| `terraform` ≥ 1.5 | Provisions the SN Cloud cluster + Pulsar topics, SAs, grants | `brew install terraform` |
| `snctl` | Mints SN Cloud tokens, manages cluster resources | https://docs.streamnative.io/docs/install-snctl |
| `gh` CLI | Downloads pinned NAR releases from GitHub | `brew install gh` then `gh auth login` |
| Docker + Compose | Runs shadowtraffic + multi-query-app + observability UI | Docker Desktop |
| `jq`, `curl`, `bash` | Glue used by the bootstrap scripts | macOS default |

You also need an **SN Cloud organization** with a paid serverless-instance entitlement (or trial access).

## 2. Clone and sync

```bash
git clone https://github.com/david-streamlio/broker-side-filtering-function.git
cd broker-side-filtering-function
git fetch origin main
git checkout main
git pull --ff-only origin main
```

## 3. Get your SN Cloud key file

In the SN Cloud console:

1. **Settings → Service Accounts** — create one (or reuse one with `super-user` role on your org).
2. Click the SA → **Create new API key** → choose **OAuth2 (key file)**.
3. Download the JSON file.
4. **Save it as `demo/infra/terraform/streamnative-key.json`** — exactly that name, exactly that location. The bootstrap scripts hard-code the path; the file is in `.gitignore`.

Read your org slug out of the file once it's in place:

```bash
jq -r '.client_email' demo/infra/terraform/streamnative-key.json
# → <something>@<ORG-SLUG>.auth.streamnative.cloud
```

The `ORG-SLUG` between `@` and `.auth` is what you'll export next. Typical shape: `o-abc1234`.

## 4. Set required env vars

```bash
export TF_VAR_organization="o-abc1234"          # your org slug from step 3
# Optional overrides — defaults are fine for most users:
export TF_VAR_cluster_name="stock-demo"         # also used as instance name; keep "stock-demo" for both demos
export TF_VAR_location="us-central1"            # GCP region
export TF_VAR_tenant="stock-demo"               # Pulsar tenant used for filtering/* topics
```

Note: the cluster/tenant default is `stock-demo` for *both* demos — the parcels demo just adds extra topics inside the same cluster. Don't change the prefix unless you understand the spawned-filter name conventions.

## 5. Mint the super-admin Pulsar token

For a fresh instance, **always use `--login`** — the key-file mint path requires an Auth0 client-grant that doesn't exist for brand-new instances. (Skipping this is the most common bootstrap failure.)

```bash
export PULSAR_TOKEN=$(snctl -O "$TF_VAR_organization" auth get-token "$TF_VAR_cluster_name-instance" --login)
```

A browser window opens — sign in with the same SN Cloud account that owns the org. After the browser confirms, the JWT is written to the env var. Verify:

```bash
echo "$PULSAR_TOKEN" | head -c 8   # should print "eyJhbGci"
```

## 6. Bootstrap the cluster (Terraform + grants + packages + orchestrator)

This is the **shared** path — same script for both demos:

```bash
cd demo/infra/terraform
SELECTOR_REL=v1.1.0 ./recreate.sh
cd ../../..
```

Allow **~15–20 minutes** the first time (most of it is serverless instance provisioning). The script:

1. `terraform apply` — creates the GCP-backed serverless cluster, service accounts (`stock-demo-app` + `stock-demo-orchestrator-function`), `stock-demo/filtering` namespace, source topics in `public/default` (stock-quotes + parcel-{shipments,scans,exceptions}) and control/state topics in `stock-demo/filtering`, and all `pulsar_permission_grant` resources.
2. Writes `app.token` and `orchestrator-function.token` files alongside the script — these are the SA-scoped JWTs the demo runtime uses.
3. Sets `kafkaEntryFormat=pulsar_non_batched` per source topic and namespace-default on `public/default`, so KoP-produced Kafka headers surface as Pulsar message properties on the consumer side. (This is what makes JMS selectors like `carrier = 'UPS'` evaluate against actual data.)
4. Downloads + uploads the pinned `selector-filter` NAR as a Pulsar package (`function://stock-demo/filtering/selector-filter@1.1.0`).
5. Deploys the orchestrator function from the pinned orchestrator NAR.
6. Writes a `demo/.env` file with all the connection URLs and token paths the docker-compose files consume.

When it exits, you should see `🎉 Bootstrap complete` and a list of next-step commands.

## 7. Bring up a demo stack

Pick **one** — they share the same cluster, so only one can run at a time. (To switch, `make demo-down` first.)

### 7a. Stock demo

```bash
make demo-cloud
```

Starts (detached) the local containers:
- **shadowtraffic** — produces synthetic stock-quote records via Kafka protocol into KoP
- **multi-query-app** — registers ~3 queries from `demo/multi-query-app/queries-cloud.yaml` against `stock-quotes`
- **observability UI** — http://localhost:3200

The orchestrator spawns **one** `selector-filter-stock-quotes-*` function on the cloud cluster.

### 7b. Parcels demo

```bash
make demo-cloud-parcels
```

Same containers, different shadowtraffic + query config:
- **shadowtraffic** — produces records into three Kafka topics (`parcel-shipments`, `parcel-scans`, `parcel-exceptions`)
- **multi-query-app** — registers 9 queries from `demo/multi-query-app/queries-parcels.yaml`, including multi-topic queries that fan out across all three source topics
- **observability UI** — http://localhost:3200

The orchestrator spawns **three** `selector-filter-parcel-{shipments,scans,exceptions}-*` functions on the cloud cluster.

Wait ~60 seconds for shadowtraffic to warm and the spawned filter functions to reach RUNNING.

## 8. Verify

Open the UI: **http://localhost:3200**.

### Stock demo expected state

- **Spawned Filters**: 1 row (`selector-filter-stock-quotes-*`), `RUNNING`, non-zero IN and MATCH rates.
- **Active Queries**: 3, each with non-zero `MATCHED` and `DROP %` matching its selector's selectivity.

### Parcels demo expected state

- **Spawned Filters**: 3 rows (one per source topic), all `RUNNING`, non-zero IN and MATCH rates.
- **Active Queries**: 9 — each with `MATCHED` > 0 and `DROP %` close to its expected selectivity:

  | Query | Selector | Expected drop |
  | --- | --- | --- |
  | `all-na-scans` | `origin_region IN ('NA_WEST','NA_EAST')` | ~40% |
  | `fedex-exceptions` | `carrier = 'FEDEX'` | ~75% |
  | `business-tier-feed` | `customer_tier = 'BUSINESS'` (multi-topic) | ~75% |
  | `eu-bound-shipments` | `dest_region = 'EU'` | ~80% |
  | `apac-origin-scans` | `origin_region = 'APAC'` | ~85% |
  | `last-mile-scans` | `scan_type IN ('OUT_FOR_DELIVERY','DELIVERED')` | ~85% |
  | `ups-priority` | `carrier = 'UPS' AND service_level IN ('EXPRESS','OVERNIGHT')` | ~89% |
  | `dispatch-watchlist` | `__key__ IN ('TRK000001',…)` (multi-topic) | ~90% |
  | `critical-exceptions` | `severity = 'CRITICAL'` | ~95% |

### Command-line sanity check (both demos)

```bash
APP_TOKEN=$(cat demo/infra/terraform/app.token)
BROKER=$(grep PULSAR_ADMIN_URL demo/.env | cut -d= -f2)
curl -s -H "Authorization: Bearer $APP_TOKEN" \
  "$BROKER/admin/v3/functions/stock-demo/filtering" | jq
# Stock:   ["orchestrator","selector-filter-stock-quotes-…"]
# Parcels: ["orchestrator","selector-filter-parcel-exceptions-…",
#           "selector-filter-parcel-scans-…","selector-filter-parcel-shipments-…"]
```

`numUserExceptions` should stay near zero across all filter functions:

```bash
for fn in $(curl -s -H "Authorization: Bearer $APP_TOKEN" \
    "$BROKER/admin/v3/functions/stock-demo/filtering" \
    | jq -r '.[]|select(startswith("selector-filter"))'); do
  echo "=== $fn ==="
  curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/stock-demo/filtering/$fn/stats" \
    | jq '{received: .receivedTotal, processed: .processedSuccessfullyTotal, errors: .userExceptionsTotal}'
done
```

## 9. Shut down

**Soft** (cluster stays, spawned filters auto-GC'd within ~90s):
```bash
make demo-down
```
Use this between flipping between stock and parcels, or when stepping away. `make demo-down` tears down whichever compose stack is currently up. The cluster, orchestrator, and source topics persist; spawned filters get tombstoned via the SDK's UNREGISTER → orchestrator idle-GC chain.

**Hard** (wipes spawned filters, packages, source/state/private-output topics + clears their TF state — needs `PULSAR_TOKEN` set):
```bash
make reset-demo-cloud
```
Implicitly runs `make demo-down` first, then `reset-cloud.sh`. Use this when bumping `SELECTOR_REL` or starting completely fresh. The cluster, tenant, and SAs themselves persist — those are owned by the Terraform root state, not the reset script.

**Full teardown** (delete the entire instance):
```bash
cd demo/infra/terraform && terraform destroy
```

## 10. Common gotchas

- **Token expired** mid-session (24h default): re-run `snctl auth get-token … --login` and re-export `PULSAR_TOKEN`.
- **`recreate.sh` errors with "instance still provisioning"**: fresh GCP-backed instances take 10–15 min to come up; the script retries 30× at 60s. Walk away and come back.
- **Filters show `back-off restarting`**: confirm `SELECTOR_REL` is a real release tag (not a SNAPSHOT) and that you ran `make reset-demo-cloud` before bumping versions.
- **Filters RUNNING but no matches**: was the four-layer onion historically debugged on 2026-05-28 (PRs #29 → #30 → #31 → #32). All four fixes are now in `main`; if you see this on `main`, the symptom is something new — start from `latestUserExceptions[0]` on `/admin/v3/functions/.../status`.
- **`recreate.sh` Phase 7 hits `unbound variable`**: you're on a stale checkout pre-PR #31; pull `main`.
- **Parcels demo filters spawn but never match**: you're on a stale checkout pre-PR #29 / #30 / #32; pull `main`.
