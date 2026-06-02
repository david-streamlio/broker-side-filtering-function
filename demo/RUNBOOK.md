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
export TF_VAR_cluster_name="stock-demo"         # cluster + instance name; also the stock SA's name prefix → stock-demo-app
export TF_VAR_location="us-central1"            # GCP region
export TF_VAR_parcel_app_prefix="parcel-demo"   # parcels SA's name prefix → parcel-demo-app
export TF_VAR_control_plane_tenant="control-plane"   # locked-down Pulsar tenant for orchestrator + filter-requests + state
export TF_VAR_control_plane_namespace="broker-filter"
```

Both demos run against the **same** cluster + control plane (`control-plane/broker-filter`). The only real Pulsar tenant created is `control-plane` (admin_roles = orchestrator-function SA only); the SDK SAs (`stock-demo-app`, `parcel-demo-app`) don't get their own tenants — they work off namespace-level grants on `public/default` + per-topic grants on `filter-requests`. See `topics.tf` header for why per-demo tenants were considered and rejected (short version: KoP's default kafkaNamespace + Kafka SDK ergonomics).

## 5. Mint the super-admin Pulsar token

For a fresh instance, **always use `--login`** — the key-file mint path requires an Auth0 client-grant that doesn't exist for brand-new instances. (Skipping this is the most common bootstrap failure.)

`snctl auth get-token --login` prints a banner BEFORE the JWT (the verification code + login flow narration). Capturing the whole stdout into `PULSAR_TOKEN` puts that banner in the Authorization header, which curl can't send. Pipe through `grep` to extract just the JWT:

```bash
export PULSAR_TOKEN=$(snctl -O "$TF_VAR_organization" auth get-token "$TF_VAR_cluster_name-instance" --login \
  | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' \
  | tail -1)
```

A browser window opens — sign in with the same SN Cloud account that owns the org. After the browser confirms, the JWT is written to the env var. Verify:

```bash
echo "len=$(printf '%s' "$PULSAR_TOKEN" | wc -c)"   # should print ~1100
printf '%s' "$PULSAR_TOKEN" | head -c 12; echo ""    # should print "eyJhbGciOiJS"
```

`recreate.sh` does the same defensive extraction internally (Phase 5, line ~217) — but you still need a JWT-only `PULSAR_TOKEN` for any direct curl/admin probes you might run later (see step 8).

## 6. Bootstrap the cluster (Terraform + grants + packages + orchestrator)

This is the **shared** path — same script for both demos:

```bash
cd demo/infra/terraform
SELECTOR_REL=v1.2.0 ./recreate.sh
cd ../../..
```

Allow **~15–20 minutes** the first time (most of it is serverless instance provisioning). The script:

1. `terraform apply` — creates the GCP-backed serverless cluster + one Pulsar tenant (`control-plane`, admin_roles = orchestrator-function SA only — the SDK SAs don't have tenants of their own), three service accounts (`stock-demo-app` + `parcel-demo-app` for the SDK clients, `stock-demo-orchestrator-function` for the orchestrator + its spawned filters), the `control-plane/broker-filter` namespace, source topics in `public/default` (stock-quotes + parcel-{shipments,scans,exceptions}), control + state topics in `control-plane/broker-filter`, and all `pulsar_permission_grant` resources.
2. Writes three token files alongside the script: `app.token` (stock-demo-app), `parcel-app.token` (parcel-demo-app), `orchestrator-function.token` — these are the SA-scoped JWTs the demo runtime uses.
3. Sets `kafkaEntryFormat=pulsar_non_batched` per source topic and namespace-default on `public/default`, so KoP-produced Kafka headers surface as Pulsar message properties on the consumer side. (This is what makes JMS selectors like `carrier = 'UPS'` evaluate against actual data.)
4. Downloads + uploads the pinned `selector-filter` NAR as a Pulsar package (`function://control-plane/broker-filter/selector-filter@1.1.0`).
5. Deploys the orchestrator function from the pinned orchestrator NAR into `control-plane/broker-filter`.
6. Writes a `demo/.env` file with all the connection URLs + three token paths the docker-compose files consume:
   - `PULSAR_TOKEN_PATH` (stock-demo SDK)
   - `PARCEL_PULSAR_TOKEN_PATH` (parcels SDK)
   - `ORCHESTRATOR_PULSAR_TOKEN_PATH` (observability UI — needs elevated scope to read `control-plane/broker-filter/orchestrator-state`; SDK tokens can't see that namespace)

   **Upgrade-from-prior-shape gotcha**: if `demo/.env` already exists (you've run this before), recreate.sh writes `demo/.env.generated` *instead* to avoid clobbering operator edits. You'll see a yellow `! demo/.env already exists` line in the recreate output. Swap it in:
   ```bash
   diff demo/.env demo/.env.generated   # review the changes
   mv demo/.env demo/.env.backup
   mv demo/.env.generated demo/.env
   ```

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

### What you should *not* see (and shouldn't worry about)

- **There are no `stock-demo` or `parcel-demo` tenants in the SN Cloud console.** The SDK SAs (`stock-demo-app`, `parcel-demo-app`) are pure identities with no tenant of their own — they work off namespace-level grants on `public/default` + per-topic grants on the `filter-requests` control topic. The string `stock-demo` you see in the SA name is just the cluster_name prefix; `parcel-demo` is the `parcel_app_prefix` label. Neither corresponds to a Pulsar tenant.
- **`control-plane/broker-filter` is not browseable in the SN Cloud console as either SDK SA** — only `orchestrator-function@…` is `admin_roles` on that tenant. Use `orchestrator-function.token` for admin probes against `control-plane/*` (the observability container does exactly this — see step 6's third token path).

### Command-line sanity check (both demos)

```bash
APP_TOKEN=$(cat demo/infra/terraform/app.token)
BROKER=$(grep PULSAR_ADMIN_URL demo/.env | cut -d= -f2)
curl -s -H "Authorization: Bearer $APP_TOKEN" \
  "$BROKER/admin/v3/functions/control-plane/broker-filter" | jq
# Stock:   ["orchestrator","selector-filter-stock-quotes-…"]
# Parcels: ["orchestrator","selector-filter-parcel-exceptions-…",
#           "selector-filter-parcel-scans-…","selector-filter-parcel-shipments-…"]
```

`numUserExceptions` should stay near zero across all filter functions:

```bash
for fn in $(curl -s -H "Authorization: Bearer $APP_TOKEN" \
    "$BROKER/admin/v3/functions/control-plane/broker-filter" \
    | jq -r '.[]|select(startswith("selector-filter"))'); do
  echo "=== $fn ==="
  curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/control-plane/broker-filter/$fn/stats" \
    | jq '{received: .receivedTotal, processed: .processedSuccessfullyTotal, errors: .userExceptionsTotal}'
done
```

## 9. Shut down

Three escalating levels, depending on how much state you want to keep:

### 9a. Soft — stop the demo containers, leave the cluster alone

```bash
make demo-down
```

Tears down whichever compose stack is currently up (stock or parcels). The 30-second graceful stop lets multi-query-app's JVM shutdown hook send `UNREGISTER` for each registered queryId, which the orchestrator processes and tombstones; the spawned filter functions are then GC'd by the idle-sweep within `HEARTBEAT_MAX_STALE_SECONDS + idle-gc-ttl-seconds` (~90s + 10min by default, but the GC sweep runs every 60s so most spawned filters are gone within ~2 minutes).

**Survives**: cluster, tenants, SAs, package store, orchestrator function, source topics, all data already produced. Spawned filter functions get reclaimed asynchronously. `make demo-cloud[-parcels]` from a clean slate brings everything back in ~30 seconds.

Use between switching demos, or when stepping away.

### 9b. Hard — wipe spawned filters + packages + topics + TF state, keep the cluster

```bash
make reset-demo-cloud
```

Implicitly runs `make demo-down` first (graceful UNREGISTER, then SIGKILL), then runs `demo/infra/terraform/reset-cloud.sh`. The reset script:

1. Undeploys the orchestrator function so it can't auto-recreate things mid-reset.
2. Deletes every spawned `selector-filter-*` function in `control-plane/broker-filter`.
3. Deletes every package version under `function://control-plane/broker-filter/selector-filter` + `/orchestrator`.
4. Lists every topic in `control-plane/broker-filter` (catches orchestrator-spawned `ctrl-<input-topic>-<uuid>` runtime artifacts, not just the TF-managed `filter-requests` + `orchestrator-state`) and force-deletes each.
5. Deletes the four source topics (`stock-quotes`, `parcel-{shipments,scans,exceptions}`) and any `filtered-<uuid>` private-output topics in `public/default`.
6. `terraform state rm`s each of those resources so the next `recreate.sh` plans clean CREATEs instead of UPDATE-existing-but-deleted errors.

**Needs `PULSAR_TOKEN`** (super-admin) set in your shell — JWT-only form, see step 5.

**Survives**: cluster, the `control-plane` Pulsar tenant, three SAs (`stock-demo-app`, `parcel-demo-app`, `stock-demo-orchestrator-function`), the three `*.token` files on disk, `pulsar_tenant.control_plane` + `pulsar_namespace.broker_filter` Terraform resources, and the namespace itself on the broker (it ends up empty).

Use when bumping `SELECTOR_REL` to a different release, or when you want a guaranteed-clean slate without burning the cluster.

### 9c. Full teardown — delete the entire instance

```bash
cd demo/infra/terraform && terraform destroy
```

Destroys the GCP-backed serverless instance + cluster + everything inside it. Survives: nothing in the demo's TF state; the `streamnative-key.json` and `*.token` files on disk are left behind for the next `recreate.sh`.

Use when you're done with the demo entirely, or before a major instance-size change that requires a fresh provision.

### 9d. Manual escape hatch if `reset-demo-cloud` leaves something stuck

The four-step diagnostic pattern, in order of likelihood, if `make reset-demo-cloud` finishes but the next `recreate.sh` errors with `Cannot delete non empty namespace` or `409 already exists`:

```bash
HOST="$(grep PULSAR_ADMIN_URL demo/.env | cut -d= -f2)"
PULSAR_TOKEN=$(snctl -O <org> auth get-token <cluster>-instance --login \
  | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' | tail -1)

# 1. Inventory what's actually in the namespace
curl -s -H "Authorization: Bearer $PULSAR_TOKEN" \
  "$HOST/admin/v2/persistent/control-plane/broker-filter"          # topics
curl -s -H "Authorization: Bearer $PULSAR_TOKEN" \
  "$HOST/admin/v3/packages/function/control-plane/broker-filter"   # package names

# 2. Delete leftover topics (most common cause: orchestrator-spawned ctrl-* topics
#    that pre-PR #34 reset-cloud.sh didn't know about)
curl -s -H "Authorization: Bearer $PULSAR_TOKEN" \
  "$HOST/admin/v2/persistent/control-plane/broker-filter" \
  | jq -r '.[]' | while read t; do
  curl -sw "  $t -> HTTP %{http_code}\n" -X DELETE \
    -H "Authorization: Bearer $PULSAR_TOKEN" "$HOST${t/persistent:\///admin/v2/persistent/}?force=true"
done

# 3. Delete leftover package versions
for pkg in orchestrator selector-filter; do
  for v in $(curl -s -H "Authorization: Bearer $PULSAR_TOKEN" \
    "$HOST/admin/v3/packages/function/control-plane/broker-filter/$pkg" \
    | jq -r '.[]?'); do
    curl -sw "  $pkg@$v -> HTTP %{http_code}\n" -X DELETE \
      -H "Authorization: Bearer $PULSAR_TOKEN" \
      "$HOST/admin/v3/packages/function/control-plane/broker-filter/$pkg/$v"
  done
done

# 4. Retry namespace delete (force=true is rejected by SN Cloud's
#    forceDeleteNamespaceAllowed=false; the non-force form needs the namespace
#    to be truly empty, which steps 1-3 just ensured)
curl -sw "  HTTP %{http_code}\n" -X DELETE \
  -H "Authorization: Bearer $PULSAR_TOKEN" \
  "$HOST/admin/v2/namespaces/control-plane/broker-filter"
# expect HTTP 204

# 5. Sync TF state
cd demo/infra/terraform
terraform state rm pulsar_namespace.broker_filter 2>/dev/null
```

Then `./recreate.sh` resumes cleanly.

## 10. Common gotchas

- **`curl` returns `HTTP 000` with `$PULSAR_TOKEN`**: the snctl banner contaminated the token. Re-mint with the JWT-extraction pipe in step 5, OR rescue an existing var with:
  ```bash
  PULSAR_TOKEN=$(printf '%s' "$PULSAR_TOKEN" \
    | grep -oE 'eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+' \
    | tail -1)
  ```
- **Token expired** mid-session (24h default): re-mint with the JWT-extraction pipe from step 5.
- **`recreate.sh` errors with "instance still provisioning"**: fresh GCP-backed instances take 10–15 min to come up; the script retries 30× at 60s. Walk away and come back.
- **Filters show `back-off restarting`**: confirm `SELECTOR_REL` is a real release tag (not a SNAPSHOT) and that you ran `make reset-demo-cloud` before bumping versions.
- **Filters RUNNING but no matches**: was the four-layer onion historically debugged on 2026-05-28 (PRs #29 → #30 → #31 → #32). All four fixes are now in `main`; if you see this on `main`, the symptom is something new — start from `latestUserExceptions[0]` on `/admin/v3/functions/.../status`.
- **`recreate.sh` Phase 7 hits `unbound variable`**: you're on a stale checkout pre-PR #31; pull `main`.
- **Parcels demo filters spawn but never match**: you're on a stale checkout pre-PR #29 / #30 / #32; pull `main`.
- **Doc refers to `stock-demo/filtering` but commands say `control-plane/broker-filter`**: you're on a stale checkout pre-tenant-refactor; pull `main`.
