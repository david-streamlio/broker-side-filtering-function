# Tenant refactor — `control-plane/broker-filter` control plane + parallel demo SAs

Plan-of-record for moving the orchestrator + its control/state topics out of `stock-demo/filtering` into `control-plane/broker-filter`, adding a parallel `parcel-demo-app` SDK SA, and renaming the orchestrator-function SA to a demo-neutral identity.

## Goals

1. **Lock down the control plane** — orchestrator, filter-requests, orchestrator-state, and spawned filter functions live in `control-plane/broker-filter` — a new TF-managed tenant + namespace dedicated to the filtering control plane. SDK clients have explicit per-topic grants (consume + produce on `filter-requests` only); they can't reach the orchestrator-state topic, the spawned filters, or the package store. Naming `control-plane` (vs the original `sn` proposal) sidesteps the SN Cloud system-tenant policy entirely — we fully own this tenant.
2. **Naming consistency** — stock-demo / parcel-demo are SDK-client demos. The orchestrator-function SA *ideally* shouldn't carry either name, but renaming an SA forces a destroy+recreate with a new JWT subject — too much operational pain (token rotation) for cosmetics. **Decision (2026-05-28): keep `stock-demo-orchestrator-function` as-is for this PR.** Future PR can rename standalone.
3. **Per-demo SDK identity** — `stock-demo-app` and `parcel-demo-app` as parallel SAs with the same grant shape, so SDK access can be audited + revoked per demo without touching the orchestrator.
4. **One shared orchestrator** — both demos talk to the same `control-plane/broker-filter/orchestrator`. Queries are namespaced by queryId, not by control topic; isolation is at the SA / grant level.

## Target shape

```
Tenants:
  control-plane          (NEW; admin_roles=[stock-demo-orchestrator-function])
  stock-demo  (cosmetic / SDK-side; admin_roles=[stock-demo-app])
  parcel-demo (cosmetic / SDK-side; admin_roles=[parcel-demo-app])

Namespaces (TF-managed):
  control-plane/broker-filter   ← orchestrator + filter-requests + orchestrator-state + spawned filters
                       (replaces stock-demo/filtering)

Service accounts:
  stock-demo-orchestrator-function      ← runs the orchestrator + inherited by spawned filters
                                          (name kept as-is; legacy from when stock was the only demo)
  stock-demo-app                        ← stock SDK client (unchanged)
  parcel-demo-app                       ← parcel SDK client (NEW)

Permission grants:
  stock-demo-orchestrator-function:
    control-plane/broker-filter                                   → functions, packages, produce, consume
    public/default                                     → consume, produce  (read sources, write filtered-*)

  stock-demo-app:
    public/default                                     → produce, consume, packages
    control-plane/broker-filter/filter-requests                   → produce, consume

  parcel-demo-app:
    public/default                                     → produce, consume, packages
    control-plane/broker-filter/filter-requests                   → produce, consume
```

Source topics (`stock-quotes`, `parcel-*`) and private outputs (`filtered-<uuid>`) stay in `public/default` because KoP defaults its kafkaNamespace there — moving sources would force per-topic kafkaNamespace overrides, no win.

## File-level changes

### `demo/infra/terraform/`

- **`topics.tf`**
  - Add `pulsar_tenant.control_plane` (NEW) — `admin_roles = [stock-demo-orchestrator-function@<domain>]`, `allowed_clusters = [var.cluster_name]`. The orchestrator-function SA gets full tenant-admin perms in `control-plane/*`, which covers everything it needs to operate the control plane.
  - Add `pulsar_tenant.parcel_demo` mirroring `pulsar_tenant.stock_demo` (admin_roles=[parcel_demo_app]).
  - Remove `pulsar_namespace.filtering` (was `stock-demo/filtering`).
  - Add `pulsar_namespace.broker_filter` with `tenant = pulsar_tenant.control_plane.tenant`, `namespace = "broker-filter"`, same retention/TTL policies as the old `filtering` namespace.
  - Re-point `pulsar_topic.filter_requests` and `pulsar_topic.orchestrator_state` to `control-plane/broker-filter/*`.
  - Remove orchestrator-function from `pulsar_tenant.stock_demo.admin_roles` — it doesn't own anything in stock-demo anymore.

- **`permissions.tf`** (SAs + bindings + rolebindings live here)
  - Add `streamnative_service_account.parcel_demo_app` mirroring `stock_demo_app`.
  - Add `streamnative_service_account_binding.parcel_demo_app` mirroring `stock_demo_app`.
  - Add `streamnative_rolebinding.parcel_demo_app_consumer_group_reader` + `_namespace_operator` (the KoP-side cluster role bindings needed for Kafka produce + group ops).
  - `streamnative_service_account.orchestrator_function` name stays `stock-demo-orchestrator-function` per design decision above.
- **`apikeys.tf`**
  - Add `streamnative_apikey.parcel_demo_app` + corresponding `apikey_tokens.parcel_app` output for `parcel-demo-app.token`.

- **`permissions.tf`** (grant resources, continued)
  - Existing orchestrator-function SA is now `admin_roles` of the `control-plane` tenant (see `topics.tf` change above), so most of the explicit orchestrator grants in `control-plane/broker-filter` are redundant. **Decision**: drop `orchestrator_filter_deploy`, `orchestrator_consume_control`, `orchestrator_state_consume`, `orchestrator_state_produce`. Tenant-admin covers all of those for the same SA.
  - `orchestrator_consume_source` (the merged consume+produce on `public/default` from PR #32) stays as-is — `public/default` is not the orchestrator's tenant, so explicit grants still required there.
  - Add `parcel_app_public_default` (mirror of `app_public_default` for the parcel SA — produce+consume+packages on `public/default`).
  - Re-point existing `app_filter_requests` to `persistent://control-plane/broker-filter/filter-requests`.
  - Add `parcel_app_filter_requests` (mirror of the new `app_filter_requests` for parcel-demo-app).

- **`variables.tf`**
  - Default `tenant` stays `stock-demo` (still used for the cosmetic stock-demo project tenant).
  - Add `var.parcel_tenant` default `parcel-demo`.

- **`recreate.sh`**
  - Phase 7 deploys orchestrator with `--tenant control-plane --namespace broker-filter` instead of `--tenant stock-demo --namespace filtering`.
  - Upload `selector-filter` package URL → `function://control-plane/broker-filter/selector-filter@$SF_VER`.
  - Write `demo/.env` defaults: `ORCHESTRATOR_TENANT=control-plane`, `ORCHESTRATOR_NAMESPACE=broker-filter`, `DEMO_CONTROL_TOPIC=persistent://control-plane/broker-filter/filter-requests`, `STATE_TOPIC=persistent://control-plane/broker-filter/orchestrator-state`.
  - Write both `app.token` (stock) and `parcel-app.token` files.
  - Update tenant/SA names in any echo / next-steps text.

- **`reset-cloud.sh`**
  - Update `ORCH_TENANT`/`ORCH_NS` defaults sourced from `.env` (already env-driven — likely no change).
  - Topic delete list: re-point control/state to `control-plane/broker-filter`.
  - Package delete: `control-plane/broker-filter/selector-filter` instead of `stock-demo/filtering/selector-filter`.
  - `TF_RESOURCES`:
    - Add `pulsar_topic.parcel_*` (already there from PR #30).
    - Add new `pulsar_permission_grant.parcel_app_*` entries.
    - Add `streamnative_apikey.parcel_demo_app` if managed in TF state.
    - Rename `orchestrator_consume_source` to whatever it ends up — keep the name (PR #32 already noted not renaming).

### `demo/`

- **`.env.example`**
  - `ORCHESTRATOR_TENANT=control-plane`
  - `ORCHESTRATOR_NAMESPACE=broker-filter`
  - `DEMO_CONTROL_TOPIC=persistent://control-plane/broker-filter/filter-requests`
  - `STATE_TOPIC=persistent://control-plane/broker-filter/orchestrator-state`
  - Add `PARCEL_APP_TOKEN_PATH=…/parcel-app.token` for the parcels compose to load.

- **`docker-compose.yml`** (stock)
  - No change beyond reading new ORCHESTRATOR_TENANT/NAMESPACE from `.env` (already env-driven).

- **`docker-compose.parcels.yml`**
  - Switch the multi-query-app's token mount from `app.token` → `parcel-app.token`.
  - Same for shadowtraffic.

- **`docker-compose.local.yml` + `docker-compose.parcels.local.yml`**
  - Mirror the env-var defaults (local broker doesn't have an `control-plane` tenant by default — bootstrap script needs to create the namespace too if we want symmetric local/cloud).

- **`bootstrap/bootstrap.sh`** (local-mode broker bootstrap)
  - Add: create the `control-plane` tenant if it doesn't exist, then create the `broker-filter` namespace.
  - Update existing topic-creation calls to point at `control-plane/broker-filter`.

- **`RUNBOOK.md`**
  - Update step 6 (recreate.sh) description: orchestrator now lives in `control-plane/broker-filter`.
  - Update step 7: note that stock and parcel demos use different SDK tokens (`app.token` vs `parcel-app.token`) — the docker-compose files handle this automatically.
  - Update step 8: admin-API curls now use `control-plane/broker-filter` not `stock-demo/filtering`.
  - Update step 9: `reset-cloud.sh` behavior unchanged from operator POV.
  - Update step 10 gotchas: note "Tenant `control-plane` doesn't exist" symptom for orgs without it.

### `Makefile`

- No change — all demo targets already read tenants from `demo/.env`.

## Migration path (operator-facing)

Anyone running the existing demo needs to:

```bash
# 1. Pull this refactor
git pull origin main

# 2. Hard-reset on the OLD shape (deletes orchestrator + spawned filters + old packages + old grants)
make reset-demo-cloud

# 3. terraform state rm the resources that no longer exist
cd demo/infra/terraform
terraform state rm pulsar_namespace.filtering   # gone
# (the old orchestrator-state + filter-requests TF state addresses unchanged; their .topic value just moved)

# 4. Recreate on the NEW shape — creates control-plane/broker-filter namespace + parcel-demo-app SA
SELECTOR_REL=v1.1.0 ./recreate.sh

# 5. Bring up a demo as usual
make demo-cloud           # or demo-cloud-parcels
```

Step 3 is required because TF can't auto-migrate a `pulsar_namespace.filtering` (stock-demo/filtering) into `pulsar_namespace.broker_filter` (control-plane/broker-filter) — different resource address.

## Verification

After the refactor lands and operator follows the migration:

```bash
APP_TOKEN=$(cat demo/infra/terraform/app.token)
BROKER=$(grep PULSAR_ADMIN_URL demo/.env | cut -d= -f2)

# (a) New orchestrator location
curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/control-plane/broker-filter" | jq
# → ["orchestrator","selector-filter-…","selector-filter-…",…]

# (b) Old location is empty / not found
curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/stock-demo/filtering" | jq
# → "Namespace does not exist" OR []

# (c) Both apps' tokens can REGISTER queries (produce on filter-requests)
PARCEL_TOKEN=$(cat demo/infra/terraform/parcel-app.token)
for tok in "$APP_TOKEN" "$PARCEL_TOKEN"; do
  curl -s -X POST -H "Authorization: Bearer $tok" -H "Content-Type: application/json" \
    -d '{"foo":"bar"}' \
    "$BROKER/topics/persistent/control-plane/broker-filter/filter-requests"
done
# → both return a body-validation NPE (auth-pass), neither returns "Unauthorized to produce"
```

Observability UI behavior unchanged — operator-facing API is the same, just at a different namespace under the hood.

## Rollback plan

If the migration goes sideways: revert the merge commit on main, `make reset-demo-cloud` (which still works against either shape), and `./recreate.sh` brings up the old `stock-demo/filtering` topology.

## Risks / open questions

1. **Stale `sn/broker-filter` namespace from earlier diagnostic** — the operator manually `PUT`-created `sn/broker-filter` while we were investigating whether `sn` was usable; that namespace is empty but present on the broker. Since we pivoted to `control-plane`, the stray `sn/broker-filter` should be DELETE'd before the migration to keep the cluster clean. One curl with the super-admin token:
   ```bash
   curl -s -X DELETE -H "Authorization: Bearer $PULSAR_TOKEN" \
     "$PULSAR_ADMIN_URL/admin/v2/namespaces/sn/broker-filter?force=true"
   ```
2. **Local-mode `control-plane` tenant** — `bootstrap.sh` currently creates the `stock-demo` tenant for local-mode standalone brokers. Needs to also create the `control-plane` tenant + the `broker-filter` namespace before the orchestrator is deployed locally. Mirror of the `stock-demo/filtering` setup.
3. **TF state migration** — `pulsar_namespace.filtering` is removed and `pulsar_namespace.broker_filter` is added at a different resource address. TF will plan a destroy of the old + create of the new. That's fine for fresh clusters; for in-flight cluster operators the migration steps already account for it (`make reset-demo-cloud` first).

## Status

- [x] User confirmed plan (one shared orchestrator, per-demo SDK SAs, `control-plane/broker-filter` control plane — locked in 2026-05-28)
- [x] Sanity-check: PUT against the cluster returns 204 (verified 2026-05-28; namespace-create perms confirmed)
- [x] Implement TF changes (topics.tf, apikeys.tf, permissions.tf, variables.tf) — commit 2a100f3
- [x] Implement script changes (recreate.sh, reset-cloud.sh, orchestrator-function-config.yaml) — commit f216513
- [x] Implement runtime changes (.env.example, compose files) — commit 85b0a96
- [x] Update RUNBOOK — this commit
- [ ] PR + reset + recreate to verify on live cluster
- [ ] Squash-merge
