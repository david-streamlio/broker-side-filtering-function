# Tenant refactor — `sn/broker-filter` control plane + parallel demo SAs

Plan-of-record for moving the orchestrator + its control/state topics out of `stock-demo/filtering` into `sn/broker-filter`, adding a parallel `parcel-demo-app` SDK SA, and renaming the orchestrator-function SA to a demo-neutral identity.

## Goals

1. **Lock down the control plane** — orchestrator, filter-requests, orchestrator-state, and spawned filter functions live in a tenant that's not commonly client-accessible. `sn` is the system tenant (transaction coordinator, audit logs, aliveness probes); adding `broker-filter` as a sibling namespace puts the filtering control plane behind a wall random clients don't normally touch.
2. **Naming consistency** — stock-demo / parcel-demo are SDK-client demos; the orchestrator-function SA shouldn't carry either name. Rename to `broker-filter-orchestrator-function`.
3. **Per-demo SDK identity** — `stock-demo-app` and `parcel-demo-app` as parallel SAs with the same grant shape, so SDK access can be audited + revoked per demo without touching the orchestrator.
4. **One shared orchestrator** — both demos talk to the same `sn/broker-filter/orchestrator`. Queries are namespaced by queryId, not by control topic; isolation is at the SA / grant level.

## Target shape

```
Tenants:
  sn          (data-source; existing system tenant — DO NOT manage in TF)
  stock-demo  (cosmetic / SDK-side; admin_roles=[stock-demo-app])
  parcel-demo (cosmetic / SDK-side; admin_roles=[parcel-demo-app])

Namespaces (TF-managed):
  sn/broker-filter   ← orchestrator + filter-requests + orchestrator-state + spawned filters
                       (replaces stock-demo/filtering)

Service accounts:
  broker-filter-orchestrator-function   ← runs the orchestrator + inherited by spawned filters
  stock-demo-app                        ← stock SDK client (unchanged)
  parcel-demo-app                       ← parcel SDK client (NEW)

Permission grants:
  broker-filter-orchestrator-function:
    sn/broker-filter                                   → functions, packages, produce, consume
    public/default                                     → consume, produce  (read sources, write filtered-*)

  stock-demo-app:
    public/default                                     → produce, consume, packages
    sn/broker-filter/filter-requests                   → produce, consume

  parcel-demo-app:
    public/default                                     → produce, consume, packages
    sn/broker-filter/filter-requests                   → produce, consume
```

Source topics (`stock-quotes`, `parcel-*`) and private outputs (`filtered-<uuid>`) stay in `public/default` because KoP defaults its kafkaNamespace there — moving sources would force per-topic kafkaNamespace overrides, no win.

## File-level changes

### `demo/infra/terraform/`

- **`topics.tf`**
  - Add `pulsar_tenant.parcel_demo` mirroring `pulsar_tenant.stock_demo` (admin_roles=[parcel_demo_app]).
  - Remove `pulsar_namespace.filtering` (was `stock-demo/filtering`).
  - Add `pulsar_namespace.broker_filter` with `tenant = "sn"`, `namespace = "broker-filter"`, same retention/TTL policies as the old `filtering` namespace.
  - Re-point `pulsar_topic.filter_requests` and `pulsar_topic.orchestrator_state` to `sn/broker-filter/*`.
  - Remove orchestrator-function from `pulsar_tenant.stock_demo.admin_roles` — it doesn't own anything in stock-demo anymore.

- **`apikeys.tf`** (or wherever SAs are declared)
  - Rename `streamnative_service_account.orchestrator_function` → `broker_filter_orchestrator_function`. Update its `name = "broker-filter-orchestrator-function"`.
  - Add `streamnative_service_account.parcel_demo_app` mirroring `stock_demo_app`.
  - Add the corresponding `streamnative_apikey` + `pulsar_token` output for `parcel-demo-app.token`.

- **`permissions.tf`**
  - All references to `streamnative_service_account.orchestrator_function` → `.broker_filter_orchestrator_function`.
  - All references to `var.tenant`/`pulsar_namespace.filtering` in the orchestrator grants → `pulsar_namespace.broker_filter` (`sn/broker-filter`).
  - Rename `orchestrator_filter_deploy` namespace to `sn/broker-filter`.
  - Rename `orchestrator_consume_control` topic to `persistent://sn/broker-filter/filter-requests`.
  - Rename `orchestrator_state_{consume,produce}` topics to `persistent://sn/broker-filter/orchestrator-state`.
  - `orchestrator_consume_source` (the merged consume+produce on `public/default` from PR #32) stays as-is.
  - Add `parcel_app_public_default` (mirror of `app_public_default` for the parcel SA).
  - Add `parcel_app_filter_requests` (mirror of `app_filter_requests` re-pointed at the new `sn/broker-filter/filter-requests`).
  - Re-point existing `app_filter_requests` at the new control topic location.

- **`variables.tf`**
  - Default `tenant` stays `stock-demo` (still used for the cosmetic stock-demo project tenant).
  - Add `var.parcel_tenant` default `parcel-demo`.

- **`recreate.sh`**
  - Phase 7 deploys orchestrator with `--tenant sn --namespace broker-filter` instead of `--tenant stock-demo --namespace filtering`.
  - Upload `selector-filter` package URL → `function://sn/broker-filter/selector-filter@$SF_VER`.
  - Write `demo/.env` defaults: `ORCHESTRATOR_TENANT=sn`, `ORCHESTRATOR_NAMESPACE=broker-filter`, `DEMO_CONTROL_TOPIC=persistent://sn/broker-filter/filter-requests`, `STATE_TOPIC=persistent://sn/broker-filter/orchestrator-state`.
  - Write both `app.token` (stock) and `parcel-app.token` files.
  - Update tenant/SA names in any echo / next-steps text.

- **`reset-cloud.sh`**
  - Update `ORCH_TENANT`/`ORCH_NS` defaults sourced from `.env` (already env-driven — likely no change).
  - Topic delete list: re-point control/state to `sn/broker-filter`.
  - Package delete: `sn/broker-filter/selector-filter` instead of `stock-demo/filtering/selector-filter`.
  - `TF_RESOURCES`:
    - Add `pulsar_topic.parcel_*` (already there from PR #30).
    - Add new `pulsar_permission_grant.parcel_app_*` entries.
    - Add `streamnative_apikey.parcel_demo_app` if managed in TF state.
    - Rename `orchestrator_consume_source` to whatever it ends up — keep the name (PR #32 already noted not renaming).

### `demo/`

- **`.env.example`**
  - `ORCHESTRATOR_TENANT=sn`
  - `ORCHESTRATOR_NAMESPACE=broker-filter`
  - `DEMO_CONTROL_TOPIC=persistent://sn/broker-filter/filter-requests`
  - `STATE_TOPIC=persistent://sn/broker-filter/orchestrator-state`
  - Add `PARCEL_APP_TOKEN_PATH=…/parcel-app.token` for the parcels compose to load.

- **`docker-compose.yml`** (stock)
  - No change beyond reading new ORCHESTRATOR_TENANT/NAMESPACE from `.env` (already env-driven).

- **`docker-compose.parcels.yml`**
  - Switch the multi-query-app's token mount from `app.token` → `parcel-app.token`.
  - Same for shadowtraffic.

- **`docker-compose.local.yml` + `docker-compose.parcels.local.yml`**
  - Mirror the env-var defaults (local broker doesn't have an `sn` tenant by default — bootstrap script needs to create the namespace too if we want symmetric local/cloud).

- **`bootstrap/bootstrap.sh`** (local-mode broker bootstrap)
  - Add: create the `sn` tenant if it doesn't exist, then create the `broker-filter` namespace.
  - Update existing topic-creation calls to point at `sn/broker-filter`.

- **`RUNBOOK.md`**
  - Update step 6 (recreate.sh) description: orchestrator now lives in `sn/broker-filter`.
  - Update step 7: note that stock and parcel demos use different SDK tokens (`app.token` vs `parcel-app.token`) — the docker-compose files handle this automatically.
  - Update step 8: admin-API curls now use `sn/broker-filter` not `stock-demo/filtering`.
  - Update step 9: `reset-cloud.sh` behavior unchanged from operator POV.
  - Update step 10 gotchas: note "Tenant `sn` doesn't exist" symptom for orgs without it.

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

# 4. Recreate on the NEW shape — creates sn/broker-filter namespace + parcel-demo-app SA
SELECTOR_REL=v1.1.0 ./recreate.sh

# 5. Bring up a demo as usual
make demo-cloud           # or demo-cloud-parcels
```

Step 3 is required because TF can't auto-migrate a `pulsar_namespace.filtering` (stock-demo/filtering) into `pulsar_namespace.broker_filter` (sn/broker-filter) — different resource address.

## Verification

After the refactor lands and operator follows the migration:

```bash
APP_TOKEN=$(cat demo/infra/terraform/app.token)
BROKER=$(grep PULSAR_ADMIN_URL demo/.env | cut -d= -f2)

# (a) New orchestrator location
curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/sn/broker-filter" | jq
# → ["orchestrator","selector-filter-…","selector-filter-…",…]

# (b) Old location is empty / not found
curl -s -H "Authorization: Bearer $APP_TOKEN" "$BROKER/admin/v3/functions/stock-demo/filtering" | jq
# → "Namespace does not exist" OR []

# (c) Both apps' tokens can REGISTER queries (produce on filter-requests)
PARCEL_TOKEN=$(cat demo/infra/terraform/parcel-app.token)
for tok in "$APP_TOKEN" "$PARCEL_TOKEN"; do
  curl -s -X POST -H "Authorization: Bearer $tok" -H "Content-Type: application/json" \
    -d '{"foo":"bar"}' \
    "$BROKER/topics/persistent/sn/broker-filter/filter-requests"
done
# → both return a body-validation NPE (auth-pass), neither returns "Unauthorized to produce"
```

Observability UI behavior unchanged — operator-facing API is the same, just at a different namespace under the hood.

## Rollback plan

If the migration goes sideways: revert the merge commit on main, `make reset-demo-cloud` (which still works against either shape), and `./recreate.sh` brings up the old `stock-demo/filtering` topology.

## Risks / open questions

1. **`sn` tenant admin permissions** — does the operator's `PULSAR_TOKEN` (the super-admin minted via `snctl auth get-token --login`) have rights to create namespaces in `sn`? If `sn` is locked down by SN Cloud platform policy, this fails at TF apply. **Mitigation**: try it on the live cluster before merging this PR.
2. **SA principals at cluster scope** — SN Cloud SAs are instance-scoped, not tenant-scoped; renaming an SA forces TF to destroy + recreate (new principal string, new JWT). Token files on disk change. The migration step list above accounts for this.
3. **Local-mode `sn` tenant** — the docker-compose local mode's broker doesn't have `sn` pre-created. The bootstrap script needs to create it. Minor addition.

## Status

- [ ] User confirms plan
- [ ] Verify `PULSAR_TOKEN` can create namespaces in `sn` (risk #1) — single curl test before any code changes
- [ ] Implement TF changes (topics.tf, apikeys.tf, permissions.tf, variables.tf)
- [ ] Implement script changes (recreate.sh, reset-cloud.sh, bootstrap.sh)
- [ ] Implement runtime changes (.env.example, compose files)
- [ ] Update RUNBOOK
- [ ] PR + reset + recreate to verify on live cluster
- [ ] Squash-merge
