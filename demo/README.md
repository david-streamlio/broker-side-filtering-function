# Multi-Query Stock-Quote Demo

A scripted showcase that exercises **all** of the broker-side filter's capabilities — multiple simultaneous queries on a shared source topic, dynamic selector diversity, intentional bad selectors that trip the orchestrator's diagnostic flags — and watches the whole thing through the
[broker-side-filter-observability](https://github.com/david-streamlio/broker-side-filter-observability) UI.

Unlike the TradeWise demo (which registers a single key-IN watchlist), this one runs **9 concurrent queries** on the same `stock-quotes` source so the orchestrator spawns its filter, registry, drop-% accounting, and per-query control-topic state in their full configuration.

## What it shows

- **Synthetic load** — [ShadowTraffic](https://shadowtraffic.io/) generating ~200 stock-quote events/sec across 43 tickers, 6 sectors, 2 exchanges.
- **9 selector queries** — each picking a different slice (sectors, value bands, large-block trades, bad-syntax demo) so per-filter drop %s span from ~10% (`high-value-stocks`) to ~92% (`buy-side-finance`).
- **One source topic feeds many filters** — the orchestrator demultiplexes onto private output topics, one per query.
- **Diagnostic-flag triggers** — `bad-syntax-demo` deliberately fires `selector-compile-fail` so operators see the UI surface a real fault on first scan.
- **Drop % stays smooth** — the hero banner shows ~80%+ aggregate savings using cumulative counters (not the noisy 5s delta).

## Layout

```
demo/
  README.md                            ← this file
  docker-compose.yml                   ← cloud-mode: bootstrap + shadowtraffic + multi-query-app + observability against SN Cloud
  docker-compose.local.yml             ← local-mode: pulsar-standalone + orchestrator-deploy + the four runtime services
  bootstrap/
    bootstrap.sh                       ← pre-flight (topics + permissions); auto-detects auth mode from PULSAR_TOKEN_FILE
  local/
    orchestrator-deploy.sh             ← downloads + uploads NARs + deploys orchestrator function (local-mode only)
    orchestrator-function-config.yaml  ← function config (tenant=public, ns=default, no admin token)
  shadowtraffic/
    stock-quotes.json                  ← generator config (templated bootstrap/topic/throttle)
  multi-query-app/
    pom.xml                            ← parent-reactor child; produces a shaded jar
    queries.yaml                       ← the 9 selectors (edit freely; container hot-loads on restart)
    Dockerfile                         ← two-stage Maven → JRE 17
    src/main/java/io/streamnative/filter/demo/
      MultiQueryDemo.java              ← entrypoint; one drain-loop thread per query
      QueryConfig.java                 ← YAML loader
  infra/
    README.md                          ← SN Cloud infrastructure replication (terraform-based)
    terraform/                         ← cluster + tenant + SAs + apikeys + orchestrator deploy
    pulsar-functions/orchestrator-function-config.yaml  ← cloud-mode function config (uses pulsar-admin-token)
```

## Cloud-mode vs local-mode

Two ways to run the same demo content:

| | **Cloud-mode** (`docker-compose.yml`) | **Local-mode** (`docker-compose.local.yml`) |
|---|---|---|
| Pulsar | SN Cloud serverless cluster (real auth, KoP on SASL_SSL :9093) | `streamnative/sn-pulsar:4.0.5` standalone, KoP PLAINTEXT :9092, anonymous auth |
| Bring-up | `cd infra/terraform && ./recreate.sh` first, then `docker compose up` | `docker compose -f docker-compose.local.yml up --build` |
| Cluster cost | Real SN Cloud charges while running | Free; runs on your laptop |
| Exercises | The auth + RBAC chain that production hits | Only the runtime application logic |
| Use when | Validating end-to-end against real cluster semantics | Fast iteration on UI / multi-query app / queries.yaml |

`bootstrap.sh` + `MultiQueryDemo.java` + the observability UI auto-detect the auth mode from the absence of `PULSAR_TOKEN_FILE` / `KAFKA_PASSWORD` env vars — same images run in both modes.

## Bootstrap service

The first thing `docker compose up` runs is the **`bootstrap`** service — a one-shot
container that uses `pulsar-admin` to:

1. Create the destination topic (partitioned, partition count from `DEMO_SOURCE_PARTITIONS`)
2. Create the control topic (non-partitioned)
3. Grant `produce + consume` to the demo SA on the source-topic and control-topic namespaces
4. Enable auto-topic-creation on the source namespace (so the orchestrator's spawned-filter `filtered-*` private topics auto-create)

Idempotent — all four steps treat "already exists" / "already granted" as success. shadowtraffic + multi-query-app `depends_on` it with `condition: service_completed_successfully`, so the rest of the stack doesn't start until the cluster's ready.

## Prerequisites

- A Pulsar cluster (typically SN Cloud) with KoP enabled and the broker-side-filtering-function orchestrator deployed — see the root README's bring-up section.
- An SA token with permission to create topics + grant permissions on the source-topic namespace (the bootstrap step needs this), AND `consume + produce` on the same namespaces at runtime. Typically a single `tradewise-backend`-style SA bound to the `namespace-operator` ClusterRole covers both.
- A [ShadowTraffic license](https://shadowtraffic.io/account) — free tier is sufficient (capped at ~1000 msg/s).
- Docker + docker-compose v2.

## Running (local-mode)

Fastest path — no SN Cloud account needed:

```bash
export SHADOWTRAFFIC_LICENSE_ID=<your-id>
export SHADOWTRAFFIC_LICENSE_KEY=<your-key>
docker compose -f docker-compose.local.yml up --build
```

Pulsar standalone takes ~30s to come up; orchestrator-deploy + bootstrap run in series after that (~30s more); then shadowtraffic + multi-query-app + observability start. Open <http://localhost:3200> when the observability container is healthy.

## Running (cloud-mode)

Copy `.env.example` to `.env` and fill in your cluster details:

```bash
cp .env.example .env
# edit .env: PULSAR_SERVICE_URL, KAFKA_USERNAME, PULSAR_TOKEN_PATH, etc.
```

Then:

```bash
docker compose up --build
```

Within ~20 seconds:

1. ShadowTraffic starts pushing stock-quote events into `stock-quotes`.
2. `multi-query-app` boots, registers the 9 queries through the SDK, logs the SDK-assigned queryIds (correlate them with the YAML `id` field).
3. The observability UI lights up at <http://localhost:3200>:
   - Hero banner: ~200 msg/s source → ~80 msg/s forwarded, ~60% global drop.
   - Spawned-filters table: one filter (all queries share an input topic) running with 9 active query IDs.
   - Active-queries table: 8 healthy rows + 1 row with the `selector-compile-fail` diagnostic.
   - TopicDetail on `stock-quotes` shows real messages via the peek endpoint.

## Editing the queries

`queries.yaml` is mounted into the container — edit and `docker compose restart multi-query-app` to re-register. Each entry is `{ id, selector }`:

```yaml
queries:
  - id: my-custom-slice
    selector: "sector = 'tech' AND price BETWEEN 100 AND 250"
```

Selector syntax is JMS-style. The orchestrator's selector-filter parser is the canonical evaluator — anything it rejects fires `selector-compile-fail` and surfaces in the observability UI's Diagnostic Flags panel.

## Chaos demo (planned, not implemented yet)

A `--kill-after=30s` flag on `multi-query-app` will be added in a follow-up to randomly close one consumer mid-run, demonstrating the `private-topic-no-consumer` diagnostic.

## Cleanup

```bash
docker compose down
```

The SDK fires UNREGISTER for every queryId on SIGTERM, so the orchestrator tombstones the matching registry entries within ~1 sweep interval. Verify with `snctl pulsar admin topics list <tenant>/<filtering-ns>` — `filtered-*` topics should disappear shortly after.

## Related

- [broker-side-filter-observability](https://github.com/david-streamlio/broker-side-filter-observability) — the UI image this demo points at.
- [ShadowTraffic docs](https://shadowtraffic.io/docs) — generator config reference.
- Root README — orchestrator deployment + RBAC checklist.
