# `queue` latency decomposition — KoP ingest path

## Why

PR #50's `[timing]` log measured `queue ≈ 90–115ms` on the cloud cluster and attributed it to the
BookKeeper write in the KoP ingest path — the premise for tuning namespace persistence (E/WQ/AQ)
and `journalSyncData`. That attribution is unproven: `queue = startMs − publishTime`, and under KoP
`publishTime = the Kafka producer's CreateTime` (set in `PulsarEntryFormatter`). So `queue`
conflates three independent slices, only one of which the storage knobs touch:

```
CreateTime ─[producer accumulator: linger/batch/metadata]→ on-wire
           ─[network]→ KoP receive ─[BookKeeper add-entry: WQ/AQ/journal]→ LAC advances
           ─[dispatch + function framework]→ process() entry (startMs)
```

90–115ms is high for an SSD journal fsync (usually single-digit ms), so a large share may be
producer-side or dispatch-side — neither helped by quorum tuning. The demo producer is already
`acks=1`, and `journalSyncData` isn't tunable on managed SN Cloud. **This pass decomposes `queue`
to confirm whether the BK write is actually the dominant term before any tuning.**

## How it's measured

The filter's `[timing]` log (`SelectorFilterFunction`, gated by `log-timing-every`) now prints a
producer/ingest split derived from `Message.getBrokerPublishTime()` (the broker-receive instant,
populated by `AppendBrokerTimestampMetadataInterceptor`):

```
[timing] key=AAPL queue=94ms producer=78ms ingest=16ms eval=0ms send=14ms total=14ms fanout=1 pending=0
```

| Term | Span | Tunable by |
|---|---|---|
| `producer` | CreateTime → broker receive (`brokerPublishTime − publishTime`) | producer `linger.ms`/`batch.size`/metadata |
| `ingest`   | broker receive → `process()` (`startMs − brokerPublishTime`) | **BookKeeper E/WQ/AQ/journal** + dispatch |
| `queue`    | CreateTime → `process()` (`= producer + ingest`) | — |
| `eval`     | `process()` → all `sendAsync()` submitted | filter CPU |
| `send`     | submit → output acked | output batching (#45) |

`producer`/`ingest` show `?` when broker-entry-metadata isn't enabled. The split narrows the
question: if `ingest` is small, the BK write is **not** the bottleneck and E/WQ/AQ tuning won't help.

### Enablement
- **Filter:** `log-timing-every > 0` (default 0/off). Threaded from the orchestrator's
  `filter-log-timing-every` knob → spawned filter `userConfig.log-timing-every`.
- **Broker:** `AppendBrokerTimestampMetadataInterceptor` in `brokerEntryMetadataInterceptors` +
  `exposingBrokerEntryMetadataToClientEnabled=true`. Wired in `demo/docker-compose.local.yml`;
  the local orchestrator config sets `filter-log-timing-every: 50`.

## Running it locally (with this branch's un-released code)

The local stack prefers host-mounted NARs over the release download, so stage locally-built NARs:

```bash
# JDK 17 for the build (Java 24 breaks Mockito)
export JAVA_HOME=/usr/local/Cellar/openjdk@17/.../Home
make package                                   # builds functions/*/target/*.nar (1.0.0-SNAPSHOT)
mkdir -p demo/local/nars
cp functions/selector-filter/target/selector-filter-function-*.nar      demo/local/nars/selector-filter-function-1.2.0.nar
cp functions/orchestrator/target/selector-filter-orchestrator-*.nar     demo/local/nars/selector-filter-orchestrator-1.2.0.nar
docker compose -f demo/docker-compose.local.yml up --build -d   # NOT `make demo-local` (that re-downloads)
# Collect:
docker logs stock-demo-pulsar 2>&1 | grep -i brokerEntryMetadata   # confirm the interceptor is active
docker compose -f demo/docker-compose.local.yml logs -f <filter-container> | grep '\[timing\]'
curl -s localhost:8080/metrics | grep pulsar_storage_write_latency   # Probe B: pure BK-write slice
```

## Cloud-metrics access (investigated)

`snctl` / `pulsarctl` / `pulsar-admin` expose only the admin `topics stats` API — **no Prometheus
endpoint**, so `pulsar_storage_write_latency` (the pure BK-write slice) is **not scriptable** on
SN Cloud; it's available only via the cloud console's Grafana (manual). The eventual cloud pass
therefore relies on the in-filter `[timing]` producer/ingest split (Probe A), optionally
cross-checked against console Grafana by hand.

## Results

_Numbers still pending._ The 2026-06-02 local run **validated the instrumentation wiring end-to-end**
but could not collect `[timing]` numbers:

- ✅ Orchestrator passthrough works: the spawned filter's userConfig was
  `{"log-timing-every":50,"control-topic":…}` — `filter-log-timing-every` → `log-timing-every`.
- ✅ Broker-entry-metadata interceptor active; filter spawned + initialized (8 queries).
- ❌ **Blocked by a known local KoP flake** — the filter discarded every input message with
  `SingleMessageMetadata: Some required fields are missing`
  (`deSerializeSingleMessageInBatch`): the source topic stored Kafka batches instead of
  `pulsar_non_batched`, so the filter's plain Pulsar consumer couldn't parse them
  (`numReceived: 0`). This is the entry-format property-revert issue, independent of this change
  (it's the message payload format, not `brokerEntryMetadataInterceptors`). Fix is to pre-create the
  source topic via Pulsar admin before any Kafka client touches it (separate demo-infra task).

**To get numbers:** re-run locally once the entry-format issue is resolved (pre-created topic), or
run the cloud pass where the ~90-115ms `queue` actually occurs and the entry format is stable. The
verdict on whether `ingest` (BK write) is the dominant term — and thus whether E/WQ/AQ tuning is
worth pursuing — depends on that data.
