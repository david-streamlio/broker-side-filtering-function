# M3: windows on hot vs cold storage

How a time window maps onto the log's ledgers, and what reading hot (BookKeeper) vs cold (offloaded to
object storage) ledgers costs. Runs on the tiered bench stack: sn-platform with tiered storage on MinIO
(the Apache image ships no offloaders).

`run_m3.py`:

1. loads `--events` unbatched events into 2,000-entry ledgers and offloads all but the newest `--keep`;
2. records the ledger timeline from `topics info-internal` (entries, size, close time, `isOffloaded`),
   which doesn't touch the tier;
3. reads a cold window (offloaded ledgers in the middle), a hot window (the newest closed ledgers) and the
   whole topic, each with the windowed read and with a client scan of the same bounds. It counts MinIO
   GETs and bytes per read, sampling MinIO's cluster metrics until they settle. Each cold read starts
   from a broker restart (empty caches) and a warm-up read; the cold windowed read then runs again, from
   the tiered-storage cache;
4. trims a second topic's first phase with a 1-minute retention, loads a second phase, and checks that a
   window from the first phase reports `TRUNCATED_BY_RETENTION`.

```bash
docker compose -f bench/docker-compose.yml stop
docker compose -f bench/docker-compose.tiered.yml up -d
caffeinate -ims python3 bench/m3/run_m3.py --events 200000
python3 bench/m3/chart.py bench/out/m3/m3-<timestamp>.json
```

`--retention-only <result.json>` reruns just step 4 and merges it into an existing result.

## Reading the numbers

* **Cold cost here is requests and bytes, not time.** The broker runs emulated (amd64 on arm64) and
  local MinIO has no per-request latency, so a cold read isn't slower here; on real object storage each
  GET adds tens of milliseconds (spike S3).
* **The windowed read's setup reads the topic's oldest message** to detect retention truncation. On a
  tiered topic that's an offloaded ledger: about 7 GETs and 1.4 MB per windowed read, even for a hot
  window (the tier reads at least a block).
* **Retention deleting everything hides truncation.** If no message is left, a window reports COMPLETE
  with nothing scanned: there's nothing to compare its start against.
* **After a restart, sn-platform's functions worker can fail to start the orchestrator** (HTTP 503 from
  the package service, no retry). The runner restarts the function when that happens.
