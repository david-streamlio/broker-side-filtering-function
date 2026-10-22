# M1: client-side scan vs windowed read

One entity's events over a time window (the generator's hot order `8675309`, the last 24 h), read
four ways by `bench/clients`' `EntityReadClient`:

| Arm | How |
|---|---|
| `pulsar-scan` | Pulsar `Reader`, `seek(now - 24h)`, read to the end, discard other orders on the client |
| `pulsar-window` | Pulsar SDK, `.selector("__key__ = '8675309'").window(TimeWindow.last(24h))` |
| `kafka-scan` | Kafka consumer, `offsetsForTimes(now - 24h)`, read to the end offsets, discard other orders |
| `kafka-window` | Kafka SDK, same selector and window |

`run_m1.py` runs the arms interleaved for each repetition and records, per run:

* **Client:** records and payload bytes delivered, bytes on the wire (Kafka: `bytes-consumed-total`;
  Pulsar window: the private topic's bytes out; Pulsar scan: the source topic's bytes out), time to
  first match and to completion, peak TCP connections (sampled with `lsof`).
* **Broker:** bytes and messages dispatched from the source topic (to the scanning client, or to the
  windowed filter), BookKeeper read operations, and container CPU from cgroup counters, split per
  process: broker JVM, the windowed filter's JVM, other function instances.
* **Checks:** the answer is exactly the hot order's events in `seq` order, and a scan saw the whole
  window, against the generator's manifest.

## Running

```bash
mvn -DskipTests package                                   # NARs + bench/clients/target/bench-clients.jar
pip install -r bench/requirements.txt

# Pulsar arms: Apache Pulsar, native
docker compose -f bench/docker-compose.yml up -d
caffeinate -ims python3 bench/m1/run_m1.py --profile laptop-lite --reps 3

# Kafka arms: KoP needs sn-platform (one bench broker at a time; same host ports)
docker compose -f bench/docker-compose.yml stop
docker compose -f bench/docker-compose.kop.yml up -d
caffeinate -ims python3 bench/m1/run_m1.py --profile laptop-lite --reps 3 \
    --container bench-kop --arms kafka-scan,kafka-window --data-dir bench/out/data-kop

python3 bench/m1/chart.py <pulsar-result.json> <kafka-result.json> --out m1.png
```

The first run loads the topic with the generator (`--profile`, `--seed`) and deploys an orchestrator;
later runs reuse both. Pass `--redeploy` after rebuilding the NARs. Results go to `bench/out/m1/`
unless `--out-dir` says otherwise.

## Reading the numbers

* **Client and broker share a laptop.** Bytes cost almost nothing over loopback, so a scan's time here
  is far below what the same bytes take over a real network. The bytes and records columns carry over;
  the time column only shows the floor.
* **The windowed read's broker CPU includes starting its filter.** Under `ProcessRuntime` (the Apache
  broker) every windowed query gets a new function JVM. Under `ThreadRuntime` (the KoP broker) filters
  run inside the broker JVM, so there's no per-process split.
* **The KoP broker is emulated** on arm64 hosts (`linux/amd64`): compare CPU only between the Kafka
  arms, not with the Pulsar arms.
* **Keep the machine awake.** A host sleep mid-run stalls the generator (send timeouts) and jumps the
  VM's wall clock on wake, which makes the orchestrator's idle GC treat a running window as stale and
  delete its filter. Hence `caffeinate`.
* **`last(24h)` on a topic younger than 24 h reports `TRUNCATED_BY_RETENTION`:** the window starts
  before the oldest retained message, and the server can't tell retention from "the topic is newer".
