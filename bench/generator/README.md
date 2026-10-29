# Order-lifecycle load generator

Deterministic load for the time-windowed filtered-read demo and measurements. It replaces
ShadowTraffic (no license needed).

```bash
python3 -m venv .venv && .venv/bin/pip install -r bench/requirements.txt
.venv/bin/python bench/generator/orders_gen.py --profile smoke \
    --topic persistent://public/default/orders
```

## What it produces

Order events (`created`, `paid`, `picked`, `packed`, `shipped`, `out_for_delivery`,
`delivered`, plus some `cancelled` and `returned`). Many orders are in flight at once, so the
stream interleaves them like a real order system.

| Field | Where | Use |
|---|---|---|
| `order_id` | message key **and** property | `__key__ = '8675309'` or `order_id = '8675309'`: a handful of events |
| `customer_id` | property | Zipf-skewed (`--zipf`, default 0.8): a few customers have many orders |
| `region` | property | uniform over `--regions` (default 100): `region = 'r07'` is ~1% |
| `event_type`, `seq` | properties | per-order `seq` is contiguous from 0, so gaps are detectable |
| `event_time` | property **and** Pulsar event time / Kafka timestamp | simulated time (see below) |
| payload | value | ~`--payload-bytes` (default 1024) of JSON: items, totals, address, carrier |

The **hot order** (`--hot-order`, default 8675309) has a fixed 7-event lifecycle, identical in
every run regardless of seed. It's the order the demo asks about. `--hot-steps N` sends only its first
N events; the stage demo (`demo/window`) sends the rest live.

Same `--seed` and parameters give the same events in the same order. Only wall-clock send
times differ.

## Shopping sessions (`--model sessions`)

For the checkout-troubleshooting stage demo (`demo/window`), `--model sessions` (`sessions.py`) generates
shopping-session events keyed by **customer** instead: `session_start`, `search`, `product_view`,
`add_to_cart`, `checkout_started`, `payment_submitted`, `3ds_challenge`, `3ds_approved` or `3ds_timeout`,
`payment_approved`, `order_placed`, and so on, a few seconds apart. Properties: `customer_id` (also the
key), `session_id`, `region`, `platform`, `event_type`, `seq`, `event_time`; the ~1 KB JSON value carries
the cart and the step's detail (for example a decline reason). Lifecycle scale defaults to 1.0 in both
modes: sessions already take seconds.

The hot entity (`--hot-order`, default 8675309) is then a customer with a fixed 21-event history: three
checkout attempts time out on 3-D Secure, then they open the support chat. `HOT_LIVE` is what the stage
demo sends live afterwards. `--no-hot-order` sends background traffic only, for a long load the demo later
adds the customer to:

```bash
orders_gen.py --model sessions --mode realtime --rate 50 --duration 24h --no-hot-order \
    --topic persistent://public/default/session_events
```

## Two clocks, two modes

A windowed read uses the **log's clock**: broker receive time (spike S1). A client can't
backdate it. So:

* **`--mode bulk`** (default) sends a profile as fast as allowed (`--rate` caps it).
  `event_time` covers `--sim-span` (default 24h) ending now, but the broker sees it all arrive
  within minutes. Use it for "a day of traffic, by volume" measurements.
* **`--mode realtime`** paces sends to the wall clock at `--rate` events/s, with
  `event_time` = now. Lifecycles are compressed (`--lifecycle-scale`, default 0.01) so orders
  progress in minutes. `--backfill 15m` sends the first 15 simulated minutes immediately so the
  stream starts in steady state. Use it for the stage demo and long-running loads.

| Profile | Events | Broker storage (approx.) |
|---|---|---|
| `smoke` | 100k | ~120 MB |
| `laptop-lite` | 2M | ~2.4 GB |
| `laptop` | 10M | ~12 GB (check Docker Desktop's disk limit) |

## Outputs

* `--sink pulsar` (default): Pulsar producer. `--batching/--no-batching`,
  `--batch-delay-ms`, `--compression none|lz4|zstd` are there for the decompression
  measurement (M4).
* `--sink kafka`: native Kafka producer (e.g. via KoP). **Pass `--prepare-topic`** (with
  `--admin-url`) so the topic is pre-created with the parent property
  `kafkaEntryFormat=pulsar_non_batched`. Without it, KoP stores Kafka batches in a format the
  filter function's consumer discards as corrupted (spike S2).
* `--sink null`: generate only. Measures generator speed (~48k events/s on an M-series
  laptop), and `--dump N` prints sample events.

Every run writes to `--out-dir` (default `bench/generator/out/`):

* `<topic>-<start>-manifest.json`: parameters, git sha, counts by event type / region /
  top customers, and the hot order's events with their message IDs (Pulsar) or offsets (Kafka).
* `<topic>-<start>-expected.csv`: events per order. A reader that saw every event of an order
  saw `seq` 0..events-1.

## Examples

```bash
# 2M events, LZ4-compressed batches
orders_gen.py --profile laptop-lite --compression lz4 --topic persistent://public/default/orders-lz4

# Stage demo: steady 2,000 events/s for 30 minutes, starting in steady state
orders_gen.py --mode realtime --rate 2000 --backfill 15m --duration 30m

# Native Kafka ingest through KoP
orders_gen.py --sink kafka --bootstrap 127.0.0.1:9092 --prepare-topic \
    --admin-url http://localhost:8080 --topic orders --profile smoke

# Late arrivals (event time vs log time demo): 2% of events sent 30s-15m late
orders_gen.py --profile smoke --late-fraction 0.02
```

## Tests

```bash
.venv/bin/python -m pytest bench/generator/tests
```
