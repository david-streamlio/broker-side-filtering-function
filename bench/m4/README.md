# M4: what filtering costs, by compression and selector type

## Function arm (`run_m4.py`)

For each compression (none, lz4, zstd), a topic of `--events` order events, batched as the generator
sends them. For each selector type, one windowed read over the whole topic:

| Type | Selector | What the filter does |
|---|---|---|
| `key` | `__key__ = '8675309'` | reads the message key; the payload isn't parsed |
| `property` | `order_id = '8675309'` | reads a message property; the payload isn't parsed |
| `json` | `carrier = 'none'` | parses every JSON value; nothing matches |

The function's consumer always decompresses batches. While a read runs, the runner samples the filter
JVM's CPU time and the messages dispatched to the filter's subscription. **CPU per message** is the
slope of one against the other between 10% and 90% of the scan, so the JVM's startup and the tail are
left out. (Subtracting a half-window read to cancel startup was too noisy.) Each read is checked: the
whole topic scanned and the expected matches (7, 7, 0).

```bash
mvn -DskipTests package
docker compose -f bench/docker-compose.yml up -d
caffeinate -ims python3 bench/m4/run_m4.py --events 1000000 --reps 3
python3 bench/m4/chart.py bench/out/m4/m4-function-<timestamp>.json
```

## Broker arm

The selector EntryFilter (`broker-filter/`) evaluates key and property selectors at the broker from the
entry's metadata, without touching the payload, but only for unbatched entries; batched entries pass
through unfiltered. Load it with the override:

```bash
docker compose -f bench/docker-compose.yml -f bench/docker-compose.entryfilter.yml up -d
```

A subscription gets filtered when it carries the subscription property `selector`, for example
`pulsar-admin topics create-subscription -s s1 -p "selector=__key__ = '8675309'" <topic>` or
`subscriptionProperties` on a Java `ConsumerBuilder`. Every other subscription gets every entry.
