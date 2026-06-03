# Berlin Buzzwords 2026 — Demo Runbook

## Setup checklist (before going on stage)

```bash
cd /Users/arodriguez/Code/berlinbuzzword2026/DavidRefactor18thMay/broker-side-filtering-function

# Verify all containers are running
docker ps

# Expected:
#   stock-quotes-shadowtraffic   Up
#   filter-observability         Up (healthy)
#   multi-query-demo             Up

# Observability UI
open http://localhost:3200
```

---

## Step 1 — Show ShadowTraffic: a Kafka producer running on your machine

**Concept:** _"This is a synthetic stock-quote feed. It's just a Kafka producer — the same library your team already uses."_

```bash
# Show the config file (43 symbols, random price/volume/sector/exchange/side)
cat demo/shadowtraffic/stock-quotes.json

# Show it's running
docker ps | grep shadow

# Kafka producer metrics (built-in Prometheus endpoint)
docker exec stock-quotes-shadowtraffic curl -s localhost:9400/metrics \
  | grep -E "generator_events_sent_total|generator_iteration_duration"

# Shows:
#   generator_events_sent_total{...} 450000        ← total events sent
#   generator_iteration_duration_seconds p99 < 0.001  ← < 1ms producer latency

# Increase speed for impact (optional)
./demo/shadowtraffic-fast.sh     # throttleMs=1 → ~800 msg/s

# Restore normal
./demo/shadowtraffic-normal.sh   # throttleMs=5 → ~150 msg/s
```

**Key point:** ShadowTraffic produces to `stock-quotes` using `SASL_SSL` — exactly like any production Kafka producer.

---

## Step 2 — Show the observability with active filters

**URL:** http://localhost:3200

**Walk the UI:**

1. **Overview** — hero banner: `source=150 msg/s → forwarded=148 → drop=2%`
   - 9 queries running simultaneously on ONE source topic
   - Each query gets a private filtered output topic

2. **Click the filter function name** → Filter Detail page
   - See all 8 selectors inline (tech-titans, bank-watch, nasdaq-only…)
   - Live charts: In / Match / Drop%
   - The `bad-syntax-demo` query fires `selector-compile-fail` flag → red badge

3. **Click any output topic** (e.g. `filtered-xxx`) → Topic Detail
   - Blue "Filter in action" panel shows the JMS selector + source link
   - Recent messages show real stock data with `p0` partition badge

4. **Click a query** → Query Detail
   - p50 / p90 / p99 end-to-end latency
   - Drop % specific to this selector

**Stop ShadowTraffic to show observability reacts:**
```bash
docker stop stock-quotes-shadowtraffic
# → source drops to 0, SOURCE_TOPIC_STALLED flag appears within 5s
docker start stock-quotes-shadowtraffic
# → traffic resumes, flag clears
```

---

## Step 3 — Create a new filter live from the web UI

**URL:** http://localhost:3200 → Simulator tab

1. Click **Register filter** in the Simulator
2. Select symbols (e.g. AAPL, TSLA) or type a custom selector
3. Click **Register filter** → new query appears in Overview within seconds
4. Navigate to the new output topic → "Filter in action" panel shows your selector
5. Click ✎ to modify the selector live (no restart)
6. Click ✕ to remove the query

**Show the selector edit:**
- Change `__key__ IN ('AAPL')` to `__key__ IN ('TSLA')`  
- The output topic immediately starts showing TSLA instead of AAPL
- No restart. No downtime. Reconfigured at the broker.

---

## Step 4 — Live query creation with timing measurement

**Concept:** _"How long from 'I want this filter' to 'first matching message arrives'?"_

```bash
# Default selector: AAPL + MSFT + NVDA
./demo/live-query-demo.sh

# Custom selectors
./demo/live-query-demo.sh "__key__ IN ('TSLA')"
./demo/live-query-demo.sh "sector = 'tech' AND side = 'B'"
./demo/live-query-demo.sh "exchange = 'NYSE' AND sector IN ('finance','tech')"
```

**Expected output:**
```
✓ Query registered at 14:19:50.524
✓ Filter function active after 790ms
✓ First message key: COST

Time to first message: 1332ms
  Orchestrator activation: ~790ms
  Filter processing:       ~648ms
```

**After running:** open the printed URLs to show:
- Output topic with "Filter in action" panel
- Query detail with live latency metrics

---

## Step 5 — Live filter modification (AAPL → TSLA)

**Concept:** _"Reconfigure a running filter in milliseconds."_

```bash
# Default: AAPL → TSLA
./demo/live-modify-demo.sh

# Custom
./demo/live-modify-demo.sh "__key__ IN ('JPM')" "__key__ IN ('NVDA')"
./demo/live-modify-demo.sh "sector = 'tech'" "sector = 'energy'"
```

**Expected output:**
```
[3/5] Letting filter run for 3 seconds...
  3s — recent keys: AAPL, AAPL, AAPL

[4/5] MODIFYING selector live (no restart!)
  ✓ Selector modified at 14:19:50.524

Time from MODIFY → first TSLA message: 3552ms
```

**What to say:** _"Same output topic. No restart. The broker reconfigured the filter instantly. Your Kafka consumers keep running — they just start receiving different messages."_

---

## Step 6 — Python Kafka consumer running on your machine

**Concept:** _"Your existing Kafka consumers work unchanged — just subscribe to the private filtered topic."_

```bash
# Step A: Create a query and get the topic name
./demo/live-query-demo.sh "__key__ IN ('AAPL','NVDA')"
# Note the outputTopic: persistent://public/default/live-demo-xxxxx

# Step B: Show the consumer code
cat demo/kafka-consumer.py   # standard confluent-kafka Consumer

# Step C: Run it (15 second window with latency stats)
python3 demo/kafka-consumer.py live-demo-xxxxx
# or with custom duration:
python3 demo/kafka-consumer.py live-demo-xxxxx 30
```

**Output shows:**
```
  [    1]  13:12:15.322  13:12:15.539    217ms  AAPL  side=S, exchange=NYSE, sector=consumer  seq=723857...
  [    2]  13:12:15.377  13:12:15.665    288ms  AAPL  side=S, exchange=NASDAQ, sector=finance  ...

End-to-end transit latency (15s window)
  p50 (median) : 215ms
  p90          : 300ms
  p99          : 641ms
```

**Key point:** `produce_time` = when ShadowTraffic called `send()`, `receive_time` = when Python consumer received it. The difference is the full broker-side filtering pipeline.

---

## Step 6b — Fallback: local Kafka producer (if latencies look bad)

If transit latencies are high (ShadowTraffic → SN Cloud network overhead),
show that a LOCAL producer running on your machine gets much lower numbers
since it doesn't go through the KoP bridge batching delay:

```bash
# Create a query that matches the LOCAL key
./demo/live-query-demo.sh "__key__ IN ('LOCAL')"
# Note the outputTopic

# Run the validated local producer (sends to stock-quotes with key=LOCAL)
./launch-toolset-producer.sh

# In a second terminal, consume from the filtered output:
python3 demo/kafka-consumer.py live-demo-xxxxx 15
```

The local producer shows that the ~200ms transit is the KoP bridge overhead
from ShadowTraffic (running in Docker, routing through the Kafka-to-Pulsar bridge),
not the filter function itself. The filter function adds only ~15ms.

---

## Quick reference — all demo commands

```bash
# Speed
./demo/shadowtraffic-fast.sh         # ~800 msg/s
./demo/shadowtraffic-normal.sh       # ~150 msg/s

# Create query + measure time-to-first-message
./demo/live-query-demo.sh [selector]

# Modify query live + measure time-to-first-new-message
./demo/live-modify-demo.sh [from-sel] [to-sel]

# Python Kafka consumer with latency stats
python3 demo/kafka-consumer.py <topic> [seconds]

# Local producer (key=LOCAL, fallback for latency demo)
./launch-toolset-producer.sh

# Cleanup all Simulator queries
# Use ✕ buttons in the UI, or:
curl -s http://localhost:3200/api/overview | python3 -c "
import sys,json,re,urllib.request
raw=re.sub(r'[\x00-\x1f]','',sys.stdin.read())
d=json.loads(raw)
for q in d.get('queries',[]):
    qid=q['queryId']
    req=urllib.request.Request(f'http://localhost:3200/api/query/{qid}',method='DELETE')
    try: urllib.request.urlopen(req); print(f'deleted {qid[:8]}')
    except: pass
"
```

---

## Notes

- **kafkaEntryFormat** must be `pulsar_non_batched` on the source topic — bootstrap.sh sets this. If the filter stops receiving, run: `make demo-down && make demo-cloud`
- **Simulator queries** are lost if the observability restarts (heartbeat TTL 90s). Re-run the curl/script.
- **multi-query-app queries** (from queries.yaml) survive restarts — they're owned by the multi-query-app container.
- **`bad-syntax-demo`** in queries.yaml is intentional — fires `selector-compile-fail` flag for the demo.
