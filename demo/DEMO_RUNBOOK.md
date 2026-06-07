# Berlin Buzzwords 2026 — Demo Runbook

**Time budget: 10 minutes** | **Format: UI-first, no CLI on stage**
Focus: cost savings · latency transparency · live reconfiguration

---

## Pre-demo checklist (5 min before stage)

```bash
# 1. Health check — must pass before you walk up
./demo/check-cluster.sh
./demo/check-docker.sh

# 2. Verify all local containers are up
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -E "shadow|query|observ"
# Expected: 3 rows, all "Up"

# 3. Ensure colocated producer is running (key=COLOCATED, in-cluster toolset pod)
# If not running: ./demo/launch-toolset-producer.sh -n 0 -t 50 -k COLOCATED -d

# 4. Open browser before you walk up
open http://localhost:3200
```

**Silence monitoring alerts:**
→ StreamNative Cloud console → Monitoring → mute instance for 2 hours.

**Browser tabs to have open:**
1. `http://localhost:3200` — Overview (primary screen)
2. `http://localhost:3200` — second tab for navigation

---

## 10-minute demo

### [~1.5 min] Step 1 — Cold open: it's already running, and saving

**URL:** http://localhost:3200 → Overview

1. Click **▶ Start counting** immediately as you walk up — the savings counter starts ticking.

2. Point to the hero numbers:
   - **~170 msg/s** flowing through `stock-quotes`
   - **Active queries · latency monitored** — each has a private output topic

3. Point at the **Fanout bar** — the green block (Data NOT sent) dominates the tiny blue (Data sent):
   - `95% not sent` → data that never left the broker for each subscriber
   - `$0.69/hr · 8 GB/hr not delivered`

> _"A single Kafka topic, nine subscribers. Each gets only the slice they asked for.
> Everything else is dropped at the broker — it never travels over the network.
> You pay for ingestion once. Traditional fan-out pays per consumer, per message."_

---

### [~2 min] Step 2 — Latency: what you're actually measuring

**URL:** http://localhost:3200 → Overview → Active queries · latency monitored

**Set the scene first — where is everything running?**

> _"Quick picture of the setup. ShadowTraffic — here on my laptop — speaks Kafka protocol and
> connects to the cluster over the network. The broker ingests it through KoP, Kafka on Pulsar:
> the bridge between the Kafka API and the Pulsar storage engine. Both producers use
> the Apache Kafka client — ShadowTraffic uses the Java library, the colocated one uses Python.
> Same protocol. Same bridge. Same filter."_

> _"The consumers are also running locally — a small Java app that registered these queries
> and reads from the private output topics via the standard Kafka consumer API.
> No changes to the consumer code. It just subscribes to a different topic."_

> _"And then there's a second producer — a Python script running as a pod colocated with
> the broker inside the cluster. Intra-cluster network, essentially zero extra latency."_

1. Point at any `filtered-*` query — **p50 will vary (typically 100–400ms on WiFi)**. Frame it proactively:

> _"This number will vary depending on the network between my laptop and the cluster.
> Conference WiFi — expected to drift. That's actually what makes the next comparison interesting."_

2. Find the `watchlist-COLOCATED` query (key=COLOCATED) — **p50 ≈ 8ms**:

> _"Same Kafka client, same KoP bridge — but intra-cluster. No WiFi.
> p50 is 8 milliseconds. The colocated producer itself gets a 7ms produce ack,
> so the filter adds about 1 millisecond on top. That's it."_

3. **The proof that selector complexity doesn't matter** — look at the other queries:

> _"Here's what's interesting. These other queries — `side = 'S' AND sector = 'tech'`,
> `exchange = 'NYSE' AND side = 'S'` — they're evaluating fields in the message payload,
> not just the key. And they also show p50 of 8ms. Field-level filtering costs the same
> as key filtering. The JMS selector evaluation is negligible — the latency is the wire,
> not the logic."_

4. Point to the **last message timestamp** next to each query — aligned with the live clock top-right.

> _"Every query shows when the last correlated message arrived — real-time, not an aggregate."_

**Key numbers to have ready:**
- Colocated produce ack: **p50 ≈ 7ms** (Python kafka-python → intra-cluster KoP)
- Filter end-to-end: **p50 ≈ 8ms** (above + selector eval + output write)
- Filter overhead: **~1ms** regardless of selector complexity
- ShadowTraffic: **p50 ≈ 150ms** — same stack, 140ms is your WiFi

**Presenter note:** if p90 is very high (>2s) for ShadowTraffic queries, lean into it —
_"p90 is inconsistent because the conference WiFi is inconsistent. Watch the COLOCATED query —
same KoP bridge, intra-cluster network — it stays flat. The filter is not the variable here."_

---

**More queries = more savings** — point at the Fanout section:

> _"The more subscribers on this topic, the more dramatic the savings.
> Right now we have 9 queries. If we had 50, the 'Data NOT sent' bar grows proportionally —
> the source pays once, every additional subscriber is nearly free."_

---

### [~1 min] Step 3 — What's inside

**URL:** http://localhost:3200 → click the filter function name

1. Show all **N selectors running inside one function instance** — IN/MATCH/DROP% per selector.
2. Point to `bad-syntax-demo` red badge → _"Bad selectors fail gracefully — they don't crash the function."_
3. Click any **output topic** → **"Filter in action"** blue panel shows the JMS selector + source link.
4. Scroll messages — real stock symbols, real fields.

> _"One function, N selectors, N private output topics.
> Adding a new subscriber doesn't spawn a new consumer group on the source —
> it adds one more selector expression to this function."_

---

### [~2 min] Step 4 — Create a live filter: AAPL + price threshold

**URL:** http://localhost:3200 → Simulator tab

1. In the JMS selector textarea type directly:
   ```
   __key__ = 'AAPL' AND price > 0
   ```
2. Click **Register filter** → new query appears in Overview within seconds.
3. Navigate to the output topic → click through recent messages → show AAPL records with price values.

> _"That's a field-level filter — it's evaluating the message payload in the broker,
> not in your consumer. This is standard JMS selector syntax running server-side."_

---

### [~2.5 min] Step 5 — Modify live: price > 1000 (instant empty)

1. Click **✎** on the AAPL query you just created.
2. Change the selector to:
   ```
   __key__ = 'AAPL' AND price > 1000
   ```
3. Save. Switch to the output topic — **messages stop arriving immediately**.
4. Point at the source rate (still 170 msg/s) — data is still flowing, nothing matches.

> _"ShadowTraffic generates prices between $50 and $500. Nothing passes $1,000.
> The filter changed in place — no restart, no redeployment. Your consumer kept running
> and now receives nothing because nothing qualifies."_

5. Click **✕** to remove the query. Back to Overview.

6. Point at the **Start counting** total — `$X.XX saved · Y GB not sent` since you walked on stage.

> _"In the time it took to show you this, the broker saved Y gigabytes of network traffic
> that would have been pushed to every subscriber in a traditional fan-out system.
> This is the O(1) vs O(N) cost model for data delivery."_

---

## Fallback plan

| Symptom | Fix |
|---|---|
| Filter function not spawning | `docker restart multi-query-demo` |
| Simulator query not appearing | Refresh page — heartbeat every 15s |
| No traffic on overview | `docker start stock-quotes-shadowtraffic` |
| COLOCATED query not showing latency | Check toolset producer: `./demo/launch-toolset-producer.sh -n 0 -k COLOCATED -d` |
| p50 shows — ⚠ | Click **↺ Reset sampler** in the Functions section header |
| Filter receiving 0 messages | `./demo/reset-source-topic.sh` |
| Cluster unreachable | `./demo/check-cluster.sh` |

---

## Notes

- **"Start counting"** — click it as you walk up, not before. The audience sees it from zero.
- **COLOCATED query** — must be in `watchlist-*` group (created via Simulator or `live-query-demo.sh`). Pre-register it in the pre-demo checklist if it's not already there.
- **ShadowTraffic price range** — `uniform(50.0, 500.0)`. `price > 500` will be empty; `price > 1000` is guaranteed empty.
- **`bad-syntax-demo`** in queries.yaml is intentional — fires `selector-compile-fail` badge.
- **Simulator queries** heartbeat every 15s from the observability app. Survive up to 600s (10 min) without a heartbeat.
