# Time-windowed read demo: "what happened to order 8675309?"

The stage scene for *Stop Scanning the Whole Topic: Server-Side Filtered Reads for Agent Context*.
From a clean broker it runs order traffic, then an agent asks for one order's recent events three
ways. A key filter over a time window, nothing else.

```bash
make window-demo-build        # once: NARs, bench clients, MCP server (mvn, ~2 min)
make window-demo              # the scene, straight through: ~1:40
make window-demo PAUSE=1      # waits for Enter before each act, for presenting
make window-demo-down         # stop the broker, delete its data
```

`make window-demo` starts from nothing every time: it removes the demo's broker and data, then runs
`docker compose up`. It uses its own compose project (`window-demo`) and volume, so it never touches
the bench brokers' data. It shares their host ports, though, and refuses to start while one of them
is running.

## The scene

| When | What |
|---|---|
| 0:00 | `docker compose up` from clean: Apache Pulsar 4.2.1 standalone (`docker-compose.yml`) |
| 0:05 | Pulsar is up; order traffic starts (`bench/generator`, ~1,400 events/s in the first minute, ~1 KB each) |
| 0:17 | Orchestrator deployed |
| 0:25 | Order 8675309 is created, then paid, picked and packed over the next ~36 s |
| 1:11 | **Act 1:** the client scans the last 60 s and keeps order 8675309 |
| 1:15 | **Act 2:** the same window, read server-side with `.selector("__key__ = '8675309'").window(TimeWindow.last(...))` |
| 1:20 | **Act 3:** `.window(TimeWindow.since(...))`: the order's history, a caught-up marker, then live |
| 1:26 | The warehouse ships the order: `shipped`, `out_for_delivery`, `delivered` arrive live, 3 s apart |
| 1:36 | Done |

A run on the laptop (Colima, 2 CPUs / 6 GB), event lines trimmed:

```text
[1:11] orders flowing: 88,989 events in the topic, 1,351/s

Act 1: the client scans the last 60 s and keeps order 8675309
  client read 85,175 events (87.2 MB) to keep 4, in 3.6 s

Act 2: the server scans the last 60 s; the client gets order 8675309
  client received 4 events (4.1 KB), in 3.9 s
  server scanned 88,970 events (91.1 MB) in 1.0 s, COMPLETE

Act 3: order 8675309's history, then live
  16:39:00.160  created           seq 0
  16:39:02.560  paid              seq 1
  16:39:23.560  picked            seq 2
  16:39:35.560  packed            seq 3
  ── caught up: history delivered, now live ──
  server scanned 107,364 events (109.9 MB) in 1.1 s, COMPLETE

  warehouse: shipped
  16:40:02.469  shipped           seq 4  +16 ms
  warehouse: out_for_delivery
  16:40:05.474  out_for_delivery  seq 5  +12 ms
  warehouse: delivered
  16:40:08.480  delivered         seq 6  +17 ms

[1:36] done: 1:36 after a clean docker compose up
```

## What to point at

* **Act 1 vs Act 2, the bytes:** the same 4 events, but 87 MB to the client versus 4 KB. The server
  still reads the whole window ("server scanned"); only the client's side shrinks.
* **Act 1 vs Act 2, the time:** about the same here, and the scan can win. Starting the window's
  filter costs ~3 s (the server's scan itself took 1 s), and over loopback 87 MB costs almost
  nothing. Over a real network the scan's time grows with its bytes (see M1/M2).
* **The body is never deserialized.** The selector reads the message key from the metadata; the
  ~1 KB JSON body passes through as opaque bytes.
* **The window is on the log's clock** (broker time), resolved when each read starts, so Act 2's
  window is a few seconds later than Act 1's and holds slightly more events.
* **Act 3: an empty receive never means "done".** The caught-up marker does; after it, events are
  live. The `+ms` is how long after an event was written it reached the agent (producer and agent
  share the host clock).

The code under each act's title is abbreviated; the agent is `bench/clients`' `WindowDemo.java`.

## Asking an agent (MCP)

After the scene the broker keeps running, with order 8675309's whole lifecycle in the topic. The
[entity events MCP server](../../mcp/entity-events/README.md) puts the windowed read behind one tool,
`get_entity_events`, and this directory's `.mcp.json` registers it for Claude Code:

```bash
cd demo/window && claude      # approve the project's entity-events server once
> What happened to order 8675309 in the last 15 minutes?
```

Claude calls `get_entity_events(key: "8675309", lookback: "15m")` and gets the order's 7 events (about
9 KB, in ~3.5 s) instead of the topic. A headless rehearsal (~22 s):

```bash
claude -p "What happened to order 8675309 in the last 15 minutes?" \
    --mcp-config .mcp.json --strict-mcp-config --allowedTools mcp__entity-events__get_entity_events
```

What to expect:

* **`TRUNCATED_BY_RETENTION`, on purpose.** The topic is a few minutes old, so a 15-minute window starts
  before its first event, and the server can't vouch for that part (retention and a newer topic look the
  same). The tool says so, and Claude explains it, then reasons from the order's gap-free `seq` 0–6 that
  nothing is missing for this order. That's the "an empty answer must say whether it's complete" point,
  live.
* **Compressed lifecycle.** The demo squeezes an order's day into about a minute, and Claude may remark
  that steps seconds apart look like test data. Say so before it does.
* An order ID with no events gives an empty list; `x' OR '1'='1` is rejected as an invalid key.

## Before going on stage

* Build (`make window-demo-build`) and run the scene once: that creates `demo/window/.venv`
  (needs network for `pip`) and leaves the `apachepulsar/pulsar:4.2.1` image pulled.
* Stop the bench brokers (`bench-pulsar`, `bench-kop`, `bench-tiered-pulsar`).
* For the agent part: run `claude` in `demo/window` once beforehand to approve the `entity-events`
  server, and keep that session open on stage.
* Keep the Mac awake (`caffeinate -dims`): a sleep mid-scene stalls the traffic.
* With `PAUSE=1`, a long pause grows the windows: each act reads from just after traffic started,
  so the order's history stays in it. More to scan only makes the point stronger.

## Knobs

In `window_demo.py`: `RATE` (target events/s; the generator ramps toward it as orders progress),
`HOT_AT_S` / `HOT_STEPS` (when the order starts and how much of its lifecycle the generator sends),
`WARMUP_S` (traffic before Act 1), `LIVE_GAP_S` (between the warehouse's events). Each act's output
from the agent's stderr, and the generator's log, go to a temp directory named in any failure.
