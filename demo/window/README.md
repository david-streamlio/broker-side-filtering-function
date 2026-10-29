# Time-windowed read demo: "why can't customer 8675309 check out?"

The stage scene for *Stop Scanning the Whole Topic: Server-Side Filtered Reads for Agent Context*.
A support agent triages a customer whose checkout keeps failing. It reads that customer's recent
session events from a busy topic, three ways, then keeps watching while the customer retries. A key
filter over a time window, nothing else.

```bash
make window-demo-build        # once: NARs, bench clients, MCP server (mvn, ~2 min)
make window-demo              # the scene, straight through: ~2:05, from a clean broker
make window-demo PAUSE=1      # waits for Enter before each act, for presenting
make window-demo KEEP=1       # the scene on top of the broker's retained data
make window-demo-stop         # stop the broker, keep its data
make window-demo-down         # stop the broker, delete its data
```

`make window-demo` starts from nothing: it removes the demo's broker and data, then runs
`docker compose up`. It uses its own compose project (`window-demo`) and volume, so it never touches
the bench brokers' data. It shares their host ports, though, and refuses to start while one of them
is running.

## The scene

| When | What |
|---|---|
| 0:00 | `docker compose up` from clean: Apache Pulsar 4.2.1 standalone (`docker-compose.yml`) |
| 0:05 | Pulsar is up; shopping-session traffic starts (`bench/generator --model sessions`: browse, cart, checkout, payment, ~1 KB events keyed by customer); the observability UI starts at http://localhost:3200 |
| 0:17 | Orchestrator deployed |
| 0:17–1:08 | Customer 8675309 shops on the iOS app, starts checkout three times, and each payment times out on the card's 3-D Secure challenge; then they open the support chat (21 events) |
| 1:21 | **Act 1:** the client scans the last 70 s and keeps customer 8675309's events |
| 1:26 | **Act 2:** the same window, read server-side with `.selector("__key__ = '8675309'").window(TimeWindow.last(...))` |
| 1:31 | **Act 3:** `.window(TimeWindow.since(...))`: the customer's history, a caught-up marker, then live |
| 1:35 | The customer retries while the agent watches: it times out again, the agent says to approve the prompt in the bank app, and the payment goes through: 10 live events, 2.5 s apart |
| 2:05 | Done |

A run on the laptop (Colima, 2 CPUs / 6 GB), event lines trimmed:

```text
Act 1: the client scans the last 70 s and keeps customer 8675309's events
  client read 61,232 events (62.7 MB) to keep 21, in 3.6 s

Act 2: the server scans the last 70 s; the client gets customer 8675309's session
  12:16:34.671  payment_submitted seq 9
  12:16:35.671  3ds_challenge     seq 10
  12:16:40.671  3ds_timeout       seq 11
  ...           (two more attempts, the same way)
  12:17:06.671  chat_opened       seq 20
  client received 21 events (21.5 KB), in 4.1 s
  server scanned 65,896 events (67.5 MB) in 1.0 s, COMPLETE

Act 3: customer 8675309's session, then live while they retry
  ── caught up: history delivered, now live ──
  12:20:16.074  3ds_timeout       seq 24  +11 ms
  agent: "Your bank is asking you to approve the payment. Open your bank app, approve it, then try again."
  12:20:27.614  3ds_approved      seq 27  +24 ms
  12:20:30.123  payment_approved  seq 28  +11 ms
  12:20:35.144  order_confirmed   seq 30  +10 ms

[2:07] done: 2:07 after a clean docker compose up
```

## What to point at

* **The sequence is the diagnosis.** A status lookup says "payment failed". The events say why: three
  attempts, each `payment_submitted → 3ds_challenge → 3ds_timeout`, on the iOS app. That's what the
  agent needs, and it's one customer's slice of a busy topic.
* **Act 1 vs Act 2, the bytes:** the same 21 events, but 62.7 MB to the client versus 21.5 KB. The
  server still reads the whole window ("server scanned"); only the client's side shrinks.
* **Act 1 vs Act 2, the time:** about the same here, and the scan can win. Starting the window's filter
  costs ~3 s, and over loopback 63 MB costs almost nothing. Over a real network the scan's time grows
  with its bytes (see M1/M2).
* **The body is never deserialized.** The selector reads the message key from the metadata; the ~1 KB
  JSON body passes through as opaque bytes.
* **Act 3: why keep listening.** The customer is still in the chat; the next attempt happens while the
  agent watches, and it sees the timeout, then the approval, as they're written (`+ms`: how long after an
  event was written it reached the agent; producer and agent share the host clock). An empty receive
  never means "done": the caught-up marker does.

The code under each act's title is abbreviated; the agent is `bench/clients`' `WindowDemo.java`.

## Retained data: a topic with real history

The broker's data lives in the compose project's `pulsar-data` volume, with retention unlimited
(`defaultRetentionTimeInMinutes=-1`). Broker time is set on arrival, so a day of history has to really
arrive over a day:

```bash
make window-demo-load                    # 24 h of session traffic at 50 events/s (~4.3M events), kept
make window-demo-load DURATION=2h RATE=200
make window-demo-stop                    # later: stop the broker; the volume keeps the data
make window-demo KEEP=1                  # the scene on top of it (no wipe)
```

The load publishes background sessions only. The demo adds customer 8675309's story live, so it's always
recent. On retained data, a window that starts after the topic's first event comes back `COMPLETE` instead
of `TRUNCATED_BY_RETENTION`. Ctrl-C stops a load; what was sent stays. `make window-demo-down` deletes the
volume.

## Watching it on the server: the observability UI

The stack includes the [broker-side filter observability UI](https://github.com/david-streamlio/broker-side-filter-observability)
(0.5.0) at **http://localhost:3200**. Its **Windowed reads** panel shows every windowed read as it runs, from
the server's side: the source topic, selector and window, the state (scanning, complete, truncated, live), and
what the server scanned against what reached the client. During the scene:

* **Act 2** appears as `COMPLETE`, with the server's scanned count against the 21 events sent to the client.
* **Act 3** shows `LIVE` while the customer retries, then `LIVE · CLOSED`.
* **Each agent tool call** (below) appears as one more window.

Put the browser next to the terminal. The panel keeps the last 50 windows in memory: restarting the UI
clears them. The orchestrator is deployed as `window-demo-orchestrator`, the name the UI is configured
with.

## Asking an agent (MCP)

After the scene the broker keeps running, with customer 8675309's whole session in the topic. The
[entity events MCP server](../../mcp/entity-events/README.md) puts the windowed read behind one tool,
`get_entity_events`, and this directory's `.mcp.json` registers it for Claude Code with
`ENTITY_TOPIC=persistent://public/default/session_events`:

```bash
cd demo/window && claude      # approve the project's entity-events server once
> Customer 8675309 says checkout keeps failing. What happened in the last 15 minutes?
```

Claude calls `get_entity_events(key: "8675309", lookback: "15m")` and gets the customer's 31 events (the
history and the live retry) instead of the topic. Called directly, a 1 h lookback took ~4 s. A headless
rehearsal:

```bash
claude -p "Customer 8675309 says checkout keeps failing. What happened in the last 15 minutes?" \
    --mcp-config .mcp.json --strict-mcp-config --allowedTools mcp__entity-events__get_entity_events
```

What to expect:

* **`TRUNCATED_BY_RETENTION` on a clean broker.** The topic is a few minutes old, so a 15-minute window
  starts before its first event, and the server can't vouch for that part (retention and a newer topic look
  the same). The tool says so; the customer's gap-free `seq` 0–30 show nothing is missing for them. On
  retained data (`KEEP=1` after a load) the same call comes back `COMPLETE`.
* A customer ID with no events gives an empty list; `x' OR '1'='1` is rejected as an invalid key.

## Before going on stage

* Build (`make window-demo-build`) and run the scene once: that creates `demo/window/.venv`
  (needs network for `pip`) and leaves the `apachepulsar/pulsar:4.2.1` image pulled.
* Stop the bench brokers (`bench-pulsar`, `bench-kop`, `bench-tiered-pulsar`).
* Open http://localhost:3200 in a browser tab after a rehearsal run, so the image is pulled and the tab is
  ready (the UI takes ~20 s to start).
* For the agent part: run `claude` in `demo/window` once beforehand to approve the `entity-events`
  server, and keep that session open on stage. Rehearse the question with the session scenario.
* Keep the Mac awake (`caffeinate -dims`): a sleep mid-scene stalls the traffic.
* With `PAUSE=1`, a long pause grows the windows: each act reads from just after traffic started,
  so the customer's history stays in it. More to scan only makes the point stronger.

## Knobs

In `window_demo.py`: `RATE` (target events/s; the generator ramps toward it as sessions start),
`HOT_AT_S` (when the customer's session starts), `WARMUP_S` (traffic before Act 1), `LIVE_GAP_S` and
`AGENT_PAUSE_S` (pacing of the live retry). The customer's history and live script are `HOT_HISTORY` and
`HOT_LIVE` in `bench/generator/sessions.py`. Each act's output from the agent's stderr, and the generator's
log, go to a temp directory named in any failure.
