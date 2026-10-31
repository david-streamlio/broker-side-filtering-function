# Entity events MCP server

An [MCP](https://modelcontextprotocol.io) server with one tool, `get_entity_events`: one entity's
events over a recent time window, read with the Pulsar SDK's windowed consumer
(`__key__ = '<key>'` over `TimeWindow.last(...)`). The window is filtered on the server, so only that
entity's events reach the agent, along with the server's completeness status.

A second tool, `watch_entity_events` (spike), keeps a live windowed read open (`TimeWindow.since(...)`):
the first call (with `key`) returns the entity's history, its completeness status and a `watch_id`; each
call with that `watch_id` waits up to `wait_s` (at most `WATCH_MAX_WAIT`, default 60s) and returns only the
events that arrived since. `{watch_id, stop: true}` closes it; a watch with no call for
`WATCH_IDLE_TIMEOUT` (default 5m) is closed, and at most `MAX_WATCHES` (default 5) are open, since each
holds a filter on the server. The agent still makes each call, so it's a long poll on a live subscription.

It speaks MCP over stdio (MCP Java SDK 2.0.1), so any MCP client can start it: Claude Code, the Claude
desktop app, or your own agent.

```bash
make window-demo-build          # builds target/entity-events-mcp.jar with the rest of the demo
make window-demo                # a broker with order traffic to ask about
cd demo/window && claude        # approve the project's entity-events server, then ask:
                                # "What happened to order 8675309 in the last 15 minutes?"
```

## The tool

| | |
|---|---|
| **In** | `key` (required): the entity's key, 1–128 letters, digits or `. _ : -` |
| | `lookback`: how far back to read, e.g. `15m`, `6h` (default `1h`, at most `MAX_LOOKBACK`) |
| | `max_events`: the most events to return (default 1000; the agent can ask for more or fewer, with no server cap). When more match, the tool keeps the newest and lists them oldest first |
| **Out** | `status`: `COMPLETE` or `TRUNCATED_BY_RETENTION`, plus `status_meaning`, a sentence for the agent |
| | `window` (`from`, `to`), `scanned` (events the server read), `matched`, `returned`, `more_events`, and `omitted` (how many older events were left out) when there were more |
| | `events`: `{ time, key, properties, value }`. `time` is when the log received the event, `properties` are the event's own (the filter's `__` ones are dropped), and `value` is parsed JSON when the value is JSON |

Invalid arguments and failures come back as tool errors with a message the agent can act on (for
example "lookback 48h is longer than this server allows (24h)").

## What makes it safe to hand to an agent

- **Agent text never becomes selector syntax.** The key must match `[A-Za-z0-9._:-]{1,128}`, so it can't
  close the quote it's placed in.
- **The server picks the topic** (`ENTITY_TOPIC`); the agent can't name one.
- **Bounded cost:** the lookback is capped and rejected beyond the cap, not shortened, because a shorter
  window answers a different question. Each call has a timeout. The agent sets the result size with `max_events` (default 1000, no server
  cap), and `matched` and `more_events` say whether there were more.
- **Honest answers:** the tool answers at the window's completion record, never on an empty receive. It
  deduplicates on `__src_msgid__`, because delivery is at-least-once. And it passes the server's status
  through: `TRUNCATED_BY_RETENTION` means the window starts before the oldest event the log holds,
  either because retention removed older events or because the topic is newer than the window. So an
  empty list is "no events" only when the status is `COMPLETE`.

## Configuration

Environment variables, with defaults for the window demo's broker:

| Variable | Default |
|---|---|
| `PULSAR_SERVICE_URL` | `pulsar://localhost:16650` |
| `PULSAR_ADMIN_URL` | `http://localhost:18080` |
| `FILTER_CONTROL_TOPIC` | `persistent://public/default/filter-requests` |
| `PRIVATE_TOPIC_NAMESPACE` | `public/default` |
| `ENTITY_TOPIC` | `persistent://public/default/orders` |
| `MAX_LOOKBACK` | `24h` |
| `CALL_TIMEOUT` | `60s` |

stdout carries the protocol; logs go to stderr. The SDK session opens on the first call, so the agent
can start the server before the broker is up.

`demo/window/.mcp.json` registers the server for Claude Code as a project-scoped server (started through
`demo/window/entity-events-mcp.sh`, which finds a JDK 17+). To add it elsewhere:

```bash
claude mcp add entity-events -- java -jar /path/to/mcp/entity-events/target/entity-events-mcp.jar
```
