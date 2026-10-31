# Entity events MCP server

An [MCP](https://modelcontextprotocol.io) server that lets an agent read recent events from Pulsar topics,
filtered on the broker. Its tools follow the shape of Confluent's Real-Time Context Engine
(`listTopics`, `getMetadata`, `queryData`): the agent finds the topics it may read, asks which fields it
can filter on, then reads events with a JMS selector built from those fields.

| Tool | What it does |
|---|---|
| `list_topics` | The topics this server may read (the operator's allowlist, `ENTITY_TOPICS`), each by short and full name |
| `get_schema` | The fields a selector on one topic can name: `__key__`, `__publish_time__`, message headers (with example values) and fields of the value, each with its source, type and cost, plus filtering guidance and an example selector |
| `check_selector` | A dry run of a selector: valid or not (with a fix), the filter that would run, the fields it uses and their cost, and how many of the topic's most recent events it matches, with warnings. Starts no filter |
| `get_entity_events` | Events over a recent time window (`TimeWindow.last(...)`), for one entity's `key`, a `selector`, or both, with the server's completeness status |
| `watch_entity_events` | (spike) The same read kept open (`TimeWindow.since(...)`): history first, then only what arrived since |

`get_entity_events` reads with the Pulsar SDK's windowed consumer. The window is filtered on the server,
so only matching events reach the agent, along with the server's completeness status.

`watch_entity_events` keeps a live windowed read open: the first call (with `key` and/or `selector`)
returns the history, its completeness status and a `watch_id`; each call with that `watch_id` waits up to
`wait_s` (at most `WATCH_MAX_WAIT`, default 60s) and returns only the events that arrived since.
`{watch_id, stop: true}` closes it; a watch with no call for `WATCH_IDLE_TIMEOUT` (default 5m) is closed,
and at most `MAX_WATCHES` (default 5) are open, since each holds a filter on the server. The agent still
makes each call, so it's a long poll on a live subscription.

It speaks MCP over stdio (MCP Java SDK 2.0.1), so any MCP client can start it: Claude Code, the Claude
desktop app, or your own agent.

```bash
make window-demo-build          # builds target/entity-events-mcp.jar with the rest of the demo
make window-demo                # a broker with order traffic to ask about
cd demo/window && claude        # approve the project's entity-events server, then ask:
                                # "What happened to order 8675309 in the last 15 minutes?"
```

## The tools

### `list_topics`

No arguments. Returns `{ "topics": [ { "name": "orders", "topic": "persistent://public/default/orders" } ], "next": "..." }`.

### `get_schema`

| | |
|---|---|
| **In** | `topic`: short or full name from `list_topics` (default: the first topic) |
| **Out** | `topic`, `sampled` (recent messages read), `body_fields_from` (`registered AVRO schema`, `registered JSON schema`, `sampled JSON values` or `none`) |
| | `fields`: `{ name, source, type, cost, examples }`. `source` is `key`, `metadata`, `header` or `body`; `cost` says whether filtering on it decodes the value |
| | `how_to_filter` (plain guidance) and `example`, a selector built from the topic's fields, e.g. `__key__ = '<id>' AND event_type = 'login'` |

Fields follow the filter's own resolution order: the built-ins, then message properties (Kafka headers,
via KoP), then value fields. A header shadows a value field of the same name, so it's listed once, as a
header. Value fields come from the topic's registered Avro or JSON schema when there is one, else from
parsing recent values as JSON; nested fields use dotted paths (`customer.tier`), and arrays, maps and
`_`-prefixed fields are left out. Headers carry no schema, so they're learned from the last
`SCHEMA_SAMPLE` messages. Results are cached per topic for `SCHEMA_CACHE_TTL`.

### `check_selector`

| | |
|---|---|
| **In** | `selector` (required), `topic` (short or full name, default the first topic), `key` (optional, checked with the selector as the read would) |
| **Out** | `valid`; when `false`, `error` is the same message `get_entity_events` would return (see below) |
| | `filter`: the effective selector, `__key__ = '<key>' AND (<selector>)` when a key is given |
| | `fields_used`: `{ name, source, type }`; `cost`: `metadata only`, or `decodes each event's value` when a body field is used |
| | `sampled`, `matched_in_sample` (and `selector_matched_in_sample` with a key): the selector tried on the recent messages `get_schema` sampled |
| | `warnings`: plain sentences, below; `sample_note`: the sample is small and recent |

It starts no filter and opens no SDK session. The only broker read is `get_schema`'s sample, which is
cached, so checking several drafts is cheap. The sample is evaluated with a resolver that mirrors the
filter's (`MessageBindings`): `__key__`, `__publish_time__` when the sample carries it, properties as
strings, then JSON value fields by dotted path, where numeric text reads as a number. Warnings cover:

- comparisons that can't match as written (below);
- a selector that matched every sampled event (likely too broad) or none (with the sampled values of the
  fields it names);
- a filter with no key and no `=` or `IN` on any field, which reads and returns every matching event in
  the window;
- body fields, which decode each event's value, with the topic's headers as cheaper alternatives;
- a `LIKE` pattern that starts with `%`.

A selector on `__publish_time__` isn't tried on samples that don't carry their publish time
(`sample_skipped` says so). Avro values are read as JSON only, so body fields of Avro topics read as absent
in the sample; `sample_note` says so.

### `get_entity_events`

| | |
|---|---|
| **In** | `key`: the entity's key, 1–128 letters, digits or `. _ : -` |
| | `selector`: a JMS selector over the fields `get_schema` returns, e.g. `event_type = 'login' AND device.os = 'ios'` |
| | `topic`: short or full name from `list_topics` (default: the first topic) |
| | `lookback`: how far back to read, e.g. `15m`, `6h` (default `1h`, at most `MAX_LOOKBACK`) |
| | `max_events`: the most events to return (default 1000; the agent can ask for more or fewer, with no server cap). When more match, the tool keeps the newest and lists them oldest first |
| **Out** | `status`: `COMPLETE` or `TRUNCATED_BY_RETENTION`, plus `status_meaning`, a sentence for the agent |
| | `window` (`from`, `to`), `scanned` (events the server read), `matched`, `returned`, `more_events`, and `omitted` (how many older events were left out) when there were more |
| | `events`: `{ time, key, properties, value }`. `time` is when the log received the event, `properties` are the event's own (the filter's `__` ones are dropped), and `value` is parsed JSON when the value is JSON |
| | `selector_warnings`, when the selector has comparisons that can't match as written (below) |

When both `key` and `selector` are given, an event must match both. `watch_entity_events` takes the same
`key`, `selector` and `topic` on its first call.

### How a selector is checked

Before any filter starts, the server:

0. rewrites two SQL habits JMS doesn't share: `!=` becomes `<>`, and a quoted field name (`"event_type"` or
   `` `event_type` ``) loses its quotes where a field name stands. String literals are never touched, and
   double quotes where a value goes (`event_type = "login"`) are left for the parser, whose error says
   strings go in single quotes. A rewrite is reported in `selector_warnings` with the selector as it ran;
1. caps its length (1000 characters);
2. parses it with the same JMS selector parser the broker-side filter uses, so a syntax error comes back
   as a tool error instead of a failed filter. The error carries the parser's detail, where it happened
   ("at character 17 (right after "event_type LIKE")", or "at the end of the selector"), the habit behind
   it when it's a common one (double quotes around a value, `==`, `&&`), and a one-line syntax reminder;
3. checks every identifier against the fields `get_schema` returns for that topic, so a misspelled or
   invented name is rejected instead of silently matching nothing. The error suggests the name meant, when
   one is close: another case (`Event_Type`), without underscores (`key` for `__key__`), the last segment of
   a dotted path (`tier` for `customer.tier`), or one or two edits away (`amout` for `amount`), and points
   to `get_schema`.

Comparisons that are valid but can't match as written are flagged, not rejected (`selector_warnings` on the
event tools, `warnings` from `check_selector`). Under the selector's value rules a string never equals a
number, so `=` is false and `<>` true for every event; ordering a string against a number (`< > <= >=`,
`BETWEEN`) is unknown, which never matches; and `LIKE` and `IN` only match strings. So:

- a header (always a string) or `__key__` compared with a number: `seq > 5` matches nothing, `seq = 5` is
  false for every event; quote the value (`seq = '5'`);
- a numeric body field compared with a quoted string: `amount = '5'` is false for every event; drop the
  quotes. This includes a JSON string field whose values are numeric text (`"price": "77351.21"`), since the
  filter reads such text as a number.

The time window is `lookback`, not a selector term.

Invalid arguments and failures come back as tool errors with a message the agent can act on (for
example "lookback 48h is longer than this server allows (24h)", or a topic that isn't on the allowlist,
with the ones that are).

## What makes it safe to hand to an agent

- **Selectors are parsed and checked, never concatenated unchecked.** An agent's selector is
  length-capped, parsed with the filter's own JMS parser, and may name only fields `get_schema` lists for
  the topic. The key is still validated against `[A-Za-z0-9._:-]{1,128}`, so it can't close the quote it's
  placed in.
- **The server allowlists topics** (`ENTITY_TOPICS`, or `ENTITY_TOPIC` alone); the agent picks among
  them by name and anything else is rejected.
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
| `ENTITY_TOPICS` | unset: comma-separated topics the tools may read; the first is the default |
| `ENTITY_TOPIC` | `persistent://public/default/orders` (the one topic when `ENTITY_TOPICS` is unset) |
| `MAX_LOOKBACK` | `24h` |
| `CALL_TIMEOUT` | `60s` |
| `MAX_WATCHES` | `5` |
| `WATCH_IDLE_TIMEOUT` | `5m` |
| `WATCH_MAX_WAIT` | `60s` |
| `SCHEMA_SAMPLE` | `25`: recent messages `get_schema` reads to learn headers (and value fields, without a registered schema) |
| `SCHEMA_CACHE_TTL` | `5m`: how long a topic's fields are cached |

### Schema registries: Pulsar or Kafka, by how the topic was written

`get_schema` reads a topic's value schema from the registry that matches its storage:

- **Pulsar-written topics:** the Pulsar schema registry (`admin.schemas().getSchemaInfo`).
- **Kafka-written topics (through KoP):** a Confluent-compatible Kafka Schema Registry, KoP's or Confluent's,
  at `GET {url}/subjects/{subject}/versions/latest`.

How a topic counts as Kafka-written, checked in this order: it's listed in `KAFKA_TOPICS` (or `PULSAR_TOPICS`
says otherwise); its newest messages carry KoP's marks (a base64-encoded key, or the
`__ksn_internal_header_format` property); or the topic has a `kafkaEntryFormat` property. Otherwise it's
Pulsar-written. KoP being enabled on the broker isn't enough on its own: Pulsar producers can write to the same
topics.

| Variable | Default |
|---|---|
| `KAFKA_TOPICS` | unset: comma-separated topics (full or short name) treated as Kafka-written, without detection |
| `PULSAR_TOPICS` | unset: comma-separated topics treated as Pulsar-written, without detection |
| `KAFKA_SCHEMA_REGISTRY_URL` | unset: no registry, so a Kafka topic's value fields are learned from samples |
| `KAFKA_SCHEMA_REGISTRY_SUBJECT` | `{topic}-value`, where `{topic}` is the topic's short name (`-partition-N` stripped) |
| `KAFKA_SCHEMA_REGISTRY_USERNAME`, `KAFKA_SCHEMA_REGISTRY_PASSWORD` | unset: basic auth |
| `KAFKA_SCHEMA_REGISTRY_TOKEN` | unset: bearer token, used instead of basic auth if both are set |

**Kafka serializer wire format:** values a Kafka serializer framed for a registry (a `0x00` byte and a 4-byte
schema id before the payload) can't be read by the filter yet: it parses the raw value as JSON. For such topics
`get_schema` offers only the key and headers, and says why in `body_fields_from`.

stdout carries the protocol; logs go to stderr. The SDK session and the admin client open on the first
call that needs them, so the agent can start the server before the broker is up.

`demo/window/.mcp.json` registers the server for Claude Code as a project-scoped server (started through
`demo/window/entity-events-mcp.sh`, which finds a JDK 17+). To add it elsewhere:

```bash
claude mcp add entity-events -- java -jar /path/to/mcp/entity-events/target/entity-events-mcp.jar
```
