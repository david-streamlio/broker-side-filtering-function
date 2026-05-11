# Selector Filter Function for Apache Pulsar

A generic [Pulsar Function](https://pulsar.apache.org/docs/functions-overview/) that consumes raw
`byte[]` messages from one or more topics, evaluates a set of JMS selector expressions against each
message's **key**, **source topic**, and **user properties**, and routes matches to per-query output
topics.

It is a generalization of single-purpose filter functions that hardcode the filter shape. Queries
are added, updated, and retracted at runtime via a dedicated **control topic** — no redeploy is
required to change which messages get routed where.

## How it works

1. The function subscribes to one or more **input topics** (explicit list or `topicsPattern`).
2. It builds a `TableView<byte[]>` over a single **control topic**. Each compacted record on the
   control topic is a JSON `QueryMessage` keyed by query id.
3. For every input message, the function evaluates **every active query's selector** against the
   message. Each match results in an asynchronous send to that query's `outputTopic`. The payload
   bytes are forwarded verbatim, along with all user properties and the original key.
4. The function uses `processingGuarantees: MANUAL`. The source record is acked only after every
   fan-out send succeeds; a single send failure causes `record.fail()` and the broker redelivers.

## Selector language

Queries use **JMS selector** syntax — a subset of SQL92 `WHERE` clauses operating on typed
properties. The parser is provided by DataStax `pulsar-jms-filters-common` (which itself reuses
Apache ActiveMQ's selector implementation).

### Bindings available to selectors

| Binding | Source | Notes |
|---|---|---|
| `__key__` | `record.getMessage().get().getKeyBytes()` decoded as UTF-8 | The **logical** message key. For KoP-bridged messages with `kafkaEntryFormat=pulsar_non_batched`, the base64-encoded Pulsar key is transparently decoded back to the original Kafka bytes — no per-topic configuration required. Falls back to `record.getKey()` when no underlying message is available. May be `null`. |
| `__topic__` | `record.getTopicName()` | The fully-qualified source topic with any `-partition-N` suffix stripped. |
| Any user property | `record.getProperties()` | Producers attach filterable attributes via `producer.newMessage().property(name, value)`. KoP surfaces Kafka headers as Pulsar string properties; binary headers are bridge-encoded by KoP itself and visible to selectors as their string form. |

### Examples

```sql
-- Match by key
__key__ = 'AAPL'
__key__ IN ('AAPL','MSFT','GOOGL')
__key__ LIKE 'US-%'

-- Match by source topic (useful when subscribed to a topicsPattern)
__topic__ LIKE 'persistent://public/trades/%'
__topic__ = 'persistent://public/quotes/ask'

-- Match by user properties
region = 'US' AND marketcap_bucket = 'LARGE'
asset_class IN ('EQ','FX','RATES')
sentiment LIKE 'BULL%'

-- Combinations
__topic__ LIKE '%/trades/%' AND region IN ('US','UK') AND priority_tier = 'GOLD'
```

### Kafka-on-Pulsar (KoP) compatibility

The function is source-format agnostic. A Kafka producer that sends `key = b"BTC"` to a KoP topic
configured with `kafkaEntryFormat=pulsar_non_batched` produces a Pulsar message whose stored key is
base64 (`"QlRD"`), with `Message.hasBase64EncodedKey()=true`. Because the function reads the key
through `Message.getKeyBytes()` — which the Pulsar client transparently decodes — the selector
`__key__ = 'BTC'` matches in both KoP and native cases. No per-topic configuration or external
metadata is needed: every message carries its own decode hint in `MessageMetadata`.

**Kafka header → Pulsar property** conversion is performed by KoP at ingest time and has lossy
edges the function does not try to hide:

| KoP behavior | What selectors see |
|---|---|
| Multiple Kafka headers with the same key | Only the **latest** value survives. Selectors evaluating against that header compare against the latest. |
| Header value is valid UTF-8 | Property value is the UTF-8 string (e.g. `header-value`). Selectors write literal string comparisons: `header_key = 'header-value'`. |
| Header value is **not** valid UTF-8 | Property value is the **base64** encoding (e.g. `iA==` for `0x88`). Selectors must compare against the base64 string: `binary_header = 'iA=='`. The bytes can't be losslessly represented as a Pulsar string property otherwise. |
| Header value's bytes are themselves a valid base64 string | KoP injects an extra property `__ksn_internal_header_format` containing a JSON map that disambiguates which header values are base64 vs UTF-8. The function **hides this property from selector lookups** (`__ksn_internal_header_format IS NOT NULL` will never match) but **preserves it in the forwarded property map** so downstream KoP-aware tools can still decode correctly. |
| Property is flagged base64-encoded in `__ksn_internal_header_format` | The function **transparently decodes** the property: base64 → bytes → UTF-8 string (if valid). Selectors compare against the decoded form (e.g. `header_key = 'key'` instead of `header_key = 'a2V5'`). Decoded values are cached per record so repeated lookups across many queries reuse the same `String` instance. If the decoded bytes are not valid UTF-8 (true binary headers), the function falls back to surfacing the raw base64 string and selectors must compare against that. |

Behavior by `kafkaEntryFormat` when a **Kafka producer** writes to the topic and the **function** (a native Pulsar consumer) is meant to filter it:

| `kafkaEntryFormat` | Native Pulsar consumer can read? | This function works? | Notes |
|---|---|---|---|
| `pulsar` (default) | **no** for Kafka-produced records | **no** | Kafka records are stored as **batched** Pulsar messages designed for Kafka clients. Native Pulsar consumers log `BatchDeSerializeError` and drop them. Pulsar-produced records DO work. |
| `pulsar_non_batched` | yes in principle | **no on KoP 4.2.0.6** | The function's key-decode logic is unit-tested and correct (`MessageBindingsTests.fromRecordDecodesKoPBase64Key`). However, observed KoP 4.2.0.6 behavior emits messages with batch-envelope framing whose `SingleMessageMetadata` omits required fields. The Pulsar Function's `ConsumerImpl` raises `IllegalStateException: Some required fields are missing` at deserialization — the message never reaches our function code. **Verified live in `KoPFilterFunctionIT.pulsarNonBatchedFormatProducesMessagesIncompatibleWithFunctionConsumer`** (recorded as a negative test). A future KoP version that emits strictly non-batched envelopes, or a native Pulsar producer that explicitly sets `hasBase64EncodedKey`, will exercise the happy path. |
| `kafka` | no | **no** | Records stored in Kafka native wire format — only Kafka clients can consume. Selector-filtering this topic is impossible regardless of selector. |

Live KoP coverage runs in `function/src/test/java/.../KoPFilterFunctionIT.java` against
[`streamnative/sn-platform:4.2.0.6`](https://hub.docker.com/r/streamnative/sn-platform), which
bundles the KSN (Kafka StreamNative) protocol handler. Three tests cover the matrix:

| Test | What it proves |
|---|---|
| `pulsarFormatTopicIsNotConsumableFromNativePulsarFunction` | With `kafkaEntryFormat=pulsar` (default), Kafka-produced records are stored as batched Pulsar messages; the function logs `BatchDeSerializeError` and produces zero output. Documents the limitation. |
| `pulsarNonBatchedFormatTopicFiltersByDecodedKey` | With `kafkaEntryFormat=pulsar_non_batched`, KoP base64-encodes the Pulsar key but the function's `Message.getKeyBytes()` path decodes transparently — `__key__ = 'BTC'` matches. **Happy path for Kafka↔function interop.** |
| `kafkaFormatTopicIsNotReadableByFunctionAndProducesNoOutput` | With `kafkaEntryFormat=kafka`, the function (a native Pulsar consumer) cannot read the records; selector-filtering on these topics is impossible. The test asserts zero output appears within 20 seconds. |

Each test spins up a fresh container (since `kafkaEntryFormat` is a global broker setting) and uses
`PULSAR_PREFIX_*` env vars to inject KoP config into `broker.conf`. Requires Docker. The regex
`topicsPattern` integration test in `RegexPatternIT.java` is independent and runs against a stock
Pulsar testcontainer.

### Important constraints

1. **Pulsar user properties are always `String`.** ActiveMQ JMS selector semantics return `UNKNOWN`
   (no match) when comparing a `String` property to a numeric literal. Compare strings to strings:
   `price = '150.5'`, not `price > 150`. If you need numeric comparisons, encode buckets:
   `marketcap_bucket = 'LARGE'`.
2. **No JMS pseudo-headers.** This function exposes only `__key__`, `__topic__`, and user
   properties to the selector. Names such as `JMSPriority`, `JMSExpiration`, `JMSTimestamp`,
   `JMSMessageID`, `JMSDestination`, etc. are not supported and should not appear in selector
   expressions. If a name happens to collide with a Pulsar user property, it is looked up like
   any other property — no special handling.
3. **Selectors are compiled once per query** at the time the query appears in the TableView, and
   cached until retraction. Hot reload is supported: re-publishing on the same query-id key
   replaces the compiled selector.

## Control-topic schema

Each query is a JSON document published to the control topic, **keyed by the query id**:

```json
{
  "name": "us-large-cap-trades",
  "selector": "__topic__ LIKE 'persistent://public/trades/%' AND region = 'US'",
  "outputTopic": "persistent://public/filtered/us-large-cap"
}
```

Fields:

| Field | Required | Description |
|---|---|---|
| `name` | optional | Human-readable label used in logs. |
| `selector` | **required** | JMS selector expression. |
| `outputTopic` | **required** | Fully-qualified Pulsar topic to fan out matches to. |

### Adding a query

```bash
pulsar-client produce persistent://public/control/queries \
  --key us-large-cap-trades \
  --files deployment/sample-query.json
```

### Updating a query

Publish a new JSON value with the **same key**. The compiled selector is replaced in place.

### Retracting a query

Publish a message with the same key and an empty or `null` body. The function will remove the
compiled selector and stop fanning out matches.

Using the Pulsar Java client:

```java
producer.newMessage().key("us-large-cap-trades").value(null).send();
```

## Deployment

Two sample configurations are provided in `deployment/`.

### Explicit input topics

```bash
pulsar-admin functions create \
  --function-config-file deployment/selector-filter-function-config.yaml \
  --jar target/selector-filter-function-1.0.0-SNAPSHOT.nar
```

### Pattern-based input topic discovery

```bash
pulsar-admin functions create \
  --function-config-file deployment/selector-filter-function-config-pattern.yaml \
  --jar target/selector-filter-function-1.0.0-SNAPSHOT.nar
```

### Configuration reference

| Setting | Purpose |
|---|---|
| `inputs` *or* `topicsPattern` | What topics to filter. Standard Pulsar Function config. |
| `userConfig.control-topic` | **Required.** Topic that carries the active query set. |
| `processingGuarantees: MANUAL` | The function explicitly acks/fails each record. |
| `retainKeyOrdering: true` | Preserve per-key ordering across the function boundary. |
| `forwardSourceMessageProperty: true` | User properties are propagated to outputs. |
| `cleanupSubscription: true` | Function's subscription is deleted on function-delete. |

### Control-topic setup

The control topic should be **non-partitioned and compacted** so the `TableView` stays small and
the function does not replay full history on restart.

```bash
pulsar-admin topics create persistent://public/control/queries
pulsar-admin topics set-compaction-threshold persistent://public/control/queries -t 10K
pulsar-admin topicPolicies set-retention persistent://public/control/queries --size -1 --time -1
```

## Building

Requires JDK 17+ and Maven 3.9+.

```bash
# unit tests
mvn -B clean test

# build the NAR
mvn -B clean package -DskipTests

# unit + integration tests (requires Docker)
mvn -B clean verify

# docker image
mvn -B clean package -Pdocker
```

The function NAR appears at `function/target/selector-filter-function-${VERSION}.nar` and is the
artifact you register with `pulsar-admin functions create --jar`. The client jar is at
`client/target/selector-filter-client-${VERSION}.jar`.

## Client library — `Session` + `FilterControl`

The `selector-filter-client` module ships a `Session` context plus a builder that returns a
standard `org.apache.pulsar.client.api.Consumer<byte[]>` subscribed to its own private output
topic. The returned consumer also implements `FilterControl`, so callers can swap the active
selector at runtime without recreating the subscription.

```xml
<dependency>
    <groupId>io.streamnative</groupId>
    <artifactId>selector-filter-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Basic usage

```java
try (Session session = Session.builder()
        .serviceUrl("pulsar://broker:6650")
        .adminUrl("http://broker:8080")
        .controlTopic("persistent://public/control/queries")
        .privateTopicTenant("public")
        .privateTopicNamespace("filtered")
        .build();
     Consumer<byte[]> consumer = session.newConsumer()
             .topic("persistent://public/quotes/trades")
             .selector("region = 'US' AND marketcap_bucket = 'LARGE'")
             .subscribe()) {

    // Standard Pulsar Consumer API
    Message<byte[]> msg = consumer.receive();
    System.out.println(new String(msg.getData()));
    consumer.acknowledge(msg);
}
```

What happens inside `subscribe()`:
1. The user selector is parsed against the same JMS-selector grammar the function uses. A
   malformed selector throws `IllegalArgumentException` *before* any network I/O.
2. A fresh `queryId` (UUID) is generated.
3. A private output topic `persistent://{tenant}/{namespace}/filtered-{queryId}` is created and
   given aggressive TTL / retention / inactive-cleanup policies via `PulsarAdmin`.
4. The session composes the effective selector as `__topic__ = '...' AND (<user selector>)` and
   publishes a `QueryMessage` to the control topic, keyed by `queryId`.
5. A native Pulsar `Consumer<byte[]>` is subscribed to the private topic and returned, wrapped so
   it also implements `FilterControl`.

What happens on `consumer.close()`:
1. The underlying Pulsar consumer is closed.
2. A tombstone (null body) is published to the control topic with the same `queryId`, prompting
   the function to retract the query. Tombstoning still happens if the underlying close throws.
3. If `deleteOnClose=true` (the default), the private topic is deleted via `PulsarAdmin`.

### Dynamic selector updates — `FilterControl`

Cast the returned `Consumer<byte[]>` to `FilterControl` to inspect or replace its selector at
runtime. The same `queryId` is reused — the function hot-reloads the compiled expression in
place; the consumer keeps reading from the same private output topic.

```java
Consumer<byte[]> consumer = session.newConsumer()
        .topic("persistent://public/quotes/trades")
        .selector("region = 'US'")
        .subscribe();

FilterControl fc = (FilterControl) consumer;
fc.setSelector("region IN ('US','UK')");      // hot reload — same queryId
fc.setSelector(null);                          // drop user filter — topic-match only

System.out.println(fc.queryId());              // stable for the lifetime of the consumer
System.out.println(fc.currentSelector());      // current user-side selector (may be null)
System.out.println(fc.sourceTopics());         // immutable list passed at build time
System.out.println(fc.privateTopic());         // private output topic
```

Each consumer holds **exactly one** active selector. To register additional independent filters,
build additional consumers from the same `Session`.

`setSelector(...)` validates the new expression with the same parser as `subscribe()`; if it is
malformed, it throws `IllegalArgumentException` and the previous selector remains active.

### Multi-source consumers

`session.newConsumer().topics(Collection<String>)` renders the source constraint as a JMS `IN`
clause:

```java
session.newConsumer()
    .topics(List.of("persistent://public/trades/eq", "persistent://public/trades/fx"))
    .selector("region IN ('US','UK')")
    .subscribe();
// Composed selector:
//   __topic__ IN ('persistent://public/trades/eq','persistent://public/trades/fx')
//   AND (region IN ('US','UK'))
```

### Builder reference

| Method | Default | Meaning |
|---|---|---|
| `topic(String)` / `topics(Collection<String>)` | — | At least one is required. Calling both keeps only the latest. |
| `selector(String)` | `null` | User-side JMS selector. `null` or blank means topic-match only. |
| `subscriptionName(String)` | `session-<uuid>` | Subscription name on the private topic. |
| `subscriptionType(SubscriptionType)` | `Exclusive` | Subscription type for the underlying consumer. |
| `subscriptionInitialPosition(SubscriptionInitialPosition)` | `Earliest` | Initial position. |

### Tuning private-topic cleanup

The `PrivateTopicPolicy` controls how aggressively private topics are reaped:

| Field | Default | Meaning |
|---|---|---|
| `messageTtlSeconds` | 300 | Messages older than this expire from the backlog. |
| `retentionTimeMinutes` | 5 | Retention time floor for the topic. |
| `retentionSizeMB` | 64 | Retention size cap. |
| `inactiveDeleteSeconds` | 600 | Pulsar deletes the topic after this long with no active subscriptions. Backstop if the client dies without calling `close()`. |
| `deleteOnClose` | true | When `false`, leave the topic alone on `close()` and rely solely on the inactive-cleanup policy. |
| `applyPerTopicPolicies` | true | When `false`, do not touch per-topic policies and fall back to namespace defaults (useful when admin permissions are restricted). |

Override individual fields via `PrivateTopicPolicy.builder()`:

```java
Session.builder()
    .privateTopicPolicy(PrivateTopicPolicy.builder()
        .messageTtlSeconds(60)
        .inactiveDeleteSeconds(120)
        .build())
    ...
```

### Selector translation

Selectors are parsed by `com.datastax.oss:pulsar-jms-filters-common`, which wraps Apache
ActiveMQ's `SelectorParser`. The **client** uses the same library to validate selector syntax at
`subscribe()` / `setSelector()` time — so malformed expressions fail locally with
`IllegalArgumentException` instead of being silently dropped as bad queries on the function's
control topic. The **function** compiles and caches the same expression for O(1) per-message
evaluation.

### Key-only fast path

When a query's selector references only the `__key__` binding (e.g. `__key__ IN ('AAPL','MSFT')`),
the function avoids resolving the source topic and user-property map for that record's evaluation.
This is observable via `SelectorEvaluator.referencesOnlyKey()`, useful for telemetry or eager
profiling of registered queries. The optimization is automatic and transparent — clients don't
have to opt in.

## Operations

### Metrics

The function emits the following metrics via `Context.recordMetric(...)`. Pulsar exposes them on
the function-worker Prometheus endpoint; compute rates in your monitoring system with `rate()` or
`increase()` over your preferred time window (e.g. `rate(filter_messages_in[1m])`).

**Per input message** (exactly one of `matched` or `dropped` fires per `in`):

| Metric | Unit | Recorded as | Meaning |
|---|---|---|---|
| `filter.messages.in` | count | `+1` per input | Total input records seen. |
| `filter.bytes.in` | bytes | `+payload.length` per input | Total input bytes seen. |
| `filter.messages.matched` | count | `+1` per matching input | Input records that matched at least one query. |
| `filter.messages.dropped` | count | `+1` per non-matching input | Input records that matched no query. |
| `filter.bytes.dropped` | bytes | `+payload.length` per non-matching input | Bytes dropped. |
| `filter.messages.out` | count | `+fanout` per matching input | Total fan-out sends across all matching queries. |
| `filter.bytes.out` | bytes | `+payload.length * fanout` per matching input | Total bytes published to output topics. |
| `filter.send.failures` | count | `+1` per failing input | Records where one or more output sends failed (record is then `record.fail()`'d for redelivery). |

**Per-message latency** (when enabled):

| Metric | Unit | Meaning |
|---|---|---|
| `filter.processing.time.ms` | sample (double, ms) | Wall-clock time from `process()` entry to ack/fail completion. Recorded as a sample so dashboards can compute p50/p95/p99. **Disable via `userConfig.enable-processing-time-metric=false`** if profiling shows the metric itself is a bottleneck at extreme throughput — the function then makes zero `System.nanoTime()` and zero `recordMetric` calls on the timing path. Default: enabled. |

**Query lifecycle**:

| Metric | Recorded as | Meaning |
|---|---|---|
| `filter.queries.loaded` | `+1` per successful load | Count of queries compiled and added (includes updates that overwrite an existing entry). |
| `filter.queries.retracted` | `+1` per retraction | Count of tombstones that removed a previously loaded query. |
| `filter.queries.bad` | `+1` per rejection | Malformed JSON, missing required field, or invalid selector expression. |
| `filter.queries.active` | gauge | Current active query count; re-recorded after every load/retract. |

**Derived dashboard expressions** (Prometheus / equivalent):

```
match rate              = rate(filter_messages_matched[1m]) / rate(filter_messages_in[1m])
drop rate               = rate(filter_messages_dropped[1m]) / rate(filter_messages_in[1m])
average fan-out         = rate(filter_messages_out[1m]) / rate(filter_messages_matched[1m])
input throughput (B/s)  = rate(filter_bytes_in[1m])
output throughput (B/s) = rate(filter_bytes_out[1m])
bad-query rate          = rate(filter_queries_bad[1m])
```

Function logs include a heartbeat every `LOG_FREQUENCY` (100,000) matched messages:
`Filtered N messages (dropped=M, active queries=K)`.

### Loop guard

If any query's `outputTopic` is also one of the function's input topics, a `WARN` is logged at
startup. The function does **not** refuse to start — operators are responsible for breaking the
cycle.

### Backpressure & failure handling

A failed send on any matching output topic causes `record.fail()`, and the broker redelivers the
input message. There is no per-output dead-letter queue in this version; if you need one, use a
downstream consumer or extend the function.

### Selector evaluation cost

Cost per message is `O(N)` in the active query count `N`. For `N` in the low thousands this is
fine. If `N` grows much beyond that, consider sharding queries across multiple function instances
with different control topics.

## Project layout

```
pom.xml                                       -- parent (multi-module)
common/                                       -- shared QueryMessage type
  src/main/java/io/streamnative/filter/common/QueryMessage.java
function/                                     -- the broker-side Function (NAR)
  pom.xml
  src/main/java/io/streamnative/filter/
    SelectorFilterFunction.java
    SelectorEvaluator.java                    -- compile + match + binding-requirement analysis
    CompiledQuery.java
    MessageBindings.java                      -- lazy __key__/__topic__/properties resolver
    TopicNames.java
  src/main/resources/META-INF/services/pulsar-io.yaml
  src/main/docker/Dockerfile
  deployment/
client/                                       -- Session-style client library (JAR)
  pom.xml
  src/main/java/io/streamnative/filter/client/
    Session.java
    SessionBuilder.java
    FilterableConsumerBuilder.java
    FilteringPulsarConsumer.java              -- Consumer<byte[]> + FilterControl wrapper
    FilterControl.java                        -- dynamic-selector cast interface
    PrivateTopicPolicy.java
    Selectors.java                            -- compose + validate selector syntax
```

## References

- [Apache Pulsar Functions](https://pulsar.apache.org/docs/functions-overview/)
- [Pulsar TableView](https://pulsar.apache.org/docs/client-libraries-tableviews/)
- [JMS 2.0 message selectors (Oracle docs)](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html)
- [DataStax pulsar-jms](https://github.com/datastax/pulsar-jms)
- [Apache ActiveMQ selector grammar](https://activemq.apache.org/components/classic/documentation/selectors)
