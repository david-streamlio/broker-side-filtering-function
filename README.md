# Selector Filter Function for Apache Pulsar

Broker-side message filtering for Apache Pulsar. SDK clients ask for "filtered output of topic X
where this JMS selector matches" and get back a real Pulsar/Kafka consumer subscribed to a private
output topic carrying only matching records. A pair of Pulsar functions makes this work without
clients knowing anything about the wiring:

- **`selector-filter`** — a generic [Pulsar Function](https://pulsar.apache.org/docs/functions-overview/)
  that consumes raw `byte[]` messages, evaluates JMS-selector expressions against each message's
  **key**, **source topic**, and **user properties**, and routes matches to per-query output
  topics.
- **`orchestrator`** — a Pulsar function that listens on the SDK-facing control topic, maintains a
  per-input-topic registry of running filter-function instances, spawns new filter functions on
  demand, and routes per-query control messages to the appropriate filter instance. Detects schema
  mismatches across registrations on the same input topic and rejects them.

The pair gives operators **one deployable orchestrator** that auto-provisions per-input-topic
filter functions as SDK clients subscribe — no manual function-per-topic deployment.

> **Pre-1.0 — clean break.** The orchestrator is required in this version. The previous
> SDK-publishes-directly-to-filter-function flow has been removed: SDK clients now publish
> `FilterRequest` records to the orchestrator's input topic, and the orchestrator is the only
> thing that publishes `QueryMessage` records to filter functions. The legacy direct-deploy
> YAMLs under `functions/selector-filter/deployment/` are kept for reference but are not how
> SDK-driven workloads are deployed.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                  SDK clients                                    │
│         (Pulsar SDK / Kafka SDK over KoP / Python / C++ — future)               │
│                                                                                 │
│      session.newConsumer().topic(...).selector(...).subscribe()                 │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │  FilterRequest{command:REGISTER|MODIFY|UNREGISTER, ...}
                                       │  key = queryId
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│        SDK → Orchestrator topic   (e.g. persistent://public/filters/             │
│                                          filter-requests)                       │
└──────────────────────────────────────┬──────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  Orchestrator function (this repo's functions/orchestrator)                     │
│   - Looks up input topic in the per-input-topic registry (TableView-backed)     │
│   - Spawns a new selector-filter function instance for unknown input topics     │
│   - Rejects schema mismatches on existing input topics                          │
│   - Publishes per-query QueryMessage to the matching filter's control topic     │
└─────────┬───────────────────────────────────────────────────────────────────────┘
          │  spawn via admin.functions().createFunctionWithUrl()
          │  QueryMessage{name, selector, outputTopic} → per-function control topic
          ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│  selector-filter function instance(s)  — one per input topic                    │
│   - Reads its control topic via TableView; one entry per active query           │
│   - For each input record, evaluates every active query's selector              │
│   - Forwards matches to that query's outputTopic (the SDK's private topic)      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## How it works (selector-filter, per-instance)

1. Each spawned `selector-filter` instance subscribes to one **input topic** (or one
   `topicsPattern`).
2. It builds a `TableView<byte[]>` over its **per-instance control topic**. Each compacted
   record on the control topic is a JSON `QueryMessage` keyed by query id.
3. For every input message, the function evaluates **every active query's selector** against the
   message. Each match results in an asynchronous send to that query's `outputTopic`. The payload
   bytes are forwarded verbatim, along with all user properties and the original key.
4. The function uses `processingGuarantees: MANUAL`. The source record is acked only after every
   fan-out send succeeds; a single send failure causes `record.fail()` and the broker redelivers.

## How it works (orchestrator)

1. Orchestrator subscribes to its single **SDK-facing input topic** (the one
   `Session.controlTopic(...)` points at).
2. Maintains a `TableView<byte[]>` on a **state topic** with two keyspaces:
   - `t:<inputTopicFqnOrPattern>` → `RegistryEntry` recording the per-instance control topic
     and serializer FQNs (for schema-mismatch detection).
   - `q:<queryId>` → `QueryEntry` recording the SDK's outputTopic and the list of per-function
     control topics this queryId was fanned out to.
3. On `FilterRequest{command:REGISTER, ...}`: looks up `t:<inputTopic>`. If absent, spawns a new
   `selector-filter` function via `admin.functions().createFunctionWithUrl(...)` referencing a
   pre-uploaded Pulsar package (operator-configured `function-package-url`). If present + serializer
   match, reuses. If present + serializer mismatch, rejects.
4. On `MODIFY`: republishes the QueryMessage on the same queryId across every per-function control
   topic the original REGISTER fanned out to — filter functions hot-reload via TableView.
5. On `UNREGISTER`: publishes tombstones (null body) on every per-function control topic the
   queryId was registered on. Idempotent on unknown queryIds.

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

Live KoP coverage runs in `functions/selector-filter/src/test/java/.../KoPFilterFunctionIT.java` against
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

Production deployment uses the **orchestrator** to auto-spawn per-input-topic filter functions
on demand. Operators deploy the orchestrator once; SDKs do the rest. The legacy direct-deploy
mode (deploy the filter NAR yourself, point a pre-existing SDK at its control topic) still works
but is no longer how new clusters should be set up.

### Infrastructure-as-Code

The recommended path is the Terraform wrappers under [`deploy/`](deploy/). Two targets ship:

- **StreamNative Cloud** — managed; bring your own Kafka-enabled instance. See
  [`deploy/terraform/sn-cloud/README.md`](deploy/terraform/sn-cloud/README.md).
- **Pulsar Helm chart on Kubernetes** — self-hosted; includes a KoP broker-values snippet for
  cross-format. See [`deploy/terraform/pulsar-helm/README.md`](deploy/terraform/pulsar-helm/README.md).

Both compose the same shared modules in [`deploy/terraform/modules/`](deploy/terraform/modules/) —
operators with bespoke deployment pipelines can use the modules directly.

The manual `pulsar-admin` steps below remain accurate for ad-hoc setups, debugging, and operators
who do not use Terraform.

### Orchestrator-managed deployment (recommended)

1. **Pre-create the SDK input + orchestrator state topics:**

   ```bash
   pulsar-admin topics create persistent://public/filters/filter-requests
   pulsar-admin topics create persistent://public/filters/orchestrator-state
   pulsar-admin topicPolicies set-retention persistent://public/filters/filter-requests --size -1 --time -1
   ```

2. **Upload the selector-filter NAR as a Pulsar package** — the orchestrator will reference
   this package URL when spawning filter functions:

   ```bash
   pulsar-admin packages upload \
     function://public/filters/selector-filter@1.0 \
     --path functions/selector-filter/target/selector-filter-function-1.0.0-SNAPSHOT.nar \
     --description "selector-filter NAR"
   ```

3. **Deploy the orchestrator function:**

   ```bash
   pulsar-admin functions create \
     --function-config-file functions/orchestrator/deployment/orchestrator-function-config.yaml \
     --jar functions/orchestrator/target/selector-filter-orchestrator-1.0.0-SNAPSHOT.nar
   ```

4. **Configure the SDK** to publish to the orchestrator's input topic:

   ```java
   Session session = Session.builder()
       .controlTopic("persistent://public/filters/filter-requests")
       ...
   ```

The orchestrator handles the rest — it spawns one `selector-filter` function instance per unique
input topic that any SDK client subscribes to. Filter functions stay running across SDK
disconnects; idle reclamation is a v2 follow-up.

### Orchestrator configuration reference

See [`functions/orchestrator/deployment/orchestrator-function-config.yaml`](functions/orchestrator/deployment/orchestrator-function-config.yaml)
for a fully-annotated sample. Key `userConfig` knobs:

| Key | Required | Purpose |
|---|---|---|
| `state-topic`            | **yes** | Compacted topic backing the orchestrator's per-input-topic + per-queryId registry. |
| `function-package-url`   | **yes** | Pulsar package URL of the pre-uploaded selector-filter NAR (`function://tenant/ns/name@version`). |
| `function-tenant`        | **yes** | Tenant under which spawned filter functions are deployed. |
| `function-namespace`     | **yes** | Namespace for spawned filter functions. |
| `pulsar-admin-url`       | **yes** | Pulsar HTTP admin URL the orchestrator uses to spawn functions (`http://broker:8080`). Required because `ctx.getPulsarAdmin()` isn't available in lightweight worker configs. |
| `function-control-topic-tenant`    | no | Tenant for per-filter-function control topics (defaults to `function-tenant`). |
| `function-control-topic-namespace` | no | Namespace for per-filter-function control topics (defaults to `function-namespace`). |
| `function-parallelism`     | no | Parallelism applied to each spawned filter function. Default 1. |
| `apply-compaction`         | no | Whether the orchestrator programmatically sets `compactionThreshold` on each per-function control topic. Default `true`. |
| `compaction-threshold-bytes` | no | Compaction threshold to apply when `apply-compaction=true`. Default 10 MB. |
| `idle-gc-enabled`          | no | Master switch for idle-filter GC. When `true`, the orchestrator runs a background sweeper that deletes filter functions whose queryIds have all UNREGISTERed for at least `idle-gc-ttl-seconds`. Default `false` (opt-in). See [Idle filter GC](#idle-filter-gc) below. |
| `idle-gc-ttl-seconds`      | no | Continuous-idle time before a filter function becomes deletion-eligible. Default 300 (5 minutes). Only applies when `idle-gc-enabled=true`. |
| `idle-gc-sweep-interval-seconds` | no | How often the sweeper evaluates registry entries. Default 30 seconds. Only applies when `idle-gc-enabled=true`. |
| `heartbeat-max-stale-seconds` | no | Maximum allowed gap between SDK heartbeats before the orchestrator synthesizes an UNREGISTER for a queryId. Default 90 (three missed heartbeats at the SDK default 30-second interval). Only applies when `idle-gc-enabled=true`. See [Dead-consumer detection](#dead-consumer-detection) below. |

### Deployment prerequisites

- **Orchestrator parallelism MUST be 1** in v1. Multiple instances race on spawn decisions for
  the same input topic. v2 will support sharding via leader election or KeyShared partitioning.
- **The orchestrator's runtime identity needs functions admin** on the configured
  `function-tenant`/`function-namespace` so it can call `admin.functions().createFunctionWithUrl(...)`.
  In production with auth enabled, attach a service-account role with at least `functions` admin.
- **Function worker configuration**: the orchestrator constructs its own `PulsarAdmin` from
  `pulsar-admin-url` and does not depend on `ctx.getPulsarAdmin()`. No special worker config needed.

### Legacy direct-deploy mode

Operators who prefer to deploy filter functions manually (one per input topic, no orchestrator)
can still do so — the filter function is unchanged. See
[`functions/selector-filter/deployment/`](functions/selector-filter/deployment/) for the legacy
config samples. The SDK in this version requires the orchestrator, however; using direct-deploy
mode means writing your own `QueryMessage` producer instead of using `Session`.

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

### Performance testing

The `perf/` module hosts ShadowTraffic-driven load tests. They are **off by default** — the
suite takes 20–30 minutes and depends on a paid ShadowTraffic license.

```bash
# Phase-2 smoke test (1K records through KoP) — requires SHADOWTRAFFIC_LICENSE_* env vars.
export SHADOWTRAFFIC_LICENSE_ID=...
export SHADOWTRAFFIC_LICENSE_EMAIL=...
export SHADOWTRAFFIC_LICENSE_ORGANIZATION=...
export SHADOWTRAFFIC_LICENSE_EDITION=...
export SHADOWTRAFFIC_LICENSE_EXPIRATION=...
export SHADOWTRAFFIC_LICENSE_SIGNATURE=...

mvn -Pperf -pl perf verify
```

Without the license env vars, JUnit skips the perf ITs cleanly via
`@EnabledIfEnvironmentVariable`, so unmodified CI runs continue to pass. See
`tasks/todo-perf-shadowtraffic.md` for the full backlog (additional benchmark scenarios,
CI integration, regression-chart plumbing).

The function NAR appears at
`functions/selector-filter/target/selector-filter-function-${VERSION}.nar` and is the artifact
you register with `pulsar-admin functions create --jar`. The Pulsar client jar is at
`sdk/java/pulsar/target/selector-filter-client-${VERSION}.jar`; the Kafka client jar at
`sdk/java/kafka/target/selector-filter-client-kafka-${VERSION}.jar`.

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

## Kafka client library — `FilterableKafkaConsumers`

The `selector-filter-client-kafka` module wraps a real
`org.apache.kafka.clients.consumer.KafkaConsumer` pointed at a **KoP**-enabled Pulsar broker so
Kafka apps can subscribe to filtered output topics. The returned consumer implements both the
Kafka `Consumer<K, V>` interface and `FilterControl` — same dynamic-selector API as the Pulsar
SDK, accessed by cast.

```xml
<dependency>
    <groupId>io.streamnative</groupId>
    <artifactId>selector-filter-client-kafka</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

### Deployment requirements

| Requirement | Why |
|---|---|
| Broker has KoP enabled with `messagingProtocols=kafka` | so Kafka clients can connect at all |
| **`kafkaEntryFormat=pulsar_non_batched`** | the only setting where a native Pulsar producer (what the filter function writes with) produces messages that Kafka consumers can read. Validated end-to-end against KoP 4.2.0.6 in `KoPOutputFormatSpikeIT`. |
| `Session.privateTopicTenant` / `privateTopicNamespace` match KoP's `kafkaTenant`/`kafkaNamespace` | so the per-consumer private output Pulsar topic is visible as a Kafka topic. v1 derives the Kafka name as the **last segment** of the private Pulsar FQN — e.g. `persistent://public/default/filtered-<uuid>` → `filtered-<uuid>`. |

The previously documented KoP 4.2.0.6 `pulsar_non_batched` framing bug is **asymmetric**: it only
affects the *input* direction (Kafka producer → Pulsar function-as-consumer), not the *output*
direction this SDK uses (Pulsar producer → Kafka consumer). Kafka producers feeding the function
should use a Pulsar producer or wait for a KoP version that fixes the input-direction framing.

### Basic usage

```java
try (Session session = Session.builder()
        .serviceUrl("pulsar://broker:6650")
        .adminUrl("http://broker:8080")
        .controlTopic("persistent://public/default/queries")
        .privateTopicTenant("public")
        .privateTopicNamespace("default")          // must match KoP's kafkaNamespace tail
        .build()) {

    Properties kafkaProps = new Properties();
    kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "broker:9094");      // KoP listener
    kafkaProps.put(GROUP_ID_CONFIG, "my-group");
    kafkaProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
    kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName());

    try (Consumer<String, byte[]> consumer = FilterableKafkaConsumers
            .<String, byte[]>newConsumer(session, kafkaProps)
            .topic("persistent://public/in/trades")    // PULSAR source topic
            .selector("region = 'US'")
            .subscribe()) {

        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(5));
        for (ConsumerRecord<String, byte[]> r : records) {
            Header region = r.headers().lastHeader("region");
            // ... function's forwardSourceMessageProperty surfaces here as a Kafka header
        }
        consumer.commitSync();

        // Dynamic ops via FilterControl cast — same as Pulsar SDK
        ((FilterControl) consumer).setSelector("region IN ('US','UK')");
    }
}
```

### Frozen subscription

The Kafka `Consumer<K, V>` returned from `subscribe()` enforces that its filtered subscription is
fixed at build time. The following throw `UnsupportedOperationException`:

- `subscribe(Collection<String>)` / `subscribe(Collection, ConsumerRebalanceListener)`
- `subscribe(Pattern)` / `subscribe(Pattern, ConsumerRebalanceListener)`
- `assign(Collection<TopicPartition>)`
- `unsubscribe()`

To change what's being consumed, either call `setSelector(...)` (hot-reload — same queryId, new
filter rule) or close this consumer and build a new one. Everything else on the `Consumer`
interface — `poll`, `commitSync`/`Async`, `seek`, `pause`/`resume`, `position`, `committed`,
`partitionsFor`, etc. — delegates straight through to the underlying real KafkaConsumer.

### Property → header round-trip

The filter function forwards Pulsar user properties (`forwardSourceMessageProperty=true`) to its
output topic. With `kafkaEntryFormat=pulsar_non_batched`, KoP surfaces those Pulsar properties as
Kafka headers on the consumer side. Concretely: a producer message with
`property("region", "US")` arrives at the Kafka consumer as a header `region=US` (bytes
`[0x55, 0x53]`). Validated end-to-end in `KafkaConsumerKoPIT`.

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

### Idle filter GC

On a long-running cluster, spawned filter functions accumulate — once a queryId has UNREGISTERed,
the orchestrator does not delete the filter by default. Set `idle-gc-enabled: true` to turn on a
background sweeper that deletes idle filter functions.

**Eligibility.** A filter function is deletion-eligible iff all four of these hold for its registry
entry:

1. `activeQueryIds` is present on the entry (i.e. the entry was written by the GC-aware orchestrator;
   pre-1.0 entries from older releases are skipped until they churn).
2. `activeQueryIds` is empty — every queryId that touched this filter has UNREGISTERed.
3. `idleSince` is set (stamped by the last UNREGISTER when the set became empty).
4. `now - idleSince >= idleGcTtlSeconds`.

Multi-REGISTER on the same input topic adds each queryId to the set; the filter only becomes
deletable after **all** of them UNREGISTER.

**What gets deleted, in order:**

1. The filter function instance (`admin.functions().deleteFunction(...)`). Closing the function
   cleans up its subscription on the input topic automatically — the spawned filter is built with
   `cleanupSubscription=true`.
2. The per-function control topic (`admin.topics().delete(controlTopic, force=true)`). `force=true`
   drops the compacted ledger and any lingering subscriptions.
3. The `t:` registry entry — a null tombstone on the state topic.

Each step tolerates `NotFound`, so a half-completed sequence (e.g. orchestrator crashed mid-GC)
self-heals on the next sweep tick.

**What does NOT get deleted:**

- **The SDK's private "filtered output topic"** (`persistent://.../filtered-<uuid>`). The SDK
  created it (`Session.createAndConfigurePrivateTopic`) and the SDK deletes it on
  `Session.retirePrivateQuery`. If your SDK consumer never calls `close()`, this orphans — Pulsar's
  `brokerDeleteInactiveTopics` (combined with `PrivateTopicPolicy.inactiveDeleteSeconds`) is the
  fallback reclaim path.
- **The user's input topic.** Never touched.

**Permissions.** The orchestrator's runtime principal needs more than functions-admin when GC is
on — it also needs `delete` permission on the per-function control topics under
`function-control-topic-tenant/namespace`. If the principal lacks this, you will see
`WARN deleteFunction ... failed: Forbidden` in the orchestrator logs and the `t:` entry will
survive across sweep ticks.

**Tuning.** Default `ttl=300s, sweep=30s` is conservative — a consumer that disconnects and
reconnects within 5 minutes does not lose its filter function. Tighter settings (e.g. `ttl=60s,
sweep=10s`) reclaim resources faster at the cost of churn on transient disconnects.

### Dead-consumer detection

Idle GC by itself only reclaims filter functions whose queryIds were *cleanly* UNREGISTERed
(consumer called `close()`). Consumers that crashed, were killed, or otherwise terminated without
clean shutdown leave their queryIds in `activeQueryIds` forever, pinning the filter alive.

The SDK closes this gap with periodic **HEARTBEAT** messages. Each `Session` runs a daemon
scheduler that publishes one `HEARTBEAT` message every `heartbeatIntervalSeconds` (default 30s),
listing every queryId that Session currently owns. The orchestrator stamps `lastSeen` on each
named QueryEntry; the sweeper synthesizes an UNREGISTER for any queryId whose `lastSeen` is
older than `heartbeat-max-stale-seconds` (default 90s, three missed heartbeats at the SDK
default).

**Total time from consumer death to filter deletion** is
`heartbeat-max-stale-seconds + idle-gc-ttl-seconds + sweep slack`. Defaults give roughly
6.5 minutes; tighter settings (`max-stale=15s`, `ttl=5s`, `sweep=2s`) reclaim in under 30s — at
the cost of declaring dead any consumer that experiences ≥3 consecutive publish failures within
the staleness window.

**SDK configuration.** Heartbeats are on by default. To opt out, build the Session with
`.disableHeartbeat()`:

```java
Session session = Session.builder()
    .pulsarClient(client)
    .controlTopic("persistent://public/filters/filter-requests")
    .heartbeatIntervalSeconds(30)   // default; or .disableHeartbeat() to turn off
    .build();
```

The heartbeat scheduler is a single daemon thread per Session, named
`selector-filter-heartbeat-<sessionId>`. It costs roughly one Pulsar message every
`heartbeatIntervalSeconds` regardless of how many consumers the Session owns.

**Limits.** Heartbeats are not a liveness assertion that the consumer is *processing messages* —
only that its process is running and the Pulsar producer is healthy. A consumer that is
heartbeating but stuck in a deadlocked message handler will NOT trigger dead-consumer detection.
Application-level heartbeats (e.g., a separate health endpoint, or a custom probe consumer on the
filtered output topic) cover that gap.

### Orchestrator observability

The orchestrator's idle-filter sweeper emits per-tick counters via `Context.recordMetric(...)`,
visible through `pulsar-admin functions stats` and the function-worker Prometheus endpoint. Use
these to monitor reclaim throughput and detect sweeper-side failures before they accumulate
silently across ticks.

**Sweeper metrics:**

| Metric | Recorded as | Meaning |
|---|---|---|
| `orchestrator.gc.candidates_evaluated` | `+1` per `t:` entry inspected | Filter-function registry entries the sweeper considered this tick. |
| `orchestrator.gc.functions_deleted` | `+1` per successful end-to-end GC | Filter functions reclaimed (function delete + control topic delete + `t:` retract all succeeded). |
| `orchestrator.gc.skipped_recent_register` | `+1` per recency-check miss | Snapshot looked GC-eligible but the per-key re-read showed a concurrent REGISTER refreshed it — sweeper backed off correctly. |
| `orchestrator.gc.errors` | `+1` per failed GC step | Function delete, topic delete, or `t:` retract failed with a non-`NotFound` error. The `t:` entry survives and the next tick retries. |
| `orchestrator.heartbeats.synthesized` | `+1` per synthesized UNREGISTER | The sweeper declared a queryId dead (its `lastSeen` was older than `heartbeat-max-stale-seconds`) and ran the UNREGISTER fan-out on its behalf. |
| `orchestrator.heartbeats.skipped_recent` | `+1` per recency-check miss | Snapshot showed a `q:` entry stale, but the per-key re-read showed a fresh HEARTBEAT arrived — sweeper backed off correctly. |
| `orchestrator.heartbeats.errors` | `+1` per failed synthesize | The synthesize-UNREGISTER callback threw. The `q:` entry survives and the next tick retries. |

**Reading the counters.**

```bash
pulsar-admin functions stats public/default/selector-filter-orchestrator-XXXX
```

The relevant counters appear under `userMetrics` (alongside the per-message orchestrator
counters from `process()` such as `orchestrator.requests.received` / `.accepted` /
`.rejected_*`). When scraped via the function-worker Prometheus endpoint the dots become
underscores, so dashboard expressions look like:

```
reclaim rate       = rate(orchestrator_gc_functions_deleted[5m])
sweeper error rate = rate(orchestrator_gc_errors[5m]) + rate(orchestrator_heartbeats_errors[5m])
contention rate    = rate(orchestrator_gc_skipped_recent_register[5m]) + rate(orchestrator_heartbeats_skipped_recent[5m])
```

A non-zero `sweeper error rate` is the alertable signal — the sweeper is self-healing across
ticks, but a persistent error rate means a permanent fault (most often a missing `delete`
permission on the per-function control-topic namespace; see **Idle filter GC → Permissions**).

**Structured DEBUG logs.** Every successful GC delete and every synthesized UNREGISTER also
emits a canonical key=value log line at DEBUG level alongside the existing INFO line. Format:

```
gc action=delete fn=<function-name> key=<registry-key> ctrl=<control-topic> reason=<idle|stale_heartbeat> elapsed_ms=<n>
gc action=synthesize_unregister query=<queryId> reason=stale_heartbeat elapsed_ms=<n>
```

Values do not contain spaces (function names, registry keys, and Pulsar topic URIs are all
space-free by construction), so a simple `split(' ')` followed by `split('=', 2)` is enough
for a scraper. The human-readable INFO line will be dropped one release after operators
migrate.

## Project layout

```
pom.xml                                       -- Java reactor parent (lists leaf modules)
protocol/                                     -- cross-language wire contracts
  query-message.schema.json                   -- JSON Schema for orchestrator → filter wire
  filter-request.schema.json                  -- JSON Schema for SDK → orchestrator wire
common/                                       -- Java shared types (used by functions + JVM SDKs)
  src/main/java/io/streamnative/filter/common/
    QueryMessage.java                         -- internal orchestrator → filter QueryMessage
    FilterRequest.java                        -- SDK → orchestrator FilterRequest
    Command.java                              -- REGISTER | MODIFY | UNREGISTER
    ClientType.java                           -- KAFKA | PULSAR
functions/                                    -- Pulsar function deployment artifacts (NARs)
  selector-filter/                            -- the broker-side filter function NAR
    pom.xml
    src/main/java/io/streamnative/filter/
      SelectorFilterFunction.java
      SelectorEvaluator.java                  -- compile + match + binding-requirement analysis
      CompiledQuery.java
      MessageBindings.java                    -- lazy __key__/__topic__/properties resolver
      TopicNames.java
    src/main/resources/META-INF/services/pulsar-io.yaml
    src/main/docker/Dockerfile
    deployment/                               -- legacy direct-deploy YAMLs
  orchestrator/                               -- per-input-topic spawner + control-plane router
    pom.xml
    src/main/java/io/streamnative/filter/orchestrator/
      OrchestratorFunction.java               -- the function (REGISTER/MODIFY/UNREGISTER routing)
      OrchestratorConfig.java                 -- userConfig parser (state-topic, package-url, ...)
      RegistryEntry.java                      -- per-input-topic state + compatibility matrix
      QueryEntry.java                         -- per-queryId fan-out index
      RegistryStore.java                      -- TableView facade (t:/q: keyspaces)
      FilterFunctionSpawner.java              -- spawn strategy interface
      DefaultFilterFunctionSpawner.java       -- admin.functions().createFunctionWithUrl()
      TopicKeySanitizer.java                  -- stable function-name + control-topic generation
    deployment/orchestrator-function-config.yaml
sdk/                                          -- client SDKs across all languages
  java/
    pulsar/                                   -- Pulsar-native SDK (JAR)
      pom.xml
      src/main/java/io/streamnative/filter/client/
        Session.java                          -- context + public registerPrivateQuery API
        SessionBuilder.java
        PrivateQuery.java                     -- value object passed between Session and SDKs
        FilterableConsumerBuilder.java        -- Pulsar-native consumer builder
        FilteringPulsarConsumer.java          -- Consumer<byte[]> + FilterControl wrapper
        FilterControl.java                    -- dynamic-selector cast interface
        PrivateTopicPolicy.java
        Selectors.java                        -- compose + validate selector syntax
    kafka/                                    -- Kafka-flavoured SDK over KoP (JAR)
      pom.xml
      src/main/java/io/streamnative/filter/client/kafka/
        FilterableKafkaConsumers.java         -- static factory entry point
        FilterableKafkaConsumerBuilder.java   -- fluent Kafka consumer builder
        FilteringKafkaConsumer.java           -- Consumer<K,V> + FilterControl wrapper
  # FUTURE: sdk/python/, sdk/cpp/, ...
```

## References

- [Apache Pulsar Functions](https://pulsar.apache.org/docs/functions-overview/)
- [Pulsar TableView](https://pulsar.apache.org/docs/client-libraries-tableviews/)
- [JMS 2.0 message selectors (Oracle docs)](https://docs.oracle.com/javaee/7/api/javax/jms/Message.html)
- [DataStax pulsar-jms](https://github.com/datastax/pulsar-jms)
- [Apache ActiveMQ selector grammar](https://activemq.apache.org/components/classic/documentation/selectors)
