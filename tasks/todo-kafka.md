# Kafka SDK Wrapper — server-side KoP path

## Goal

Add a Kafka-client SDK that mirrors the just-built Pulsar SDK. User code holds a real
`org.apache.kafka.clients.consumer.Consumer<K, V>` (pointed at a KoP-enabled broker) that ALSO
implements `FilterControl` for dynamic selector ops. Filter function output topic lives in a
KoP-mapped namespace with `kafkaEntryFormat=pulsar_non_batched`.

## Locked design decisions

- **Architecture**: server-side KoP. Real Kafka clients; the broker is KoP-enabled.
- **Direction confirmed by spike** (commit `a8f47a1`): Pulsar producer → `pulsar_non_batched` →
  Kafka consumer round-trips payload + properties→headers correctly on KoP 4.2.0.6.
- **Scope**: consumer only this iteration. Producer SDK deferred.
- **Selector model**: single mutable selector (same as Pulsar SDK).
- **Topic mapping v1**: `Session.privateTopicNamespace` must match KoP's `kafkaNamespace`
  (typically `<tenant>/<this>`); the Kafka topic name is the last segment of the private Pulsar
  topic (`filtered-<uuid>`). Document the constraint; fail loudly on mismatch is out-of-scope
  (the user will see an empty subscription).
- **API entry point**: new module `client-kafka` with `FilterableKafkaConsumers.newConsumer(...)`.
  Keeps Kafka deps out of the Pulsar-only client jar.

## Architecture

### Module layout

```
client-kafka/                         <-- new Maven module
  pom.xml                             <-- depends on selector-filter-client + kafka-clients
  src/main/java/io/streamnative/filter/client/kafka/
    FilterableKafkaConsumers.java     <-- public static factory entry point
    FilterableKafkaConsumerBuilder.java <-- fluent builder
    FilteringKafkaConsumer.java       <-- Consumer<K,V> + FilterControl, package-private
  src/test/java/io/streamnative/filter/client/kafka/
    FilterableKafkaConsumerBuilderTests.java
    FilteringKafkaConsumerTests.java
    KafkaConsumerKoPIT.java
```

### `Session` refactor (existing `client` module)

The Pulsar SDK currently keeps the registration logic (queryId gen, private-topic create,
control-topic publish) bundled with the Pulsar-consumer subscribe inside the package-private
`Session.subscribeFiltered(...)`. To let the Kafka SDK reuse it without depending on the Pulsar
consumer path, extract registration into a public API:

```java
// New public value class
public final class PrivateQuery {
    public String queryId();
    public String privateTopic();
    public List<String> sourceTopics();
    public String topicConstraint();   // __topic__ = '...' or __topic__ IN (...)
}

// New public methods on Session
public PrivateQuery registerPrivateQuery(List<String> sourceTopics, String userSelector)
        throws PulsarClientException;        // validates + creates topic + publishes QueryMessage

public void updatePrivateQuery(PrivateQuery pq, String newUserSelector)
        throws PulsarClientException;        // validates + republishes on same queryId

public void retirePrivateQuery(PrivateQuery pq);  // tombstone + per-policy topic delete
```

`Selectors.validate(...)` and `Selectors.combine(...)` now happen inside `Session` registration
methods so both SDKs share validation. Existing Pulsar `FilterableConsumerBuilder.subscribe()`
refactors to:

```java
PrivateQuery pq = session.registerPrivateQuery(sourceTopics, selector);
try {
    Consumer<byte[]> underlying = pulsarClient.newConsumer(...).subscribe();
    return new FilteringPulsarConsumer(session, pq, underlying, selector);
} catch (Throwable t) {
    session.retirePrivateQuery(pq);
    throw t;
}
```

### Kafka SDK API

```java
try (Session session = Session.builder()
        .serviceUrl("pulsar://broker:6650")    // pulsar broker
        .adminUrl("http://broker:8080")
        .controlTopic("persistent://public/default/queries")
        .privateTopicTenant("public")
        .privateTopicNamespace("default")      // MUST match KoP's kafkaNamespace tail
        .build()) {

    Properties kafkaProps = new Properties();
    kafkaProps.put(BOOTSTRAP_SERVERS_CONFIG, "broker:9094");  // KoP port
    kafkaProps.put(GROUP_ID_CONFIG, "my-group");
    kafkaProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    kafkaProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    Consumer<String, byte[]> c = FilterableKafkaConsumers.newConsumer(session, kafkaProps)
            .topic("persistent://public/in/trades")
            .selector("region = 'US'")
            .subscribe();

    while (running) {
        ConsumerRecords<String, byte[]> records = c.poll(Duration.ofSeconds(5));
        // ...
    }

    ((FilterControl) c).setSelector("region IN ('US','UK')");
    c.close();
}
```

### `FilteringKafkaConsumer<K, V>`

Implements `org.apache.kafka.clients.consumer.Consumer<K, V>` (~46 methods) AND `FilterControl`.

- **Delegation**: all `Consumer` methods forward to the underlying real `KafkaConsumer<K, V>`.
- **Frozen subscription**: `subscribe(...)` overloads and `assign(...)` throw
  `UnsupportedOperationException`. The subscription is fixed at build time — callers who want a
  different subscription build a new filtered consumer. `unsubscribe()` is allowed (terminates
  reading; equivalent to closing without retract — but we'll keep it strict and throw too to
  avoid leaking a consumer that thinks it's done while the query is still registered).
- **`close()`**: closes the underlying KafkaConsumer, then `session.retirePrivateQuery(pq)`.
  Idempotent. Retract still attempted if underlying close fails.
- **`close(Duration)`**: same with bounded close on underlying.
- **`setSelector(...)`**: validates + `session.updatePrivateQuery(pq, newSelector)`. Tracks new
  user selector locally for `currentSelector()`.

### Topic name resolution (v1)

```java
static String kafkaTopicNameFor(String pulsarTopicFqn) {
    // persistent://public/default/filtered-<uuid>  →  filtered-<uuid>
    int slash = pulsarTopicFqn.lastIndexOf('/');
    return pulsarTopicFqn.substring(slash + 1);
}
```

Document in README: requires `privateTopicNamespace` to match the tail of KoP's
`kafkaNamespace`. If KoP is configured with `kafkaNamespace=public/default`, then
`Session.privateTopicNamespace("default")` is correct.

## Tests

### Unit (`client-kafka/src/test/...`)

`FilterableKafkaConsumerBuilderTests` — mock Session + mock KafkaConsumer factory:
- subscribe with no topic throws
- topic/topics overwrite
- topics rejects empty
- null/blank selector allowed
- malformed selector throws `IllegalArgumentException` BEFORE any KafkaConsumer construction
- on subscribe failure, retirePrivateQuery is called
- Kafka topic name derived correctly from private topic FQN

`FilteringKafkaConsumerTests`:
- `poll(Duration)` delegates to underlying KafkaConsumer
- `commitSync` / `commitAsync` / `commitSync(offsets)` delegate
- `seek`/`pause`/`resume`/`position`/`committed` delegate
- `subscribe(...)`/`assign(...)`/`unsubscribe()` throw UnsupportedOperationException
- FilterControl.setSelector republishes on same queryId via Session.updatePrivateQuery
- FilterControl.setSelector with malformed throws and doesn't republish
- close → close underlying + retirePrivateQuery
- close idempotent
- setSelector after close throws IllegalStateException

### Integration (`KafkaConsumerKoPIT`)

Model after `KoPFilterFunctionIT.runWithKoP("pulsar_non_batched", ...)`:
- Start sn-platform:4.2.0.6 with `kafkaEntryFormat=pulsar_non_batched`
- Deploy the filter function NAR
- Source = persistent Pulsar topic (Pulsar producer writes — avoids the KoP input-direction bug)
- Build Session with `privateTopicNamespace="default"` (matches `kafkaNamespace=public/default`)
- Build a `FilterableKafkaConsumer<String, byte[]>` with selector `region = 'US'`
- Write 3 mixed messages via the Pulsar producer
- Poll the Kafka consumer; assert it sees the 2 US messages with correct payload + `region`
  header
- Cast to `FilterControl`; setSelector to `region = 'UK'`; write more; verify the swap
- Close; verify retract publishes a tombstone (proxy: consumer poll on the Pulsar control topic
  in a side-channel test, or just trust the unit test for tombstone publish)

## Implementation order

1. ☑ Refactor `Session`: add `PrivateQuery` value class and public `registerPrivateQuery` /
   `updatePrivateQuery` / `retirePrivateQuery`. Pulsar SDK switches to these. Existing client
   tests stayed green.
2. ☑ New Maven module `client-kafka`. Wired into parent pom + dependencyManagement.
3. ☑ `FilterableKafkaConsumers` static factory + `FilterableKafkaConsumerBuilder<K, V>`.
4. ☑ `FilteringKafkaConsumer<K, V>` wrapper (Consumer<K,V> + FilterControl).
5. ☑ Unit tests for builder + wrapper.
6. ☑ `KafkaConsumerKoPIT` against sn-platform:4.2.0.6 with `pulsar_non_batched`.
7. ☑ README: new "Kafka client library" section + KoP namespace constraint table.
8. ☑ `mvn -B clean verify` reactor-wide → BUILD SUCCESS (6:21).

## Review

**Files added**
- `client/src/main/java/io/streamnative/filter/client/PrivateQuery.java` — value class shared by
  both SDK paths.
- `client-kafka/pom.xml` — new Maven module.
- `client-kafka/src/main/java/io/streamnative/filter/client/kafka/FilterableKafkaConsumers.java`
- `client-kafka/src/main/java/io/streamnative/filter/client/kafka/FilterableKafkaConsumerBuilder.java`
- `client-kafka/src/main/java/io/streamnative/filter/client/kafka/FilteringKafkaConsumer.java`
- `client-kafka/src/test/java/.../KafkaTestFixtures.java`
- `client-kafka/src/test/java/.../FilterableKafkaConsumerBuilderTests.java` (11 tests)
- `client-kafka/src/test/java/.../FilteringKafkaConsumerTests.java` (21 tests)
- `client-kafka/src/test/java/.../KafkaConsumerKoPIT.java` (live end-to-end IT)

**Files modified**
- `pom.xml` (parent) — added `client-kafka` module, `kafka-clients.version`, and managed deps.
- `client/src/main/java/io/streamnative/filter/client/Session.java` — added public
  `registerPrivateQuery` / `updatePrivateQuery` / `retirePrivateQuery`. Internal Pulsar-SDK
  `subscribeFiltered(...)` now composes these methods. Selector validation centralized in
  `Session.registerPrivateQuery` (used by both SDKs).
- `client/src/main/java/io/streamnative/filter/client/FilteringPulsarConsumer.java` —
  constructor now takes a `PrivateQuery`; setSelector and close go through
  `session.updatePrivateQuery` / `session.retirePrivateQuery`.
- `client/src/main/java/io/streamnative/filter/client/FilterableConsumerBuilder.java` — drops
  explicit `Selectors.validate(...)` (now handled inside Session).
- `README.md` — new "Kafka client library" section with KoP deployment requirements table,
  usage example, frozen-subscription notes, property→header round-trip explanation. Project
  layout updated.

**Build status**
- `mvn -B clean verify` BUILD SUCCESS (6:21):
  - Common: trivial.
  - Function: 4:36 — function unit tests + 4 ITs (3 KoP matrix + Pulsar-functions IT) +
    `KoPOutputFormatSpikeIT`.
  - Session Client: 54s — 54 unit tests + 4 Pulsar ITs.
  - Kafka Client: 45s — 32 unit tests + 1 live Kafka IT (end-to-end through KoP).

**Architectural decisions of note**
- Path A (server-side KoP with real KafkaConsumer) selected over Path B
  (`pulsar-client-kafka` adapter) and Path C (hand-rolled Kafka interface impl). Validated by
  spike commit `a8f47a1`.
- v1 Kafka topic name = last segment of private Pulsar FQN. Documented constraint:
  `privateTopicNamespace` MUST match KoP's `kafkaNamespace` tail.
- Subscription methods on the returned `Consumer<K, V>` (`subscribe`/`assign`/`unsubscribe`)
  throw `UnsupportedOperationException` to make the frozen-subscription contract loud. `poll`,
  `commitSync`, `seek`, `pause`, etc. all delegate.
- No new public exception class — `IllegalArgumentException` reused (same as Pulsar SDK and
  matching feedback from prior commit).
