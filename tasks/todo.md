# SDK Wrapper — Pulsar consumer-side, single-mutable-selector

## Goal

Replace the current `Session.createConsumer(...)` API with one that returns the **standard
`org.apache.pulsar.client.api.Consumer<byte[]>` interface** and surfaces dynamic selector ops
via a `FilterControl` sub-interface accessed by cast.

User idiom should mirror Pulsar's normal `pulsarClient.newConsumer()...subscribe()` chain.

## Locked design decisions

- **Scope**: Pulsar SDK only this iteration. Kafka SDK deferred.
- **Selector model**: single, mutable. `setSelector(...)` replaces the current selector;
  the function hot-reloads it on the same `queryId` key (already supported by the function).
- **Architecture**: client-side SDK wrapper. No network proxy.
- **API shape**: returned object implements both `Consumer<byte[]>` AND `FilterControl`.
  Callers cast when they want dynamic ops.
- **Existing `Session.createConsumer(...)` overloads**: replaced by a builder. Old
  `FilteredConsumer` class is removed.
- **`Session` itself stays** as a context object (owns `PulsarClient`, `PulsarAdmin`,
  control-topic producer, `PrivateTopicPolicy`).

## API shape

```java
try (Session session = Session.builder()
        .serviceUrl("pulsar://broker:6650")
        .adminUrl("http://broker:8080")
        .controlTopic("persistent://public/control/queries")
        .build()) {

    Consumer<byte[]> c = session.newConsumer()
            .topic("persistent://public/quotes/trades")        // or .topics(Collection<String>)
            .selector("region = 'US'")                          // null/empty allowed
            .subscriptionName("my-sub")                         // optional; default session-<uuid>
            .subscriptionType(SubscriptionType.Exclusive)       // optional
            .subscribe();                                       // returns Consumer<byte[]>

    Message<byte[]> msg = c.receive();
    c.acknowledge(msg);

    // Dynamic ops: cast to FilterControl
    FilterControl fc = (FilterControl) c;
    fc.setSelector("region IN ('US','UK')");                    // hot-reload on same queryId
    System.out.println(fc.queryId());
    System.out.println(fc.currentSelector());

    c.close();   // tombstones the query + deletes private topic per policy
}
```

## New API surface

### `FilterControl` (new public interface)

```java
public interface FilterControl {
    /** Replaces the active user-side selector; null/empty means "topic-match only". */
    void setSelector(String selector) throws PulsarClientException;

    /** Current user-side selector (the bit AFTER topic constraint). May be null. */
    String currentSelector();

    /** Stable query id for the lifetime of this consumer. */
    String queryId();

    /** Source topics this consumer is filtering. */
    List<String> sourceTopics();

    /** Private output topic the inner Consumer is subscribed to. */
    String privateTopic();
}
```

### `FilterableConsumerBuilder` (new public builder)

Fluent builder mirroring `ConsumerBuilder<byte[]>`. Subset of methods that make sense for our
use case in v1:

```java
public final class FilterableConsumerBuilder {
    public FilterableConsumerBuilder topic(String topic);
    public FilterableConsumerBuilder topics(Collection<String> topics);
    public FilterableConsumerBuilder selector(String selector);
    public FilterableConsumerBuilder subscriptionName(String name);
    public FilterableConsumerBuilder subscriptionType(SubscriptionType type);
    public FilterableConsumerBuilder subscriptionInitialPosition(SubscriptionInitialPosition pos);

    /** Returns a real Pulsar Consumer<byte[]> that also implements FilterControl. */
    public Consumer<byte[]> subscribe() throws PulsarClientException;
}
```

Notes:
- `topic` and `topics` are mutually exclusive; second call overwrites first.
- At least one topic is required. `selector` may be null or empty.
- Future extension points (ackTimeout, receiverQueueSize, etc.) deliberately omitted in v1.
- `subscribe()` is the only terminal.

### `FilteringPulsarConsumer` (new package-private wrapper)

`implements Consumer<byte[]>, FilterControl`. Delegates all `Consumer` methods to the underlying
real consumer. Implements `FilterControl` by publishing a new `QueryMessage` to the control topic
keyed by the same `queryId` (hot reload). `close()` retracts query + deletes private topic per
policy (same as today).

`currentSelector` returns the user portion (not the composed `__topic__ = '...' AND (...)` form).
`setSelector` re-composes against the same `sourceTopics` and republishes.

### `Session` changes

- Add `public FilterableConsumerBuilder newConsumer()`.
- Remove `createConsumer(String, String)`, `createConsumer(Collection<String>, String)`,
  `createConsumer(Collection<String>, String, String)`.
- Move private-topic create/configure and control-topic publish helpers from `Session` into
  package-private utility methods callable by the builder/wrapper. Keep them on `Session`
  itself or factor into a `SessionInternals` shim — simpler to leave on `Session` and let
  the wrapper hold a `Session` reference (same pattern as today).
- Existing `retractAndCleanup(queryId, privateTopic)` stays — used by the new wrapper's close.

### `FilteredConsumer` — DELETE

No backwards compat needed (pre-1.0, no users).

### Other files

- `PrivateTopicPolicy.java` — unchanged.
- `Selectors.java` — unchanged.
- `SessionBuilder.java` — unchanged (builds `Session`).
- `common/QueryMessage.java` — unchanged.
- `function/...` — unchanged (function already supports hot reload).

## Tests

### Unit (new / updated)

`FilterableConsumerBuilderTests` (new):
- Builder rejects no topic.
- `topic` and `topics` overwrite each other.
- Null/empty selector is allowed.
- Default subscription name applied when omitted.
- Default subscription type is Exclusive.

`FilteringPulsarConsumerTests` (new):
- All `Consumer<byte[]>` delegation methods route to mock underlying consumer.
- `setSelector("x = 1")` publishes a new `QueryMessage` on the control producer with same
  queryId, same outputTopic, composed selector = topic-constraint AND `x = 1`.
- `setSelector(null)` publishes a `QueryMessage` with selector = topic-constraint alone
  (no `AND (...)`).
- `setSelector("")` same as null.
- `close()` calls `retractAndCleanup` and the underlying consumer's `close()`.
- Cast-to-`FilterControl` works (compile-time check via test using the interface directly).

`SessionTests` (rewrite):
- Verify `session.newConsumer().topic(...).selector(...).subscribe()` returns an object that
  is a `Consumer<byte[]>` AND a `FilterControl`.
- Verify the multi-topic path composes `__topic__ IN (...)`.
- Verify cleanup on subscribe-failure path (mock pulsarClient throws after publish; expect
  retract + topic delete attempt).

`PrivateTopicPolicyTests` — unchanged.
`SelectorsTests` — unchanged.

### Integration (live testcontainer Pulsar)

`SessionIT` (rewrite):
- `newConsumer().topic(...).selector(region='US').subscribe()` — produce mixed messages,
  receive only US matches.
- `setSelector(region='UK')` mid-stream — produce more mixed, after a small propagation
  window, receive only UK matches. Verify same `queryId` before/after.
- `close()` — verify private topic eventually disappears (within TTL window).
- Multi-topic case (`topics(List.of(a, b))`) — verify both topics are matched by topic
  constraint.

## Implementation order

1. ☑ Add `FilterControl` interface.
2. ☑ Add `FilterableConsumerBuilder` skeleton (validation + state).
3. ☑ Add `FilteringPulsarConsumer` wrapper (`Consumer<byte[]>` + `FilterControl`).
4. ☑ Wire `Session.newConsumer()`; remove old `createConsumer` overloads.
5. ☑ Delete `FilteredConsumer.java`.
6. ☑ Rewrite `SessionTests` for new API.
7. ☑ Add `FilterableConsumerBuilderTests` + `FilteringPulsarConsumerTests`.
8. ☑ Rewrite `SessionIT` for new API + add setSelector round-trip IT.
9. ☑ Update README "Client library" section.
10. ☑ Run `mvn -B clean test` → green.
11. ☑ Run `mvn -B clean verify` (full ITs, requires Docker) → green.

## Mid-implementation additions

- **Selector validation** (raised during impl). Added `Selectors.validate(...)` using the same
  `pulsar-jms-filters-common` parser the function uses. Throws `IllegalArgumentException` from
  `subscribe()` and `setSelector(...)` on malformed input. No new public exception class —
  reuses the standard JDK type with `JMSException` as the cause.

## Review
- `FilterControl.java` — dynamic-selector cast interface (single-selector model).
- `FilterableConsumerBuilder.java` — builder; terminal `subscribe()` returns `Consumer<byte[]>`.
- `FilteringPulsarConsumer.java` — package-private wrapper implementing both
  `Consumer<byte[]>` and `FilterControl`. Delegates every `Consumer` method; intercepts
  `close()`/`closeAsync()` to tombstone + cleanup; intercepts `setSelector(...)` to republish on
  the same `queryId` for hot reload.

**Files modified**
- `Session.java` — `newConsumer()` factory replaces three `createConsumer(...)` overloads;
  builder/wrapper hooks (`subscribeFiltered`, `republishQuery`, `retractAndCleanup`) are
  package-private.
- `Selectors.java` — added `validate(String)` using `SelectorSupport.build(...)`.
- `PrivateTopicPolicy.java` — javadoc updated for removed `FilteredConsumer`.
- `client/pom.xml` — added `com.datastax.oss:pulsar-jms-filters-common` dependency.
- `README.md` — full rewrite of "Client library" section + project layout block.

**Files deleted**
- `FilteredConsumer.java`.

**Test coverage added**
- `FilterableConsumerBuilderTests` — 11 tests (validation, overrides, defaults, blank-handling).
- `FilteringPulsarConsumerTests` — 18 tests (delegation, setSelector republish + same-queryId,
  null/blank handling, malformed-rejected-without-publish, close idempotence, close-on-failure).
- `SessionTests` — rewritten, 13 tests covering policies, control-topic payload, multi-topic
  composition, cleanup semantics, and pre-publish validation.
- `SessionIT` — rewritten with 4 ITs against live testcontainer Pulsar 4.0.10:
  1. End-to-end basic filtering.
  2. setSelector hot-reload mid-stream (same queryId, new match set).
  3. Multi-topic IN-composition (both inputs declared in function config from the start —
     Pulsar refuses to alter inputs after creation).
  4. Malformed selector rejected client-side without touching control topic.

**Build status**
- `mvn -B clean verify`: BUILD SUCCESS — 54 unit tests + 4 client ITs + all function unit
  tests + 4 function ITs (KoP matrix). Total time 5:21.
