# SDK Heartbeat — dead-consumer detection plan

## Goal

Today's idle-filter GC only detects consumers that called `close()` (a clean
UNREGISTER). A consumer that crashed, was killed, or had its process
terminated leaves its queryId in `activeQueryIds` forever and pins the
spawned filter function alive indefinitely.

Add a heartbeat channel: SDK Session publishes a periodic `HEARTBEAT` message
to the orchestrator's input topic. The orchestrator tracks per-queryId
`lastSeen`. The existing sweeper detects queryIds that have not heartbeat in
`heartbeat-max-stale-seconds` and synthesizes the equivalent of an UNREGISTER.

## Locked design (from this session's AskUserQuestion)

- **Origin**: one heartbeat per **Session**, covering all queryIds it owns
  (cheaper than per-consumer for high-fan-out apps).
- **Staleness rule**: per-queryId `lastSeen` in QueryEntry; sweeper synthesizes
  UNREGISTER when `now - lastSeen > heartbeatMaxStaleSeconds`. Independent of
  the existing `idle-gc-ttl`.
- **Cadence**: SDK default interval **30s**, orchestrator default
  `heartbeat-max-stale-seconds` **90s** (three missed heartbeats → declared
  dead). The two sides are coupled by convention; the SDK does not negotiate.
- **Default**: heartbeat **ON by default** in the SDK. Opt-out via
  `Session.builder().disableHeartbeat()`.

## Wire format

### New command on FilterRequest

```json
{
  "command": "HEARTBEAT",
  "queryIds": ["uuid-1", "uuid-2", "uuid-3"]
}
```

- New `Command.HEARTBEAT` enum value.
- New optional `List<String> queryIds` field on FilterRequest (used only by
  HEARTBEAT; null on REGISTER/MODIFY/UNREGISTER).
- The message-key on the wire is arbitrary for HEARTBEAT (the body carries the
  authoritative queryId list). Orchestrator-side validation relaxes the
  "missing key" check for this command specifically.
- `protocol/filter-request.schema.json`: extend `oneOf` to allow the new
  command + `queryIds` array.

### QueryEntry — add `lastSeen`

```diff
  {
    "outputTopic": "...",
    "controlTopics": ["..."],
    "registryKeys":  ["..."],
+   "lastSeen":      "2026-05-12T19:14:30Z"  // ISO Instant
  }
```

- `null` on legacy QueryEntries (pre-heartbeat) → sweeper does not declare
  these dead (would be wrong for pre-existing queries that simply haven't been
  touched yet). Backfilled on first REGISTER/MODIFY/HEARTBEAT touch.
- Set on REGISTER (creation), MODIFY (touch), HEARTBEAT (touch).

## Orchestrator additions

### New userConfig key

| key | default | meaning |
|---|---|---|
| `heartbeat-max-stale-seconds` | `90` | Synthesize UNREGISTER for any queryId whose `lastSeen` is older than this. Independent of `idle-gc-ttl-seconds`. |

### handleHeartbeat

```java
private void handleHeartbeat(FilterRequest req, Context ctx) throws Exception {
    Instant now = Instant.now(clock);
    int touched = 0;
    for (String queryId : req.getQueryIds()) {
        QueryEntry q = registry.getQueryEntry(queryId);
        if (q == null) continue; // unknown — ignore (probably already-tombstoned)
        QueryEntry refreshed = q.withLastSeen(now);
        registry.putQueryEntry(queryId, refreshed);
        touched++;
    }
    ctx.recordMetric("orchestrator.heartbeats.received", 1);
    ctx.recordMetric("orchestrator.heartbeats.touched", touched);
}
```

`QueryEntry.withLastSeen(Instant)` is a new helper that returns a copy with
the updated timestamp.

REGISTER and MODIFY also call `withLastSeen(now)` on the QueryEntry they
write, so the lastSeen invariant is maintained without a heartbeat needed.

### IdleFilterSweeper extension

Add a second pass before the existing t: pass:

1. Iterate q: entries via `RegistryStore.forEachQueryEntry` (new method).
2. For each q: with `lastSeen != null && now - lastSeen > maxStaleSeconds`:
   - Invoke the new `synthesizeUnregister` callback (orchestrator-supplied).
   - Increment `orchestrator.heartbeats.synthesized_unregister`.
3. Then run the existing t: pass.

This ordering means a dead-consumer detection in tick N causes the queryId to
be UNREGISTER-equivalent, which empties its t: entries' activeQueryIds, which
starts the idle-gc-ttl clock. The t: entry is then GC'd in tick N +
ceil(ttl / interval) ticks later. Total time from consumer death to filter
deletion: `maxStale + ttl + slack`.

### OrchestratorFunction refactor

Factor `handleUnregister`'s body into a `performUnregister(String queryId,
QueryEntry q)` package-private method:

- Tombstone on every control topic in `q.getControlTopics()`.
- Decrement `activeQueryIds` on every `q.getRegistryKeys()` t: entry.
- Remove the q: entry.

`handleUnregister` calls `performUnregister` and adds metrics. The
sweeper-supplied callback also calls `performUnregister` (no context →
no metric recording, sweeper records its own metric).

The sweeper gets a new callback in its constructor:
`BiConsumer<String queryId, QueryEntry q> synthesizeUnregister`.

## SDK additions

### Session.builder()

```java
Session.builder()
    .pulsarClient(client)
    .controlTopic(...)
    .heartbeatIntervalSeconds(30)   // default 30; 0 = disabled
    .disableHeartbeat()             // shortcut for heartbeatIntervalSeconds(0)
    .build()
```

### Session runtime state

- `Set<String> liveQueryIds = new ConcurrentHashMap<>().newKeySet()` — added
  on `registerPrivateQuery`, removed on `retirePrivateQuery`.
- `ScheduledExecutorService heartbeatScheduler` — daemon, single-thread.
- `Future<?> heartbeatTask` — for cancellation on close.

Background task:
```java
List<String> snapshot = List.copyOf(liveQueryIds);
if (snapshot.isEmpty()) return; // nothing to heartbeat
FilterRequest hb = FilterRequest.heartbeat(snapshot);
publishFilterRequest(hb, "heartbeat-" + sessionId);
```

Errors during heartbeat publishing are logged at DEBUG (transient broker
unavailability is the common case) and the task continues on the next tick.

`Session.close()` cancels the scheduler and awaits termination (5s timeout)
before closing the producer.

## Files

### NEW
- (none — all additions are field-level on existing types)

### MODIFIED
- `common/src/main/java/io/streamnative/filter/common/Command.java` — add HEARTBEAT
- `common/src/main/java/io/streamnative/filter/common/FilterRequest.java` —
  add `queryIds`, `FilterRequest.heartbeat(List<String>)` factory, validate()
  rules for HEARTBEAT
- `common/src/test/java/.../FilterRequestTests.java` — serde + validation
- `protocol/filter-request.schema.json` — wire-schema additions
- `sdk/java/pulsar/src/main/java/.../Session.java` — heartbeat scheduling + builder
- `sdk/java/pulsar/src/main/java/.../SessionBuilder.java` —
  `heartbeatIntervalSeconds(long)`, `disableHeartbeat()`
- `sdk/java/pulsar/src/test/java/.../SessionTests.java` — heartbeat publishes
  expected wire format; close cancels scheduler
- `functions/orchestrator/src/main/java/.../QueryEntry.java` — `lastSeen`
  field + `withLastSeen(Instant)` helper
- `functions/orchestrator/src/main/java/.../OrchestratorConfig.java` —
  `heartbeat-max-stale-seconds` knob (default 90)
- `functions/orchestrator/src/main/java/.../OrchestratorFunction.java` —
  `handleHeartbeat`, `performUnregister` extracted, REGISTER/MODIFY set
  lastSeen on QueryEntry
- `functions/orchestrator/src/main/java/.../RegistryStore.java` — add
  `forEachQueryEntry(BiConsumer)`
- `functions/orchestrator/src/main/java/.../IdleFilterSweeper.java` — second
  sweep pass for stale queryIds; new `synthesizeUnregister` callback in
  constructor
- `functions/orchestrator/src/test/java/.../OrchestratorFunctionTests.java` —
  heartbeat touches lastSeen, REGISTER/MODIFY set lastSeen, unknown queryId
  HEARTBEAT is no-op
- `functions/orchestrator/src/test/java/.../IdleFilterSweeperTests.java` —
  stale queryId synthesizes UNREGISTER, fresh queryId is preserved
- `README.md` — heartbeat in Operations section under "Idle filter GC"
- `functions/orchestrator/deployment/orchestrator-function-config.yaml` —
  annotated example for `heartbeat-max-stale-seconds`

## Phases

| # | Phase | Outcome | Verification |
|---|---|---|---|
| 1 | Wire format | Command.HEARTBEAT, FilterRequest.heartbeat, schema, unit tests | `mvn -pl common test` green |
| 2 | QueryEntry + orchestrator handleHeartbeat | lastSeen plumbing, REGISTER/MODIFY/HEARTBEAT touch, unit tests | `mvn -pl functions/orchestrator test` green |
| 3 | Sweeper extension | Stale queryId detection in IdleFilterSweeper, synthesizeUnregister callback, performUnregister extraction, unit tests | `mvn -pl functions/orchestrator test` green |
| 4 | SDK heartbeat scheduling | Session.heartbeatIntervalSeconds, daemon scheduler, builder wiring, SDK unit tests | `mvn -pl sdk/java/pulsar test` green |
| 5 | IT — dead consumer detection | Live IT: REGISTER, abruptly kill SDK (close PulsarClient without retirePrivateQuery), wait > maxStale + ttl, assert filter GC'd | `mvn -pl functions/orchestrator verify` green |
| 6 | Docs + reactor verify | README operations section, YAML annotation, full reactor green | `mvn -B clean verify` green |

## Risks

- **R1**: Heartbeat traffic on a tenant with thousands of Sessions. With one
  message per Session per 30s, 1000 sessions = 33 msg/s. Fine for Pulsar.
  Document expected QPS in the operations section.
- **R2**: Clock skew between SDK and orchestrator. We do NOT compare the SDK
  clock to the orchestrator clock; the orchestrator stamps `lastSeen` from
  its own clock when the heartbeat arrives. So skew is irrelevant.
- **R3**: A "slow" SDK whose heartbeat publishes get queued for >maxStale
  will be falsely declared dead. Mitigation: maxStale = 3 * interval gives
  generous slack; producer-side queueing under that much sustained backpressure
  is itself a problem to investigate.
- **R4**: Synthesized UNREGISTER race with a real UNREGISTER from a
  recovering client. performUnregister is idempotent (registry.removeQueryEntry
  on missing key is a no-op) — same as the existing UNREGISTER-on-unknown-id
  path.
- **R5**: maxStale < interval is misconfiguration; orchestrator logs WARN at
  startup if `heartbeatMaxStaleSeconds < 2 * defaultInterval`. (Cannot enforce
  the SDK side because operators may run SDKs they didn't configure.)
