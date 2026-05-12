# Idle filter function GC — implementation plan

## Goal

Today the orchestrator never deletes spawned filter functions. Once a queryId
UNREGISTERs, the filter keeps running on its now-quiet control topic. On a
long-running cluster, spawned filter functions accumulate indefinitely.

Add an opt-in GC path: a filter function becomes a deletion candidate after all
its queryIds have UNREGISTERed and an idle TTL has elapsed.

## Locked design decisions (from user, this session)

- **Reference tracking:** `Set<String> activeQueryIds` on RegistryEntry — not a
  bare counter. Diagnostic-friendly; naturally idempotent on double-UNREGISTER.
- **GC trigger:** **Periodic sweeper** task. Scheduled every N seconds (default
  30), walks `t:` entries, decides per-entry. Survives orchestrator restarts
  because state lives in the TableView.
- **Default:** **Opt-in** via `idle-gc-enabled: false` default. v1 deployments
  upgrading get zero behavior change unless they explicitly enable.

## Wire / state schema additions

### RegistryEntry (additions)

```diff
  {
    "controlTopic": "...",
    "filterFunctionName": "...",
    "keyDeserializer": null,
    "valueDeserializer": null,
    "clientType": "KAFKA",
    "createdAt": "2026-05-12T19:00:00Z",
+   "activeQueryIds": ["uuid-a", "uuid-b"],     // present iff orchestrator has seen GC-aware writes
+   "idleSince":      "2026-05-12T19:14:30Z"    // present iff activeQueryIds is empty
  }
```

- `activeQueryIds = null` (old entries) → sweeper treats them as "unknown" and
  **does not** GC. First REGISTER touching the entry backfills to a 1-element set.
- `activeQueryIds = []` + `idleSince = T` → eligible for GC at `T + idleGcTtl`.
- `activeQueryIds = [...]` → in use, `idleSince` cleared to null.

### QueryEntry (additions)

```diff
  {
    "outputTopic": "...",
    "controlTopics": ["ctrl-a", "ctrl-b"],
+   "registryKeys":  ["persistent://.../in/a", "persistent://.../in/b"]  // parallel to controlTopics
  }
```

`registryKeys[i]` is the `t:`-keyspace key whose RegistryEntry owns
`controlTopics[i]`. Lets UNREGISTER do O(1) lookups into the `t:` entry to
decrement `activeQueryIds`, rather than scanning every `t:` entry. Old QueryEntry
entries (missing `registryKeys`) fall back to today's scan path.

## OrchestratorConfig additions

| key | default | meaning |
|---|---|---|
| `idle-gc-enabled` | `false` | Master switch. When false, sweeper does not start. |
| `idle-gc-ttl-seconds` | `300` | Filter is deletable after this much continuous idle time. |
| `idle-gc-sweep-interval-seconds` | `30` | How often the sweeper runs. |

## Control flow changes

### REGISTER
1. Compute registry keys (as today).
2. For each key — same compatibility check, same spawn-or-reuse logic.
3. **NEW:** after the entry exists, build a merged entry with `queryId` added to
   `activeQueryIds`, `idleSince` cleared.
4. Write merged entry back via `registry.putInputTopicEntry(key, merged)`.
5. Publish QueryMessage to its control topic (as today).
6. Persist QueryEntry with parallel `registryKeys + controlTopics` lists.

### UNREGISTER
1. Look up `q:<queryId>` (as today).
2. Tombstone on each control topic (as today).
3. **NEW:** for each `registryKeys[i]`, look up the `t:` entry, build a merged
   entry with `queryId` removed from `activeQueryIds`. If the set becomes empty,
   set `idleSince = now()`. Write back.
4. Remove `q:<queryId>` (as today).

### Sweeper (new)
1. Started in `initialize()` iff `idle-gc-enabled = true`.
2. `ScheduledExecutorService` with one thread, fixed delay `idleGcSweepIntervalSeconds`.
3. Each tick:
   - Snapshot all `t:` entries from the TableView.
   - For each entry: if `activeQueryIds != null && activeQueryIds.isEmpty() &&
     idleSince != null && now - idleSince >= idleGcTtl`:
     - Re-read the entry from TableView (recency check, in case a REGISTER raced).
     - If still empty + still idle for ≥ TTL: GC.
4. GC sequence per entry:
   - `admin.functions().deleteFunction(tenant, ns, name)` — NotFound is OK.
   - `admin.topics().delete(controlTopic, force=true)` — NotFound is OK.
   - `registry.removeInputTopicEntry(key)` — publishes null tombstone on state
     topic so the TableView retracts the key for restart-time replay.
5. Metric: `orchestrator.gc.{evaluated,deleted,skipped_recent_register}`.
6. Closes cleanly on `OrchestratorFunction.close()`.

## Edge cases & resilience

- **REGISTER races sweeper.** Sweeper snapshots t: entry → entry is empty +
  idle. Concurrent REGISTER arrives, mutates activeQueryIds in-memory and
  publishes update. Sweeper's recency check (step 3 re-read) catches this and
  skips. If sweeper still wins the race and starts deleting, the in-progress
  REGISTER will see the function vanish at next `admin.functions().getFunctionStatus()`
  and re-spawn. Acceptable in v2; documented.
- **Old entries (no activeQueryIds).** Sweeper skips them. They GC naturally
  after the first REGISTER+UNREGISTER cycle backfills the field.
- **Orchestrator restart mid-GC.** TableView replays current state. Half-deleted
  filter (function gone, t: entry still present) is handled because the next
  sweeper tick will retry — `deleteFunction` is idempotent (NotFound → ok), and
  `removeInputTopicEntry` is the final step that drops the t: entry.
- **GC for a queryId-shared filter (multi-REGISTER on the same input topic).**
  RegistryEntry.activeQueryIds is a Set, so two REGISTERs on the same input
  result in `{q1, q2}`. Only after BOTH UNREGISTER does the set become empty and
  the TTL clock start. Correct by construction.
- **GC during MODIFY.** MODIFY does not touch activeQueryIds and is fast; the
  sweeper would have to win an unlikely race. Even if it does, MODIFY publishes
  to control topics directly via cached producer; on a deleted topic the publish
  fails. We treat MODIFY-after-GC as a no-op with a WARN log (the queryId is
  already orphaned in this scenario).

## Files

### NEW
- `functions/orchestrator/src/main/java/io/streamnative/filter/orchestrator/IdleFilterSweeper.java`
  — the scheduler + per-tick logic. Constructor: `(RegistryStore, PulsarAdmin, OrchestratorConfig, Clock)`.
  Package-private. Exposes `start()` / `close()`.
- `functions/orchestrator/src/test/java/.../IdleFilterSweeperTests.java`
  — unit tests on a `ManualClock` and mocked admin/registry. Cases: candidate
  gets deleted; recent UNREGISTER (< TTL) is skipped; non-empty set is skipped;
  null activeQueryIds is skipped; recency re-read catches concurrent REGISTER;
  NotFound on delete is tolerated.

### MODIFIED
- `RegistryEntry.java` — add `activeQueryIds`, `idleSince` (Jackson optional),
  add `withQueryId(queryId)`, `withoutQueryId(queryId, Instant nowIfIdles)`.
  Keep existing `mergedWith(req)` orthogonal to the new helpers — REGISTER applies
  both (`mergedWith().withQueryId(...)`).
- `QueryEntry.java` — add parallel `registryKeys` list. Constructor accepts
  null for backward-compat reads.
- `RegistryStore.java` — surface `streamInputTopicEntries()` for the sweeper to
  iterate without copying. (We already have `getInputTopicEntry(key)` + the
  TableView; just add a `forEachInputTopicEntry(BiConsumer)` view.)
- `OrchestratorConfig.java` — three new keys (parsed via existing
  `optionalBool` / `optionalLong`).
- `OrchestratorFunction.java`:
  - `initialize()` — instantiate and start `IdleFilterSweeper` iff enabled.
  - `handleRegister` — `entry.withQueryId(queryId)` then persist; also persist
    `QueryEntry` with parallel `registryKeys`.
  - `handleUnregister` — for each `registryKeys[i]`, `entry.withoutQueryId(queryId, now)`
    and persist. Fall back to today's behavior if `registryKeys == null` on the QueryEntry.
  - `close()` — close the sweeper.
- `OrchestratorFunctionTests` — augment REGISTER/UNREGISTER tests to assert
  `activeQueryIds` mutation. Add a multi-REGISTER-then-partial-UNREGISTER test
  proving the filter stays alive while ≥1 queryId remains.
- `RegistryEntryTests` — `withQueryId` / `withoutQueryId` semantics.
- `QueryEntryTests` — `registryKeys` round-trip + null-tolerance.
- `OrchestratorFunctionIT` — new test:
  `idleFilterIsDeletedAfterTtl()`. Tighten TTL/sweep config to ~5s/2s for IT
  speed. REGISTER, drive a message, UNREGISTER, poll
  `admin.functions().getFunctions()` until the spawned filter disappears, assert
  the control topic is also gone.
  Also: `recentReRegisterPreventsGc()` — REGISTER, UNREGISTER, REGISTER within
  TTL window, advance past TTL, assert filter NOT deleted.
- `README.md` — userConfig table gets three new rows; new "Idle filter GC"
  section under Operations with safety notes (R12-style — operator's filter-admin
  principal MUST have delete permission on the function tenant/namespace).
- `functions/orchestrator/deployment/orchestrator-function-config.yaml` — annotated
  examples for the three new keys, defaults explicitly shown commented.

## Phases (each independently verifiable)

| # | Phase | Outcome | Verification |
|---|---|---|---|
| 1 | Schema additions | `RegistryEntry.activeQueryIds`/`idleSince`, helper methods. `QueryEntry.registryKeys` parallel list. Unit tests. | `mvn -pl functions/orchestrator test` green. |
| 2 | REGISTER/UNREGISTER ref-tracking | Orchestrator writes activeQueryIds on REGISTER; clears on UNREGISTER; sets idleSince when empty. QueryEntry stores parallel registryKeys. Backwards-compat for null registryKeys (fallback scan). Updated orchestrator unit tests. | `mvn -pl functions/orchestrator test` green. |
| 3 | IdleFilterSweeper | Standalone class + unit tests against ManualClock + mocked admin/registry. Edge cases enumerated. | `mvn -pl functions/orchestrator test` green. |
| 4 | Wire into OrchestratorFunction | Start/stop sweeper based on idle-gc-enabled. Three new userConfig keys parsed. | `mvn -pl functions/orchestrator test` green. |
| 5 | OrchestratorFunctionIT — GC paths | Live IT with short TTL — proves end-to-end delete; proves re-REGISTER blocks GC. | `mvn -pl functions/orchestrator verify` green. |
| 6 | Docs + sample config | README userConfig + Operations section, YAML annotations. Full reactor `mvn -B clean verify` green. | `mvn -B clean verify` green. |

## Risks

- **R1:** Sweeper deletes a function the operator was relying on. **Mitigation:**
  opt-in default; explicit `idle-gc-ttl-seconds` configurability; operator can
  watch `orchestrator.gc.deleted` metric.
- **R2:** Permission gap — orchestrator's admin principal can `createFunction`
  but cannot `deleteFunction`. **Mitigation:** README documents this as a
  prerequisite; sweeper logs WARN on first PulsarAdminException so it's loud,
  not silent.
- **R3:** `admin.topics().delete(force=true)` removes the compacted state ledger
  of the control topic. If a spawned filter function is still alive on it (race),
  the function's TableView producer crashes. **Mitigation:** GC sequence is
  `deleteFunction` FIRST (which tears down the function's consumer on the control
  topic), THEN `delete topic`. Function NotFound during deleteFunction is
  tolerated, so a half-deleted state self-heals on the next sweep.
- **R4:** Backwards compat — v1 deployments upgrading: old `t:` and `q:` entries
  lack the new fields. **Mitigation:** Jackson `@JsonIgnoreProperties(ignoreUnknown=true)`
  already in place; null reads OK. Sweeper explicitly skips entries with
  `activeQueryIds == null`. UNREGISTER falls back to today's behavior for
  QueryEntries with null `registryKeys`.

## Verification

After all phases:
- `mvn -B clean verify` green (reactor wall-time target: under 10 min)
- IT `idleFilterIsDeletedAfterTtl()` proves a spawned filter is deleted ~5s
  after UNREGISTER+TTL on a fresh testcontainer.
- IT `recentReRegisterPreventsGc()` proves the recency re-read works.
- Manual smoke (optional): deploy orchestrator with `idle-gc-enabled: true,
  idle-gc-ttl-seconds: 60` and watch `orchestrator.gc.deleted` increment after
  SDK consumer close.
