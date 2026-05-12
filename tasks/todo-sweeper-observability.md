# Sweeper observability — briefing for a parallel session

**Audience.** A separate Claude Code session opened in this repo. The session running on the
Python SDK (`tasks/todo-python-sdk.md`) is in another terminal; you and it are working in
parallel. Use a different feature branch to avoid merge conflicts.

## Coordination protocol (read FIRST)

1. **Create a feature branch before any edit.** Run:
   ```bash
   git switch -c feat/sweeper-observability
   ```
   Stay on this branch. The other session is on `main` doing Python work. We will merge both
   back to `main` once independent.
2. **Files you will touch.** Everything under `functions/orchestrator/`. **Do NOT touch
   `sdk/python/` or `tasks/todo-python-sdk.md` or `tasks/todo-perf-shadowtraffic.md`.** Those
   are the parallel session's territory.
3. **Files you can read but should not edit unless required.** `common/`, `sdk/java/`,
   `functions/selector-filter/`. If you need a change in any of these, leave a note in
   `tasks/todo-sweeper-observability.md` (under a "follow-ups" section) and skip — the merge
   conversation will handle it.
4. **When you finish a phase**, commit on your branch with a clear conventional-commit message
   (e.g. `feat(orchestrator): metrics surface for idle-filter sweeper (Phase 1)`). Do NOT
   merge to main until the operator says so.

## Context

Today the `IdleFilterSweeper` and the heartbeat path (`tasks/todo-sdk-heartbeat.md`,
phases 1-6, already shipped on `main` as of `642a915`) work correctly but are **operationally
opaque**:

- Per-tick `SweepStats` are populated but never surfaced anywhere — they live in a
  return value that nothing reads. Operators have no way to answer "how many filters did the
  sweeper reclaim last hour?"
- The orchestrator records some metrics via `ctx.recordMetric(...)` (e.g.
  `orchestrator.heartbeats.received`, `orchestrator.requests.accepted`) but the sweeper itself
  is silent. There is no `orchestrator.gc.deleted` or `orchestrator.heartbeats.synthesized`
  counter exposed to Pulsar's function-status admin endpoint.
- Structured logging is inconsistent. The sweeper logs one INFO line per GC, which is good,
  but the heartbeat-synthesize path emits its own INFO line — they should share a structured
  field set so operators can correlate.

Closing the observability gap is the next-most-important operational item after the heartbeat
feature itself.

## Goals

1. Every per-tick `SweepStats` counter is surfaced as a Pulsar function metric the
   orchestrator records via `ctx.recordMetric(...)` (or the equivalent — `Context` is the
   sweeper's access point).
2. Sweeper INFO-level lines use structured key-value pairs so operators can grep/parse them.
3. A README "Operations → Observability" subsection documents the metric names, what they
   mean, and how to read them via `pulsar-admin functions stats`.
4. (Optional, Phase 4) A debug/admin RPC surface — e.g. a sentinel REGISTER with a special
   command that returns the current `SweepStats` as a publish on a debug topic. Skip if
   non-trivial; we can ship without it.

## Locked design

- **No new dependencies.** Use the existing `Context.recordMetric(name, value)` API. No
  Prometheus or Micrometer integration in this phase.
- **Metric names follow the `orchestrator.<area>.<event>` convention** already established
  by `OrchestratorFunction.handleRegister` etc. So:
  - `orchestrator.gc.candidates_evaluated`
  - `orchestrator.gc.functions_deleted`
  - `orchestrator.gc.skipped_recent_register`
  - `orchestrator.gc.errors`
  - `orchestrator.heartbeats.synthesized`
  - `orchestrator.heartbeats.skipped_recent`
  - `orchestrator.heartbeats.errors`
- **Sweeper records via Context, not via a side channel.** This means
  `IdleFilterSweeper` needs access to `Context` (or a `MetricsRecorder` interface it can
  invoke). Today the sweeper has no `Context`. **Wire a `BiConsumer<String,Double>` metric
  callback into its constructor** — same pattern as `beforeTopicDelete` and
  `synthesizeUnregister`. The orchestrator wires it to a lambda that calls
  `ctx.recordMetric(...)` against the most-recent process()-time context.
- **Caveat on Context lifetime.** Pulsar function `Context` is per-invocation. The sweeper
  ticks on its own scheduler thread, NOT during a `process()` invocation. So
  `ctx.recordMetric` from the sweeper tick fires WITHOUT an active record-processing context.
  Per Pulsar functions docs this is typically allowed (the Context is associated with the
  function instance, not the message), but verify by manual test. If it does NOT work, the
  fallback is: have the sweeper accumulate stats in volatile fields, and have the next
  `process()` invocation flush them via `ctx.recordMetric`. Slightly delayed but simpler.

## Phases

| # | Phase | Outcome | Verification |
|---|---|---|---|
| 1 | Sweeper metric callback | `IdleFilterSweeper` gets a `BiConsumer<String,Double>` parameter; each `SweepStats` increment ALSO fires the callback. Unit tests use a `Map<String,Double>` accumulator. | `mvn -pl functions/orchestrator test` green. |
| 2 | OrchestratorFunction wires the callback | `initialize()` builds the sweeper with a callback that delegates to `lastCtx::recordMetric` where `lastCtx` is a volatile field updated on every `process()`. Unit test asserts metric names are emitted. | `mvn -pl functions/orchestrator test` green. |
| 3 | Structured log format | Sweeper INFO lines use a single canonical key-value format: `gc action=delete fn=<name> key=<key> ctrl=<topic> reason=<idle\|stale_heartbeat> elapsed_ms=<n>`. Replace today's two divergent INFO lines (`GCed idle filter ...` and `Synthesized UNREGISTER ...`). | Unit test parses the canonical log line. |
| 4 (optional) | Live IT verifies metrics | Extend `OrchestratorIdleFilterGcIT` to fetch function stats after each test and assert the relevant counter incremented. | `mvn -pl functions/orchestrator verify` green. |
| 5 | README update | New "Observability" subsection listing the seven metric names, what they mean, and example `pulsar-admin functions stats` output. | README renders sanely. |

## Risks

- **R1: `ctx.recordMetric` from a non-process thread.** Pulsar's threading model for function
  metrics may serialize Context. If recording from the sweeper tick fails silently, metrics
  will be empty in Phase 4 IT. **Mitigation:** verify on the testcontainer early (Phase 2),
  fall back to the deferred-flush pattern if needed.
- **R2: Naming churn.** Once we publish metric names, dashboards depend on them. Use the
  conventions already in place (`orchestrator.<area>.<event>`) and lock the names in Phase 1.
- **R3: Structured-log format incompatibility with existing log scrapers.** If operators are
  parsing the current `GCed idle filter ...` line, a format change breaks them. Mitigation:
  emit BOTH the old human-readable INFO and the new structured DEBUG line in Phase 3 for one
  release, then drop the human-readable version after operators migrate.

## Files

### New
- (none)

### Modified
- `functions/orchestrator/src/main/java/io/streamnative/filter/orchestrator/IdleFilterSweeper.java`
- `functions/orchestrator/src/main/java/io/streamnative/filter/orchestrator/OrchestratorFunction.java`
- `functions/orchestrator/src/test/java/io/streamnative/filter/orchestrator/IdleFilterSweeperTests.java`
- `functions/orchestrator/src/test/java/io/streamnative/filter/orchestrator/OrchestratorFunctionTests.java`
- `functions/orchestrator/src/test/java/io/streamnative/filter/orchestrator/OrchestratorIdleFilterGcIT.java` (Phase 4 optional)
- `README.md` (Phase 5)

## Done state

After Phase 5:
- All seven sweeper-related metric names are emitted on every relevant action
- Operators can run `pulsar-admin functions stats public/default/selector-filter-orchestrator-XXX`
  and see the counters
- README documents the names + interpretation
- `mvn -B clean verify` green across the reactor

## Out of scope

- Prometheus exporter integration (separate task)
- Grafana dashboards (separate task)
- An admin RPC surface for the sweeper's current state (deferred — Phase 4 is the closest we get)
- Per-namespace GC policy (separate todo, not started)
- The "in-flight handler liveness" gap from the heartbeat docs (separate todo, not started)
