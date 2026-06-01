# Continuous existence-reconcile loop (orchestrator)

## Context
The orchestrator self-healed only reactively (spawn on REGISTER) and by deletion (idle-GC sweeper).
A filter function deleted out-of-band while a live query still referenced it was never re-created
until a fresh REGISTER. This adds the "ensure desired filters exist" half of a K8s-style controller:
a periodic loop that re-spawns MISSING functions for in-use queries. Crashed-but-present instances
remain the Pulsar Functions runtime's job (kubelet-style) — explicitly out of scope.

Locked decisions: missing-only repair; full self-heal (persist selector so a lost control topic is
rebuilt); separate `FilterReconciler` class (sweeper unchanged).

## Plan (implemented)
- [x] `QueryEntry`: add nullable `selector` (additive; threaded through constructors/withSelector/
      withLastSeen/equals/serde). Persisted so reconcile can replay each query's QueryMessage.
- [x] `RegistryEntry`: add `isPattern` flag (newEntry derives from req; threaded through copy methods).
      Needed to reconstruct topicsPattern-vs-inputTopics for the re-spawn request.
- [x] `OrchestratorConfig`: add `existence-reconcile-enabled` (default false),
      `existence-reconcile-interval-seconds` (60), `existence-reconcile-max-attempts` (5).
- [x] `FilterReconciler` (new): own daemon scheduler; per-tick snapshot of in-use `t:` entries →
      backoff gate → recency re-read → `getFunctionStatus`; on NotFound re-spawn (idempotent via
      spawner) + repopulate control topic from persisted selectors. Backoff + give-up backstop.
      `orchestrator.reconcile.*` metrics, deferred-flushed via the existing recorder.
- [x] `OrchestratorFunction`: persist selector on REGISTER, update it on MODIFY; construct+start the
      reconciler in initialize() when enabled; close it first in close().
- [x] Config YAMLs documented (orchestrator deployment template + demo/local + demo/infra), left
      DISABLED by default.

## Review (results)
- All orchestrator + common + pulsar-SDK unit tests green: **141 / 141** (JDK 17).
  New: `FilterReconcilerTests` (8) — healthy no-op, missing→respawn+republish, legacy-null-selector
  →identity, pattern→pattern request, recency-skip, backoff→give-up. Plus serde/back-compat cases on
  `QueryEntryTests` (+5) and `RegistryEntryTests` (+5), and selector-persistence on
  `OrchestratorFunctionTests` (+3).
- Bug caught by the new serde test: Jackson derived `"pattern"` from the boolean getter `isPattern()`
  (strips `is`), mismatching the `@JsonCreator` `"isPattern"` → always deserialized false. Fixed by
  pinning `@JsonProperty("isPattern")` on the getter.
- Deviation from plan: did NOT rename `recordSweeperMetric`/`pendingSweeperMetrics` (the plan's
  cosmetic "generalize" step) — it would churn ~12 test call sites for no behavior change; the
  reconciler reuses the existing deferred-metric recorder, which already does the right thing.

## Remaining
- [x] Live integration test added: `OrchestratorExistenceReconcileIT` (testcontainers Pulsar 4.2.1,
      `.withFunctionsWorker()`). Deploys orchestrator with reconcile ON + idle-GC OFF, registers a
      filter, **deletes its function out-of-band** via admin, asserts the reconciler re-spawns it and
      end-to-end filtering resumes, plus an `orchestrator.reconcile.missing_respawned` metric delta.
      Compiles; runs via failsafe (`mvn verify -Dit.test=OrchestratorExistenceReconcileIT
      -Dfailsafe.failIfNoSpecifiedTests=false`).
- [ ] **CI-validate the IT.** Could NOT be validated on this local host: every function cold-start
      times out reaching RUNNING (orchestrator hit `restarts=4 error= sysExc=[] userExc=[]` — no
      exception, just slow restart). The **unchanged, CI-green** `OrchestratorIdleFilterGcIT` fails
      here identically (its spawned *filter* functions — which run none of this code — also never
      reach RUNNING in 90s). So it's host resource starvation, not a code defect. Run in CI (the
      documented temurin-17 env) to confirm green.
- [ ] Local e2e validation (on a non-starved host), then flip `existence-reconcile-enabled: true` in
      demo configs.
- [ ] RUNBOOK: document the keys + the "delete a function, watch it heal" demo beat.
- [ ] Once enabled, update slide S6 in `tasks/slides-architecture-outline.md`: the continuous
      existence-reconcile loop moves from "roadmap" to "present behavior."
