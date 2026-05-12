# Performance tests with ShadowTraffic — briefing for a parallel session

**Audience.** A third Claude Code session opened in this repo. Two other sessions are running
in parallel:

- One on `feat/python-sdk` building `sdk/python/` (see `tasks/todo-python-sdk.md`)
- One on `feat/sweeper-observability` adding metrics to `functions/orchestrator/`
  (see `tasks/todo-sweeper-observability.md`)

You are the third. Use your own feature branch.

## Coordination protocol (read FIRST)

1. **Create a feature branch before any edit.** Run:
   ```bash
   git switch -c feat/perf-shadowtraffic
   ```
   Do NOT touch `main` directly. Keep your work isolated from the other two sessions.

2. **Files you will touch (your territory).**
   - NEW: `perf/` Maven module (everything under it)
   - MODIFY: root `pom.xml` to add `<module>perf</module>`
   - MODIFY: top-level `README.md` only to add a brief "Performance testing" pointer
     (one section, near the end). If you anticipate larger README changes, defer to a
     merge-time conversation.

3. **Files you must NOT touch:**
   - `sdk/python/` — Python SDK session territory
   - `functions/orchestrator/` — sweeper-observability session territory
   - `tasks/todo-python-sdk.md`, `tasks/todo-sweeper-observability.md`,
     `tasks/todo-perf-shadowtraffic.md` (the long-form plan you read from)

4. **Files you can read but should not edit unless required:**
   - `common/`, `sdk/java/`, `functions/selector-filter/`
   - Existing ITs in any module. If you need to crib their container-setup patterns, do so —
     but keep your perf harness in `perf/` only, not mixed into the other modules.

5. **Read the full plan first.** The detailed scope, file layout, 7-phase rollout, JSON result
   schema, and risk register are in `tasks/todo-perf-shadowtraffic.md` at the repo root.
   That document is your primary reference. This briefing only covers coordination.

6. **Commit cadence.** One commit per phase, conventional-commit style. Do not merge to main
   until the operator says so.

## What you're building (one-paragraph summary)

A new `perf/` Maven module containing TestContainers-based benchmarks. ShadowTraffic
(`shadowtraffic/shadowtraffic:latest`) generates synthetic Kafka traffic against a Pulsar
testcontainer (via the existing KoP-validated path); the selector-filter function consumes,
filters, and produces to output topics; a probe consumer measures throughput, latencies, and
filter-function CPU. Results land as JSON files under `perf/target/reports/` ready to feed a
long-running regression chart. The suite runs only under the `-Pperf` Maven profile so PR CI
stays fast.

## Open questions to resolve at kickoff (do NOT block on the operator — make a reasonable call, document it in the planning doc, move on)

- **Pulsar image.** Vanilla `apachepulsar/pulsar:4.0.10` is the default in functional ITs but
  ShadowTraffic targets Kafka natively. You may need `streamnative/sn-platform:4.2.0.6` with
  KoP enabled, mirroring the `KafkaConsumerKoPIT` setup including the
  `functions_worker.yml` sed surgery for `ThreadRuntimeFactory` (see commit `54018c7`).
- **License env vars.** Six `LICENSE_*` env vars are required (see
  `tasks/todo-perf-shadowtraffic.md` for the list). Read them from environment at
  test-start; fast-fail with a clear message if any are missing. Document this in the perf
  module's README.
- **Container resource limits.** Default testcontainer Pulsar runs lean. For meaningful perf
  numbers, allocate ≥4 GB heap to the broker. Use
  `withCreateContainerCmdModifier(cmd -> cmd.withMemory(4L * 1024 * 1024 * 1024))`.

## Done state

- Phase 1-3 minimum: scaffolding + container wiring + first throughput scenario, all green
  with a license in env
- Each phase committed independently on `feat/perf-shadowtraffic`
- Top-level `README.md` gains one "Performance testing" pointer paragraph
- `mvn -B clean verify` (without `-Pperf`) on the reactor remains as fast as it is today
  (i.e., your work does not slow PR CI)
- `mvn -B -Pperf clean verify` runs the perf suite and produces JSON results

## Out of scope

- Grafana dashboards or any external observability ingestion
- PyPI/release distribution choices (separate concern)
- Pulsar resource-limit tuning beyond the container `withMemory` call
- Auto-bisect or regression-detection logic — the harness just emits JSON, downstream tooling
  (none today) consumes it

## When you finish

Stop on `feat/perf-shadowtraffic`. Tell the operator the branch is ready. The operator will
coordinate the merge of all three feature branches back into `main`.
