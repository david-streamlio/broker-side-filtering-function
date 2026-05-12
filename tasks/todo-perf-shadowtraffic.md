# Performance tests with ShadowTraffic — backlog plan

## Context

Today the build has functional ITs but **no performance / load tests**. We do
not know:

- Filter-function throughput under sustained load (msgs/sec, MB/sec).
- p50/p95/p99 latency from input-topic publish to output-topic delivery.
- How many concurrent queries a single filter function instance can sustain
  before selector evaluation cost becomes the bottleneck (the
  `O(N)`-in-active-query-count concern noted in the README "Selector
  evaluation cost" section).
- Orchestrator behavior under heartbeat traffic at scale (Phase 4 of
  `tasks/todo-sdk-heartbeat.md` is going to add 1 msg/30s per Session — at
  10K Sessions that is 333 msg/s of heartbeats alone).

ShadowTraffic is a synthetic data generator with a Docker image that
TestContainers can manage. It generates configurable streams (Kafka, Pulsar,
etc.) at controllable rates, so we can drive deterministic load against a
running broker + filter function and measure.

This was a **backlog plan**. Phases 1-2 are landed on branch `feat/perf-tests`;
Phases 3-7 remain backlog.

## Goals

1. A new Maven module `perf/` (or `functions/selector-filter/src/test/...`
   sibling) containing perf-classified tests that run separately from
   functional ITs.
2. TestContainers wiring for ShadowTraffic alongside the existing Pulsar
   container, with a JSON config file generating realistic trade-like records
   onto an input topic.
3. A set of benchmark scenarios with documented expected ranges; CI runs them
   on tagged builds (not every PR — wall time is too high).
4. JSON/CSV result output that can feed a Grafana board or a long-running
   regression chart.

## Container configuration reference

Per the gist
[`MichaelDrogalis/98669c80a137b99354c5902a9c344d2c`](https://gist.github.com/MichaelDrogalis/98669c80a137b99354c5902a9c344d2c),
the TestContainers shape is:

```java
GenericContainer<?> shadowTraffic = new GenericContainer<>(
        DockerImageName.parse("shadowtraffic/shadowtraffic:latest"))
    .withEnv("LICENSE_ID",           System.getenv("SHADOWTRAFFIC_LICENSE_ID"))
    .withEnv("LICENSE_EMAIL",        System.getenv("SHADOWTRAFFIC_LICENSE_EMAIL"))
    .withEnv("LICENSE_ORGANIZATION", System.getenv("SHADOWTRAFFIC_LICENSE_ORGANIZATION"))
    .withEnv("LICENSE_EDITION",      System.getenv("SHADOWTRAFFIC_LICENSE_EDITION"))
    .withEnv("LICENSE_EXPIRATION",   System.getenv("SHADOWTRAFFIC_LICENSE_EXPIRATION"))
    .withEnv("LICENSE_SIGNATURE",    System.getenv("SHADOWTRAFFIC_LICENSE_SIGNATURE"))
    .withFileSystemBind("/path/to/local/config.json", "/home/config.json")
    .withExposedPorts(9400)         // metrics endpoint
    .withCommand("--config", "/home/config.json");
```

License env vars are operator-supplied at run time — they MUST come from a
secret store / env, never committed. The repo will read them from
environment with a fast-fail message if absent.

ShadowTraffic config (the JSON file) declares generators (record shape, rate)
and connections (target broker / topic). For our purposes, we want to
generate trade-like records onto a Pulsar topic — either via the Kafka
connection (driving KoP) or, if ShadowTraffic supports it, directly via
Pulsar. The gist does not include a literal Kafka or Pulsar config example,
so the kickoff for this work needs a `shadowtraffic --help` exploration pass
or a doc read at https://docs.shadowtraffic.io.

## Module shape

```
perf/                                              -- new Maven module
  pom.xml                                          -- skipTests by default; profile -Pperf turns it on
  src/test/java/io/streamnative/filter/perf/
    PerfHarnessIT.java                             -- shared base: Pulsar + ShadowTraffic containers
    FilterThroughputBench.java                     -- sustain 50K msg/s through one filter; record stats
    SelectorScalingBench.java                      -- 1, 10, 100, 1000 active queries per filter; record cost
    OrchestratorHeartbeatBench.java                -- 1K, 10K simulated Sessions sending HEARTBEATs
  src/test/resources/
    shadowtraffic/trades-50k-per-sec.json          -- generator config
    shadowtraffic/trades-burst.json
```

## Phases

| # | Phase | Status | Outcome |
|---|---|---|---|
| 1 | Module scaffolding | DONE | New `perf/` Maven module, parent POM update, surefire/failsafe configured with the `perf` profile so it is OFF by default. |
| 2 | ShadowTraffic container wiring | DONE (code only — not yet exercised against a real license) | `PerfHarnessIT.java` base + `PerfSmokeIT`: KoP-enabled Pulsar container + ShadowTraffic container on a shared Docker network, license env vars read from environment, fast-fail if absent, `@EnabledIfEnvironmentVariable` so the suite skips cleanly without a license. Smoke test: generate 1K records, assert a probe Pulsar consumer reads them back. |
| 3 | First scenario — throughput | TODO | `FilterThroughputBench`: 5-minute sustained run at a configurable target rate. Measures: actual achieved rate, output-topic rate, latency percentiles (via brokerEntryMetadataInterceptor + a probe consumer that timestamps receive). Emit results as a JSON file under `perf/target/reports/`. |
| 4 | Scenario — selector scaling | TODO | `SelectorScalingBench`: spin up N active queries (via the orchestrator path) on one filter function. Measure throughput as N grows. Confirms the O(N) selector-eval observation in the README. |
| 5 | Scenario — heartbeat overhead | TODO | `OrchestratorHeartbeatBench`: simulate K Sessions each sending HEARTBEATs at the default 30s interval. Measure orchestrator queue depth + processing latency on its input topic. Validate the R1 risk note in `tasks/todo-sdk-heartbeat.md`. |
| 6 | CI integration | TODO | Tag-triggered GitHub Actions workflow that runs `mvn -Pperf verify`. Results uploaded as a workflow artifact. NOT on every PR — wall time is 20-30 min. |
| 7 | Grafana / Datasette dashboard (optional) | TODO | Long-running regression chart fed by the per-run JSON output. Out of scope for v1 of this work. |

## Result schema (per benchmark)

```json
{
  "benchmark":     "FilterThroughputBench",
  "git_sha":       "0bc11a2",
  "ran_at":        "2026-05-12T20:00:00Z",
  "duration_sec":  300,
  "target_rate":   50000,
  "achieved_rate": 49832.4,
  "output_rate":   12471.1,
  "latency_ms": { "p50": 1.2, "p95": 4.8, "p99": 12.1, "max": 89.0 },
  "broker_cpu_avg":  0.41,
  "filter_cpu_avg":  0.83,
  "notes": "..."
}
```

The schema is durable: each new benchmark drops a JSON file under
`perf/target/reports/`. Long-term, a small script aggregates these across
git SHAs into a regression chart.

## Cross-cutting concerns

- **License handling.** ShadowTraffic is a paid product. The license env vars
  are operator-supplied; the harness fast-fails with a clear message if any
  are absent. Document in README under a new "Performance testing" section.
- **Wall time.** Each scenario runs for 5+ minutes. The whole perf suite is
  20-30 min. Keep it OFF by default behind `-Pperf` so PR CI does not eat it.
- **Reproducibility.** Pin ShadowTraffic image tag (not `latest`) once we
  pick a working version. Same for the Pulsar image (already pinned to
  `apachepulsar/pulsar:4.0.10`).
- **Bookkeeper / broker resource limits.** Default testcontainer Pulsar runs
  with modest heap/cpu. For meaningful perf numbers, allocate more — e.g.
  `withCreateContainerCmdModifier(cmd -> cmd.withMemory(4L * 1024 * 1024 *
  1024))`. Document the test-host minimum (probably 8 CPU / 8 GB RAM).
- **Pulsar vs Kafka target on ShadowTraffic side.** If ShadowTraffic does
  not have a native Pulsar connector, we use Kafka-on-Pulsar (KoP is already
  validated in `KafkaConsumerKoPIT`). The existing `pulsar_non_batched`
  setting applies.

## Verification

- Phase 2 smoke test must show ShadowTraffic actually delivers records to the
  Pulsar broker (probe consumer reads them).
- Phase 3+ benchmarks emit reproducible JSON. Two consecutive runs on the
  same host with the same config should yield numbers within a small
  tolerance (e.g. ±10% on throughput, ±20% on p99 latency).

## Risks

- **R1: License procurement.** ShadowTraffic license must be in place before
  the harness can run. Document this prominently and short-circuit if the
  env vars are missing.
- **R2: ShadowTraffic config drift.** The product evolves; configs that work
  today may need adjustment. Mitigation: pin the image version and commit
  the config JSON to the repo (under `perf/src/test/resources/`).
- **R3: Container resource limits on CI runners.** GitHub Actions default
  runners have 2 CPU / 7 GB RAM — too small for meaningful perf numbers.
  Mitigation: run the perf workflow on a self-hosted runner or a larger CI
  tier, OR have CI run a smaller "smoke perf" scenario and full benchmarks
  only on tagged releases.
- **R4: Heartbeat-traffic perf depends on heartbeat being implemented.**
  Scenario in Phase 5 cannot run until `tasks/todo-sdk-heartbeat.md` is
  shipped. Mark as blocked-on until then.
- **R5: Latency measurement methodology.** Naïve "publish time minus receive
  time" suffers from clock-skew between containers. Use Pulsar's
  `brokerEntryMetadataInterceptor` (already enabled in some ITs) plus a
  single-process probe consumer to keep all clocks in one JVM.

## Phase 1-2 implementation notes

Landed:

- `perf/pom.xml` — jar packaging, parent dependencyManagement, failsafe
  off by default (`<skipITs>true</skipITs>`), `perf` profile flips it on.
- `perf/src/test/java/io/streamnative/filter/perf/PerfHarnessIT.java` —
  shared base with `requireLicenseEnvOrFail()`, `newPulsarKoPContainer()`,
  `newShadowTrafficContainer()`, and a `renderConfig()` helper that does
  `${name}` placeholder substitution on a classpath JSON template.
- `perf/src/test/java/io/streamnative/filter/perf/PerfSmokeIT.java` —
  Phase-2 smoke: 1K records via ShadowTraffic → KoP topic → probe
  consumer. Gated with `@EnabledIfEnvironmentVariable` on
  `SHADOWTRAFFIC_LICENSE_ID`.
- `perf/src/test/resources/shadowtraffic/trades-smoke.json` —
  generator template with `${bootstrap}` / `${topic}` / `${records}`
  placeholders.
- Root `README.md` — new "Performance testing" subsection under "Building".

Verified locally:

- `mvn -pl perf -am clean test-compile` compiles cleanly.
- `mvn -pl perf verify` (no `-Pperf`) reports "Tests are skipped" — zero
  PR-CI impact.
- `mvn -pl perf -Pperf verify` without license env vars discovers the IT
  and skips it via JUnit's `@EnabledIfEnvironmentVariable`, exiting 0.

NOT verified (no license available at landing time):

- That the ShadowTraffic container starts against the chosen image tag.
- That the trades-smoke.json generator config is accepted by ShadowTraffic
  as-is — the field shapes are based on docs.shadowtraffic.io conventions
  but a real-license run will likely surface tweaks. See R2.
- That records actually flow KoP → Pulsar in this exact topology. KoP
  itself is validated by `KoPFilterFunctionIT`; the new piece is the
  ShadowTraffic-side Kafka connection over the shared Docker network.

Phase-3 entry points:

- New IT next to `PerfSmokeIT` extending `PerfHarnessIT`.
- New config under `src/test/resources/shadowtraffic/` (e.g.
  `trades-50k-per-sec.json`).
- Output JSON written to `perf/target/reports/<benchmark>-<git-sha>.json`
  per the schema in the "Result schema" section above.
