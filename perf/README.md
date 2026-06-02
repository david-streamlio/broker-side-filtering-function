# Selector Filter — Performance Harness

Load/latency harness for the broker-side selector filter. Off by default; enable with `-Pperf` at
the reactor root. Benchmark classes use the `*Bench` suffix (picked up by failsafe).

| Bench | Drives | Measures | Needs |
|---|---|---|---|
| `FilterThroughputBench` | ShadowTraffic → full SDK→orchestrator→filter pipeline | achieved rate + output→consume latency percentiles | a ShadowTraffic license (`SHADOWTRAFFIC_LICENSE_*`) |
| `SelectorScalingBench` | a plain Pulsar producer at a fixed sub-saturation rate, N registered selectors | the filter's own **value-add latency** percentiles, by selector count | no license |

## Running

CI (recommended — functions cold-start reliably on the runners): **Actions → Perf bench → Run
workflow** (`.github/workflows/perf-bench.yml`), inputs default to `selectorCounts=10,100`,
`targetRate=2000`, `duration=60`. Reports upload as the `selector-scaling-bench-reports` artifact.

Locally (needs a host that can cold-start the broker + spawned functions):
```
mvn -Pperf -pl perf -am verify -Dit.test=SelectorScalingBench \
    -Dperf.selectorCount=100 -Dperf.targetRate=2000 -Dperf.duration=60
```
JSON results land in `perf/target/reports/`.

---

# Latency optimization study (2026-06)

A worked example of **measure first, optimize second** — and why it matters.

## Goal

Minimize **span (b): filter input-receipt → output-published** — the filter's own value-add
latency. Demo registers ~10 selectors; the realistic case is ~100.

## Instrumentation

The filter records two matched-only, percentile-rendered metrics (gated by
`enable-processing-time-metric`):

- `filter.dispatch.time.ms` — **(b1)** entry → all sends dispatched: deserialize + selector
  evaluation + message build + producer enqueue. The CPU value-add.
- `filter.match.processing.time.ms` — **(b)** entry → all output sends acked. `(b) − (b1)`
  isolates the output-send component.

`SelectorScalingBench` registers N selectors (1 matching + N−1 non-matching, so every message runs
the full N-eval loop while fanout stays 1), drives a fixed **sub-saturation** rate, and reads those
percentiles from the function's `getFunctionStats`.

## What we found

At 2000 msg/s, 10 and 100 selectors:

| | b1 dispatch p50 | **b p50** |
|---|---|---|
| 10 selectors | 0.003 ms | 9.48 ms |
| 100 selectors | 0.011 ms | 9.90 ms |

Two things jump out:

1. **b1 (the eval loop) is microseconds** — and scales O(N) as expected (0.003 → 0.011 ms from
   10→100), but is **~0.1% of (b)**. Optimizing the eval loop (key-indexing, caching, etc.) would
   have been pure wasted effort.
2. **(b) is ~99.9% output-send** (≈9.5 ms), and **flat across selector count** — so the filter is
   **send-bound, not eval-loop-bound**. The ~9.5 ms is the Pulsar Functions framework's default
   output-producer **batching delay**.

## The fix

Set the spawned filter's output-producer batching via `FunctionConfig.producerConfig`, exposed as an
orchestrator knob:

```
filter-output-batch-max-publish-delay-ms   # default 1 (0 = batching disabled)
```

## Result

| b percentile | Baseline (10 / 100) | **1 ms batching** | + MessageBindings alloc cut |
|---|---|---|---|
| **p50** | 9.48 / 9.90 ms | **2.54 / 2.54 ms** | 2.63 / 2.61 ms |
| p90 | 25.3 / 20.4 ms | 38.5 / 40.1 ms | 46.4 / 27.3 ms |
| p99/p999 | 520 / 396 ms | 733 / 789 ms | 858 / 752 ms |

- **p50: 9.5 → 2.6 ms — a 3.6× cut in median value-add latency**, from a one-line config change.
- **Trade-off:** less batching lifts p90 (~20 → ~40 ms) — fewer/smaller batches mean more
  individual sends. Tune `...delay-ms` to 2–5 ms if p90 consistency matters more than absolute p50.
- **MessageBindings allocation reduction** (4 supplier lambdas/message → 0): correct, behavior-
  preserving hygiene, but **no measurable latency effect at this load** — the per-message
  allocation was never enough to move GC at 2000 msg/s. It pays off at much higher throughput.
- The **p99 (500–860 ms) is GC / cold-start / CI-runner jitter**, not representative — it varied
  with no trend across all three runs. A warmed JVM on dedicated hardware would be far tighter.

## Takeaways

- **Measure the span you care about before optimizing.** The instinctive target (the eval loop)
  was 0.1% of the latency; the real lever (producer batching) was 99.9%.
- **Latency ≠ throughput.** Batching helps throughput and tail-smoothing but adds median latency;
  the right setting depends on which you're optimizing.
- **Read percentiles, not means**, and **measure at sub-saturation** — latency at capacity is just
  queueing delay.
