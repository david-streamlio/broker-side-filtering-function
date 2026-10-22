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

---

# Micro-benchmarks (selector eval + value decode) — handoff §6

In-JVM nanobenchmarks, separate from the broker load tests above. They confirm two things behind the
homegrown-selector-engine + value-schema work: the homegrown engine is no slower than the
`pulsar-jms` parser it replaced on the eval hot path, and the value-decode path is sub-microsecond.

**Why they don't live in this module.** The value decoders (`AvroValueDecoder`,
`ProtobufNativeValueDecoder`, `ValueDecoderFactory`) are **package-private** in
`io.streamnative.filter`, so Bench B can only compile inside the function module's own test tree —
widening their visibility just for a benchmark isn't worth it. Bench A needs the test-scope
`pulsar-jms-filters` that already lives in `common`. So each bench sits next to its fixtures:

| Bench | Location | Measures |
|---|---|---|
| `SelectorEvalBench` | `common/src/test/.../selector/` | homegrown `Selector.matches` vs `pulsar-jms` `SelectorSupport.matches`, ns/op |
| `ValueDecodeBench` | `functions/selector-filter/src/test/.../` | Avro + Protobuf-native `decode(byte[])` + lookup, top-level vs nested, ns/op |

The `*Bench` suffix matches **neither** surefire's (`*Test`/`*Tests`) nor failsafe's (`*IT`) default
include patterns, so they never run in a normal build and don't count toward the module test totals.
Run them on demand (JDK 21):

```
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
mvn -pl common install -DskipTests
mvn -pl common test -Dtest=SelectorEvalBench -Dsurefire.failIfNoSpecifiedTests=false
mvn -pl functions/selector-filter test -Dtest=ValueDecodeBench -Dsurefire.failIfNoSpecifiedTests=false
# tune: -Dbench.iters=1000000 -Dbench.warmup=100000
```

## Results (JDK 21.0.1, warmup=50k, iters=500k; single dev laptop, indicative)

**Bench A — selector eval, ns/op:**

| selector | homegrown | pulsar-jms | speedup |
|---|---|---|---|
| `region = 'US'` | 18.5 | 83.4 | 4.51× |
| `region = 'US' AND tier = 'GOLD'` | 62.6 | 89.3 | 1.43× |
| `price > 100 AND price < 500` | 28.7 | 82.1 | 2.86× |
| `region IN ('US','UK','DE')` | 16.3 | 38.8 | 2.38× |
| `t LIKE 'persistent://public/trades/%'` | 65.4 | 107.4 | 1.64× |
| `price BETWEEN 100 AND 200 OR region IS NULL` | 27.8 | 64.1 | 2.31× |
| `region = 'US' AND price > 100 AND tier IN (...)` | 73.9 | 82.0 | 1.11× |
| **mean** | **41.9** | **78.1** | **1.87×** |

The homegrown engine evaluates **1.1×–4.5× faster (1.87× mean)** — it has no JMS `Message`
abstraction and evaluates straight against the `name → Object` resolver.

> The bench also prints a *compile* column. Ignore it for comparison: `pulsar-jms`'s
> `SelectorSupport.build` memoizes (its cost drops to ~11 ns as selectors get more complex — a
> cache-hit signature), while the homegrown `Selector.compile` parses on every call (~0.5–1.8 µs).
> Compile is **one-time per query** and amortized to nil by the parse-once/eval-many design, so eval
> is the number that matters.

**Bench B — value decode, ns/op:**

| case | decode | decode + lookup |
|---|---|---|
| avro top-level (4 scalars) | 246.8 | 217.7 |
| avro nested (`address.city`) | 301.9 | 314.4 |
| protobuf top-level (4 scalars) | 499.1 | 481.1 |
| protobuf nested (`address.city`) | 471.6 | 478.1 |

Decode is **sub-microsecond** — Avro ~250–300 ns, Protobuf-native ~470–500 ns. Field lookup
(top-level `get` or nested two-hop walk) is within measurement noise. Decode runs **once per message**
(cached across matching queries; see `ValueDecoder`'s javadoc), independent of registered-selector
count, so at the demo's ~100 selectors the eval loop (Bench A) still dominates per-message CPU.
