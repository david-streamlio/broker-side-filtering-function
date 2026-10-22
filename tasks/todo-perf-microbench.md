# Plan: §6 selector/value-decode micro-benchmarks

From `handoffselectorwork.md` §6 — the last code task in the value-schema / homegrown-engine arc.
Goal: confirm the homegrown selector engine is ≥ the `pulsar-jms` parser on eval, and size the
value-decode path relative to selector eval.

## Key constraint discovered
The value decoders (`AvroValueDecoder`, `ProtobufNativeValueDecoder`, `ValueDecoderFactory`,
`ValueDecoder`) are **package-private** in `io.streamnative.filter`. So Bench B **cannot** live in
the `perf` module without widening production visibility — which we will not do for a benchmark.
The two micro-benches therefore live next to their fixtures, in their owning modules' test trees.
This also means **zero pom changes** and **no change to the green-build test counts**: `*Bench`
classes match neither surefire's (`*Test`/`*Tests`) nor failsafe's (`*IT`) default include patterns,
so they only run when invoked explicitly with `-Dtest=...Bench`.

## Deliverables
- [x] **Bench A — selector eval.** `common/src/test/.../selector/SelectorEvalBench.java`.
      Compile-once / eval-many; compare homegrown `Selector.matches(resolver)` vs `pulsar-jms`
      `SelectorSupport.build(s,true).matches(resolver)` over a realistic selector corpus. Mirrors the
      resolver shape used in `SelectorOracleTest` (`Function<String,Object> = map::get`). Reports
      ns/op per selector + a compile-cost line. `pulsar-jms-filters` is already test-scope in `common`.
- [ ] **Bench B — value decode.** `functions/selector-filter/src/test/.../ValueDecodeBench.java`.
      Per-message Avro + Protobuf-native `decode(byte[])` + field lookup, top-level vs nested, reusing
      the exact fixtures from `AvroValueDecoderTests` (`Schema.AVRO(Trade/Order)`) and
      `ProtobufNativeValueDecoderTests` (`ProtobufNativeSchemaUtils.serialize` + `DynamicMessage`).
      Reports ns/op for decode-only and decode+lookup.
- [x] **perf/README** — short "Micro-benchmarks" section: where they live, why (package-private
      decoders), and the exact `-Dtest` commands. Record the measured ns/op numbers (JDK 21).
- [x] Run both with JDK 21; paste results into README + this file's Results section.
- [x] Confirmed default green counts unchanged: common 62 / function 133 (benches don't run by default).
- [x] **Deliverable B** filled — see Bench B in `functions/selector-filter/src/test/.../ValueDecodeBench.java`.

## Run commands (JDK 21)
```
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
mvn -pl common install -DskipTests
mvn -pl common -am test -Dtest=SelectorEvalBench -DfailIfNoTests=false
mvn -pl functions/selector-filter -am test -Dtest=ValueDecodeBench -DfailIfNoTests=false
```

## Non-goals
- No JMH module (perf harness style is manual timing + HdrHistogram; micro-benches use manual
  nanoTime with warmup + a DCE sink). No production-code changes. No live broker.

## Results (JDK 21.0.1, warmup=50k, iters=500k; dev laptop, indicative)

**Bench A — selector eval (ns/op):** homegrown **1.1×–4.5× faster, mean 1.87×** vs `pulsar-jms`
(mean 41.9 vs 78.1 ns). §6 hypothesis confirmed — the homegrown engine is ≥ JMS on the eval hot path.
Compile column is not comparable (`pulsar-jms` `build()` memoizes; homegrown parses each call) and is
amortized to nil by parse-once/eval-many — irrelevant to steady state.

**Bench B — value decode (ns/op):** Avro ~250–300, Protobuf-native ~470–500; field lookup within
noise. Sub-microsecond, decoded once per message (cached across queries). At the demo's ~100 selectors
the eval loop still dominates per-message CPU. §6 "value-decode path acceptable" confirmed.

Full tables in `perf/README.md` → "Micro-benchmarks" section.

**Status: §6 benchmark DONE.** Remaining handoff item: deploy smoke test (live broker, Avro + Protobuf).
