# Scope of work: SQL functions in filters (LOWER, UPPER, CAST, …)

## Ask

Support a predefined subset of SQL functions in agent predicates, for example
`LOWER(event_type) = '3ds_timeout'`, `CAST(seq AS INT) > 5`, `COALESCE(region, 'US') = 'US'`.
The idea proposed: a **second filter** reads the first filter's output topic, applies the predicates that
use functions, and writes the final topic.

This scope covers that design (**Option B**) and a much cheaper alternative (**Option A**) that's open to
us because the selector engine is this repo's own code (`common/.../selector/`: `Lexer`, `Parser`,
`Expression`, `Values`), not a third-party library.

## Function subset (both options)

| Group | Functions | Notes |
|---|---|---|
| String | `LOWER`, `UPPER`, `TRIM`, `LENGTH`, `SUBSTRING(s, from[, len])`, `CONCAT` / `\|\|` | The common need is case-insensitive matching. |
| Conversion | `CAST(x AS INT \| BIGINT \| DOUBLE \| DECIMAL \| VARCHAR \| BOOLEAN)` | Fixes "headers are always strings": `CAST(seq AS INT) > 5`. A failed cast is UNKNOWN, never an error. |
| Null handling | `COALESCE`, `NULLIF` | |
| Numeric | `ABS`, `ROUND`, `FLOOR`, `CEIL`, `MOD` | |
| Out of scope | Date/time functions (beyond the `lookback` mapping in `todo-mcp-sql.md`), regex, JSON path functions, user-defined functions, aggregates, window functions | |

Semantics follow SQL's three-valued logic: NULL in gives NULL out (UNKNOWN in a predicate), which matches
how the engine already treats a missing field.

---

## Option A: functions in the selector engine (one filter, no new topics)

Extend the engine's grammar and evaluator so a function call is just another value expression. The one
filter that already reads the topic evaluates it; nothing new is spawned.

**Work:**
1. **Lexer and Parser** (`common/.../selector/`): function-call syntax `name(args)`, `CAST(x AS type)`, and
   `||`. A fixed registry of names; an unknown name is a parse error with the supported list.
2. **Expression nodes and evaluation:** a `Call` node with per-function implementations in a new
   `Functions` class, following `Values`' coercion and three-valued rules. `referencedNames()` must still
   see the fields inside calls, so the MCP field allowlist keeps working.
3. **Cost-aware evaluation:** functions on key and header fields stay metadata-only. Functions on body
   fields decode the value once (already cached per message). Short-circuit `AND`/`OR`, now in place, keeps
   cheap terms first.
4. **Broker EntryFilter:** works unchanged for key and header functions, since it evaluates the same
   `Selector` on metadata.
5. **SDK validation** (both SDKs call `Selector.compile`): nothing to do; new syntax validates for free.
6. **MCP:** `check_selector` and `SelectorAdvice` learn functions (fields inside calls, cost, type hints).
   The SQL translator (`todo-mcp-sql.md`) maps SQL functions one-to-one instead of rejecting them.
7. **Grammar documentation:** the README "Selector language" section. This makes the grammar a
   **superset of JMS**, the way dotted paths already are. Say so plainly: selectors using functions won't
   run on other JMS engines.
8. **Tests:** unit tests per function (NULL, type and edge cases); parser tests; the short-circuit and
   allowlist interactions. The `SelectorOracleTest` differential oracle (ActiveMQ) has no functions, so new
   tests are hand-written tables. Add an M4-style microbenchmark: `LOWER` on a header must stay well under a
   microsecond per event.

**Estimate: 5–8 working days.** Low risk: a contained change in one module, existing consumers are
unaffected, and old selectors parse and evaluate exactly as before.

---

## Option B: a second, chained filter for function predicates

Stage 1 is today's filter on the source topic, running the JMS-expressible part of the predicate and
writing to a private intermediate topic. Stage 2 is a new "function filter" that reads that topic,
evaluates the remaining predicate, and writes the final private topic the client reads.

**Work:**
1. **Predicate splitting.** Rewrite the predicate into conjunctive form; top-level `AND` terms without
   functions go to stage 1, the rest to stage 2. A function inside an `OR` with plain terms can't be
   pushed down: stage 1 then needs a weaker *superset* predicate (or none), so it forwards more. Correctness
   rule: stage 1 may let extra events through, never drop one stage 2 would keep. (~3–4 days, with
   property-based tests.)
2. **A function-evaluation engine for stage 2.** It needs Option A's evaluator anyway (or an embedded SQL
   evaluator such as Calcite, which is far heavier). **Option B includes Option A's work.**
3. **A new function type (stage 2).** It consumes a private topic and forwards matches with their
   properties, `__src_msgid__` (dedupe) and publish times intact. (~1 week)
4. **Window protocol across two hops** (the hardest part). Stage 2 must:
   - pass through stage 1's completion record, `caught up` marker and `WindowStats`, so `COMPLETE` and
     `TRUNCATED_BY_RETENTION` still mean what they say;
   - adjust `matched` counts;
   - keep live windows open end to end;
   - handle stage 1 ending before stage 2 has drained.

   Today that protocol is one hop (`WindowState`, `WindowProtocol`, `WindowControl`). (~1–1.5 weeks)
5. **Orchestrator.** Register, spawn, connect and tear down chained pairs. Route control messages
   (`REGISTER` / `MODIFY` / `UNREGISTER`) to both stages. Handle schema checks on the intermediate topic.
   Extend the registry's state so idle GC and `PrivateTopicPolicy` reap intermediate topics, and recover a
   half-built chain after a restart. (~1–1.5 weeks)
6. **Reuse and sharing (where it gets complicated, as you expected).** Today there's one filter function
   *per input topic* evaluating many queries, each with its own output topic; windowed queries get a
   filter each.
   - Stage 2 per query is simple but multiplies functions and topics.
   - A shared stage 2 per stage-1 topic (or per function signature) needs a second registry, fan-out to
     per-query outputs, and reference counting so one query's teardown doesn't kill another's chain.
   - Sharing stage 1 between queries with different stage 2 predicates means stage 1 runs the *union*
     of their pushdowns.
   - Reconfiguring a query with `MODIFY` can move terms between stages, which means rebuilding the chain
     mid-stream without losing or duplicating events.
   (~1.5–2 weeks for per-query stage 2 plus a sharing design; full sharing is more.)
7. **Delivery guarantees.** At-least-once end to end with dedupe on `__src_msgid__` across both hops.
   Ordering per key is preserved only if both stages read single partitions in order, as today. A stage 2
   crash and restart must resume from its subscription without re-emitting already-acked results. (~3–4 days)
8. **SDK and MCP:** the SDK's builder accepts function predicates (splitting can happen in the SDK or the
   orchestrator; the orchestrator is better, so clients stay thin). `FilterControl`/`WindowControl` see one
   query, not two. MCP: same as Option A item 6.
9. **Observability and operations:** stage-2 metrics, chain health, and for each query which stage
   dropped how much. Docs for operators.
10. **Testing:** integration tests across the chain (Docker), failure injection (kill stage 1 or 2
    mid-window), a windowed end-to-end test (COMPLETE and TRUNCATED through two hops), and measurements:
    the extra latency of the second hop (another live-delivery leg on top of today's 9–21 ms), the extra
    ~2 s start for the second function, and the topic and storage overhead of the intermediate stream.

**Estimate: 5–8 weeks** for a per-query second stage with correct window semantics; **add 2–4 weeks**
for shared, reference-counted second stages and in-place `MODIFY`. High risk: the window protocol and
chain lifecycle are new failure surfaces, and every query pays a second start, a second hop and a second
topic.

---

## Comparison

| | Option A: functions in the engine | Option B: chained second filter |
|---|---|---|
| Effort | 5–8 days | 5–8 weeks, plus 2–4 for sharing |
| New moving parts | none | a function type, intermediate topics, chain lifecycle, two-hop window protocol |
| Latency | unchanged | a second hop and a second ~2 s start |
| Broker work | unchanged; short-circuit keeps body decodes rare | a second read and write of the stage-1 output |
| Window and completeness semantics | unchanged | must be re-proven across two hops |
| Reader reuse | unchanged (one filter per input topic) | new sharing problem at stage 2, and union pushdowns at stage 1 |
| Portability | selectors with functions are a superset of JMS | the same, but hidden behind splitting |
| When it's the right call | functions that run per event on one event's fields: all of the subset above | stage 2 needing what a per-event evaluator can't do: joins, enrichment from another source, stateful or windowed logic, or a full SQL engine (Flink SQL / Calcite) |

## Flink in the JVM: where it fits

Flink can run inside a single JVM: `StreamExecutionEnvironment` starts an embedded MiniCluster in the
same process, and `StreamTableEnvironment` on top of it runs Flink SQL with `executeSql(...)`. The SQL
Client's default "embedded" mode works the same way. The artifacts are `flink-streaming-java`,
`flink-table-api-java-bridge`, `flink-table-planner` and `flink-table-runtime`. Could it give us functions
without writing an evaluator?

**Not inside the filter (Option A's job).**
- **It runs jobs, not expressions.** Flink takes a stream in and produces a stream out. There's no
  supported public API to evaluate one predicate on one record from inside the filter's per-message loop;
  the code-generated evaluators in the planner are internal.
- **It's heavy for a Pulsar Function.** The planner and runtime add tens of MB of dependencies, start in
  seconds, and bring their own threads, memory and checkpointing. Today's filter evaluates a parsed
  selector in microseconds per event (M4).
- **Classloading:** a Pulsar Function's classloader together with Flink's dependencies (Calcite, Janino,
  shaded libraries) is a known source of conflicts.

**As Option B's second stage, yes, when stage 2 needs real stream processing.** If a query needs what
a per-event evaluator can't do (a join with another stream, enrichment, aggregation over time,
deduplication across events), an in-JVM Flink job is a sound stage 2:
- **Stage 1** (our filter) does the cheap, selective pushdown, so Flink sees a small stream.
- **Stage 2** runs Flink SQL over the stage-1 private topic (the Pulsar connector) and writes the final
  topic, with functions, joins and windows for free.

Option B's hard parts remain:
- **Window protocol:** a Flink job doesn't speak our completion records or `caught up` marker, so the
  orchestrator has to wrap it to keep `COMPLETE` and `TRUNCATED_BY_RETENTION` honest.
- **Lifecycle and sharing:** starting, connecting and tearing down the chain, and sharing stage 2 between
  queries.
- **Start time:** seconds for a Flink job, on top of the filter's ~2 s.
- **Resources:** a MiniCluster per query is expensive. Sharing one Flink runtime across queries is, in
  effect, running a Flink cluster.

This is roughly Confluent RTCE's architecture (Flink upstream, a serving layer in front), so choosing it
moves the design toward RTCE's rather than away from it. Scope it as its own feature, "a
stream-processing stage after the filter", not as the way to get `LOWER`. **Estimate on top of Option B:
roughly 2–3 weeks** to embed and wrap Flink SQL as the stage-2 engine (connector setup, protocol wrapper,
job lifecycle, resource limits, tests), replacing Option B's custom function filter.

**A cheaper piece to borrow: Calcite.** Flink SQL's parser and validator are Apache Calcite. For the SQL
translator (`todo-mcp-sql.md`), Calcite would parse agent SQL by the same rules Flink SQL uses, without the
Flink runtime. It's heavier than JSqlParser but gives Flink/RTCE-style SQL compatibility. Decide when
Phase 1 of the translator starts.

## Recommendation

Do **Option A**. Every function in the subset is a pure function of one event's fields, which is
exactly what the filter already evaluates per event. Putting them in the engine costs about a week and
adds nothing to deploy. Keep the chained-filter design (**Option B**) for when stage 2 needs something a
per-event evaluator can't do, such as enrichment, joins or stateful logic. At that point it's really a
stream-processing stage, and the trade-off against running Flink should be weighed explicitly.

Sequencing with the SQL work: Option A first, then `todo-mcp-sql.md` Phase 1 maps SQL functions directly
instead of rejecting them, so the two together give agents RTCE-style SQL including `LOWER`/`CAST`.

## Open questions

- **CAST target types and rounding:** `DECIMAL` precision, and `CAST('7.9' AS INT)` truncating or failing
  (SQL says error; we'd return UNKNOWN). Pick and document.
- **Collation for LOWER/UPPER:** use `Locale.ROOT`, to avoid the Turkish-i problem.
- **Function names that collide with field names** (`length`, `trim`): call syntax decides (`length(x)`
  is the function; bare `length` is the field). Confirm the parser can tell them apart.
- **Whether to expose functions to SDK users** or only through MCP and SQL. The engine change makes them
  available everywhere `Selector.compile` runs, unless gated.
