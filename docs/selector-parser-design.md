# Design: homegrown JMS-selector engine

**Status:** implemented (P1–P4) · **Decisions locked:** strict JMS parity first; differential-gated phased rollout.

## 1. Motivation & goals

Today both the filter function and the SDK evaluate/validate selectors via
`io.streamnative:pulsar-jms-filters` (which wraps Apache ActiveMQ's JMS selector parser). Replacing
it with a small engine we own gives us:

- **Decoupling** — drops `pulsar-jms-filters` and its transitives (`pulsar-jms-activemq-filters`,
  `jakarta.jms-api`, `hawtbuf`, the ActiveMQ parser) and removes the `javax.jms.JMSException`
  coupling from our APIs.
- **Grammar ownership** — lets us extend the grammar later (e.g. nested dotted identifiers
  `address.city`, which the JMS grammar rejects), on our schedule.
- **Purpose-built evaluation** — no JMS `Message` abstraction; evaluate straight against our
  `name → Object` resolver, and derive `referencedNames` from the AST (retiring the regex in
  `SelectorEvaluator.extractReferencedNames`).

**Goal of v1:** byte-for-byte behavioral parity with the current JMS selector semantics, proven by
differential testing, with the dependency removed.

## 2. Non-goals (v1)

- No grammar extensions (nested paths, functions, new operators) — deferred to P4.
- No change to the binding model: the resolver stays `name → Object` returning
  `String` / `Number` / `Boolean` / `null`. `MessageBindings` is untouched.
- No change to the value decoders or the orchestrator.

## 3. Current integration points (what changes)

| Site | Today | After |
|---|---|---|
| `functions/selector-filter` · `SelectorEvaluator.compile` / `matches` | `SelectorSupport.build` + `support.matches(lookup)` | `Selector.compile` + `selector.matches(resolver)` |
| `sdk/java/pulsar` · `Selectors.validate` | `SelectorSupport.build` | `Selector.compile` (parse-only) |

Both already feed the same `name → Object` lookup, so only the compile/evaluate calls and the thrown
exception type change.

## 4. Grammar (the JMS message-selector subset)

Case-insensitive keywords: `AND OR NOT BETWEEN LIKE IN IS NULL TRUE FALSE ESCAPE`.

```
selector       := orExpr
orExpr         := andExpr ( OR andExpr )*
andExpr        := notExpr ( AND notExpr )*
notExpr        := NOT notExpr | predicate
predicate      := additive ( comparisonTail )?         // predicates do NOT chain (a = b = c is invalid)
comparisonTail := compOp additive
                | [NOT] BETWEEN additive AND additive
                | [NOT] LIKE stringLit [ESCAPE stringLit]
                | [NOT] IN '(' stringLit ( ',' stringLit )* ')'
                | IS [NOT] NULL
compOp         := '=' | '<>' | '>' | '>=' | '<' | '<='
additive       := multiplicative ( ('+'|'-') multiplicative )*
multiplicative := unary ( ('*'|'/') unary )*
unary          := ('+'|'-') unary | primary
primary        := '(' orExpr ')' | literal | identifier
identifier     := [A-Za-z_$][A-Za-z0-9_$]* ( '.' [A-Za-z_$][A-Za-z0-9_$]* )*   // dotted paths (P4b); and not a keyword
literal        := stringLit | exactNumeric | approxNumeric | TRUE | FALSE
```

- **String literal:** `'...'` with doubled-quote escape (`'O''Reilly'`).
- **Exact numeric:** optional sign + digits → `Long`.
- **Approximate numeric:** digits with `.` and/or exponent → `Double`.
- **Identifier:** simple or **dotted** (`address.city`) — the dotted form was added in P4(b) and is
  resolved by `MessageBindings.traverse`; the JMS grammar rejects it.
- **Precedence (high→low):** `()` · unary `+ -` · `* /` · `+ -` · comparisons/`BETWEEN`/`LIKE`/`IN`/`IS NULL` · `NOT` · `AND` · `OR`.
  Note `NOT` binds **looser** than comparison (conventional precedence) — this is the P4(a)
  divergence from ActiveMQ; see §8a.

## 5. Semantics (the fidelity-critical part)

Evaluation yields a **ternary** `{ TRUE, FALSE, UNKNOWN }`; a record matches **iff the result is
`TRUE`**.

**Three-valued logic**

```
NOT:  TRUE→FALSE   FALSE→TRUE   UNKNOWN→UNKNOWN

AND |  T  F  U          OR  |  T  F  U
----+---------          ----+---------
 T  |  T  F  U           T  |  T  T  T
 F  |  F  F  F           F  |  T  F  U
 U  |  U  F  U           U  |  T  U  U
```

**Operand resolution.** An identifier resolves via the resolver to `String`/`Number`/`Boolean`, or
`null` if absent. `null` operands propagate per the rules below.

**Comparisons**
- `=`, `<>`: defined for (numeric,numeric) with widening, (string,string), (boolean,boolean). Any
  cross-type pair or a `null` operand → `UNKNOWN`.
- `<`, `<=`, `>`, `>=`: **numeric only** (with widening). String/boolean operands or `null` →
  `UNKNOWN`. *(This is the documented quirk: `price > 100` against a String property → `UNKNOWN` → no
  match.)*
- `BETWEEN a AND b` ≡ `x >= a AND x <= b` under 3VL (numeric).
- `LIKE`: string identifier vs pattern; `%` → any run, `_` → any single char, `ESCAPE c` escapes the
  next metachar; non-string or `null` operand → `UNKNOWN`. Implement as pattern→regex compiled once.
- `IN (s1, …)`: string-membership; identifier `null` → `UNKNOWN`. `NOT IN` negates a definite result,
  leaves `UNKNOWN` as `UNKNOWN`.
- `IS NULL` / `IS NOT NULL`: definite `TRUE`/`FALSE` presence test (never `UNKNOWN`).

**Arithmetic** (`+ - * /`, unary `-`): numeric operands → numeric (widen to `Double` if any operand
is approximate); a `null` operand → `null`, which makes an enclosing comparison `UNKNOWN`.

## 6. Architecture

```
String ─► Lexer ─► Token[] ─► Parser ─► Expr (AST) ─► Selector (immutable, compiled once)
                                              │                     │
                                       referencedNames        matches(resolver) ─► Ternary ─► boolean (==TRUE)
```

- `Lexer` — hand-written scanner producing typed tokens (string/number/boolean literals,
  identifiers, operators, punctuation, keywords). Handles `''` escapes and numeric forms.
- `Parser` — recursive-descent (or Pratt for the operator tiers) over the grammar above; produces an
  `Expr` tree and collects identifier names. Syntax errors → `SelectorParseException` with position.
- `Expr` — sealed hierarchy: `And`, `Or`, `Not`, `Comparison`, `Between`, `Like`, `In`, `IsNull`,
  `Arithmetic`, `Literal`, `Identifier`. Each implements `eval(resolver) → Ternary|Value`.
- `Selector` — the public compiled object: `matches(Function<String,Object> resolver)`,
  `referencedNames()`, `expression()`.

## 7. Module placement & API

Lives in **`common`** (both function and SDK already depend on it), new package
`io.streamnative.filter.common.selector`:

```java
public final class Selector {
    public static Selector compile(String expr) throws SelectorParseException;
    public boolean matches(java.util.function.Function<String,Object> resolver);
    public java.util.Set<String> referencedNames();
    public String expression();
}
public final class SelectorParseException extends RuntimeException { /* message + position */ }
```

- `SelectorEvaluator` becomes a thin wrapper over `Selector` (keeps `referencesOnlyKey()` etc., now
  backed by `referencedNames()` from the AST instead of the regex).
- `Selectors.validate` calls `Selector.compile` and maps `SelectorParseException` →
  `IllegalArgumentException` (unchanged external behavior).
- `JMSException` disappears from our signatures; tests asserting it switch to `SelectorParseException`.

## 8. Conformance — differential testing (the de-risker)

Failure mode is *silent mis-filtering*, so parity is gated by an **oracle**: keep
`pulsar-jms-filters` as a **test-scope** dependency through P1–P2 and run a corpus of
`(selector, bindings)` cases through **both** engines, asserting identical `matches` results (and
identical accept/reject on compile).

Corpus:
- Hand-written edge cases: 3VL combinations, `LIKE` with `%`/`_`/`ESCAPE`, type mismatches,
  `NULL`/`IS NULL`, `BETWEEN`, nested parens, precedence, `''` escapes, sign/exponent numerics.
- Real-world selector patterns from existing tests and docs.
- Optional: property-based/randomized generation of selectors + bindings for breadth.

Parity must be green before the call sites flip (P2). The implemented suite has two layers: a curated
edge-case corpus and a seeded property-based sweep (thousands of generated compound selectors ×
randomized bindings). The oracle keeps `pulsar-jms-filters` as a **test-scope** dependency in
`common` (`SelectorOracleTest`) even after P3 drops it from compile scope — it remains the regression
safety net for the engine.

## 8a. Model & known divergences (hybrid stance)

The differential approach makes **ActiveMQ** (via `pulsar-jms`) the de-facto behavioral model. We
adopt a **hybrid** north star rather than bug-for-bug replication:

- **Realistic selectors** (string properties with `=`/`<>`/`LIKE`/`IN`/`IS NULL`; properly-typed
  numeric/decoder fields with comparisons, `BETWEEN`, arithmetic; any `AND`/`OR`/`NOT`/parens
  composition) — **strict parity**, enforced by the green oracle gate. No behavior change.
- **Pathological / mistyped fringe** — where ActiveMQ is itself **internally inconsistent**, we use
  clean, *consistent* semantics and document the divergence instead of replicating the contradiction.

Known, intentional divergences from ActiveMQ (all immaterial for real selectors):

1. **Numeric op on a non-numeric (e.g. String) operand → UNKNOWN, consistently** across comparison,
   `BETWEEN`, and arithmetic. ActiveMQ is inconsistent here: an ordering comparison on a String
   yields definite `FALSE` (so `NOT (price >= 100)` on a String is `TRUE`), while `BETWEEN` on the
   same String yields UNKNOWN (so `NOT (price BETWEEN ...)` is no-match), and arithmetic on a String
   does ad-hoc parse-or-throw coercion. We treat all three as UNKNOWN. Equality (`=`/`<>`) stays
   definite (values are equal or not), matching ActiveMQ.
2. **`NOT` precedence (fixed in P4(a) — deliberate divergence).** ActiveMQ binds `NOT` *tighter* than
   comparison, so `NOT a = b` parses as `(NOT a) = b`. We now use the **conventional** precedence
   where `NOT` binds *looser* than comparison, so `NOT a = b` ≡ `NOT (a = b)` (see the grammar in §4).
   This is an intentional behavior change from ActiveMQ. To keep the oracle meaningful, bare
   `NOT <comparison>` selectors are excluded from the differential gate; parenthesized `NOT (...)`
   evaluates identically in both engines and stays gated.

## 9. Phased rollout (one PR each)

- **P1 — Engine + oracle.** Implement `common/.../selector/*` + unit tests + the differential suite
  (oracle = pulsar-jms, test scope). No production call site changes; zero behavior change.
- **P2 — Switch call sites.** Point `SelectorEvaluator` and `Selectors.validate` at `Selector`;
  swap `JMSException` → `SelectorParseException`. `pulsar-jms` stays test-scope (oracle still runs).
- **P3 — Drop the dependency.** Remove `pulsar-jms-filters` from compile scope (and the 4
  transitives); keep the oracle suite (test-scope in `common`). Benchmark via the `perf` module to
  confirm no evaluation regression.
- **P4 — Extensions (done).**
  - **P4(a) — `NOT` precedence.** Fixed `NOT` to bind looser than comparison (conventional), a
    documented divergence from ActiveMQ (§8a item 2). Bare `NOT a = b` dropped from the oracle gate;
    parenthesized `NOT (...)` stays gated.
  - **P4(b) — Nested dotted identifiers (`address.city`).** Dotted-identifier grammar in the lexer;
    each value decoder (JSON / Avro / Protobuf-native) emits **nested maps**; `MessageBindings.traverse`
    walks the dotted path. Arrays / maps / repeated protobuf fields are not addressable.

## 10. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Semantic divergence (3VL, `LIKE`, coercion, `NULL`) | Differential oracle gates P2; truth tables/coercion specified above |
| Back-compat with selectors in production | Corpus includes real-world patterns; phased + reversible PRs |
| Evaluation performance regression | `perf`-module benchmark in P3; parse-once/eval-many design |
| Long-term maintenance of an owned parser | Small, standard, fully-tested; offset by dropping an external dep |

## 11. Effort

~800–1,200 LOC (lexer + parser + evaluator) plus a substantial test/oracle suite. The engine is
standard; the cost is semantic fidelity and the conformance corpus. Three PRs (P1–P3), plus P4
(P4(a) NOT-precedence + P4(b) nested paths), both shipped.
