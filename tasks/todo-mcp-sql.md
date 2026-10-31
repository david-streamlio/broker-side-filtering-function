# MCP: predicates in SQL, translated to JMS selectors

## Goal

Let an agent express a read in SQL, the way it would against Confluent's Real-Time Context Engine
(`queryData` takes `SELECT … WHERE … ORDER BY … LIMIT`), and have the `entity-events` MCP server
translate it into what the filter actually runs: a JMS selector plus the tool's own arguments
(`topic`, `lookback`, `max_events`). Agents write SQL fluently. A JMS selector is already a SQL-92
`WHERE` clause, so most of the work is the parts of a SQL statement *around* the `WHERE`, and clear
rejections for what JMS can't express.

## Locked design decisions

- **Scope: the MCP module only** (`mcp/entity-events`). No new Maven module and no SDK API this
  iteration. The SDKs keep `.selector(...)`. If applications want SQL later, the translator moves to its
  own module next to `common` (not into `common`, which ships inside the Pulsar Function and the broker
  filter, and shouldn't carry a SQL parser).
- **Output is a selector string**, run through the existing checks unchanged: length cap, then
  `Selector.compile`, then field allowlist from `get_schema`. The translator adds no new trust: anything
  it emits is checked as if the agent had written it.
- **A new `sql` argument** on `get_entity_events`, `watch_entity_events` and `check_selector`, used
  *instead of* `selector`. Passing both is an error. `selector` stays the primary, documented path.
- **Rejected, not approximated:** anything JMS can't express exactly (functions, `CASE`, `ILIKE`,
  subqueries, joins, aggregates) returns a tool error naming the construct and the nearest supported
  form. The translator never silently drops a condition.
- **The thin normalizer stays** (`SelectorNormalizer`: `!=` → `<>`, quoted identifiers unquoted) for
  the `selector` argument. It's the cheap path; the SQL translator is the full one.
- **Parser: JSqlParser** (Apache-2.0/LGPL dual licensed, a small jar, a plain AST for
  `PlainSelect`/`Expression`). Apache Calcite is overkill for one clause. Pin the version once the
  offline Maven build (`mvn -o`) can resolve it.

## Mapping

| SQL | Becomes | Notes |
|---|---|---|
| `FROM t` | `topic` | Short or full name, checked by `Topics.resolve` (the allowlist). Optional; defaults as today. |
| `WHERE <expr>` | `selector` | Printed from the AST with a whitelist of node types (below). |
| `LIMIT n` | `max_events` | `n ≥ 1`. |
| `ORDER BY <time> DESC` | (default) | The tools return the newest events, listed oldest first. Any other `ORDER BY` is rejected. |
| `SELECT *` / `SELECT a, b` | (projection) | Phase 3: trim each returned event to the named fields to save agent context. Until then `SELECT` columns are accepted and ignored, with a note in the result. |
| `<time> > now() - INTERVAL 'n' UNIT` (top-level `AND` term) | `lookback` | Only on the time column (`__publish_time__`, or an alias like `ts` / `time`). Removed from the selector. Units: SECOND, MINUTE, HOUR, DAY. Over `MAX_LOOKBACK` → the usual rejection. |
| `<time> BETWEEN a AND b` with literal timestamps | (rejected, v1) | A closed window needs `TimeWindow.between`; the tools only do "last N". Revisit with an absolute-window argument. |

**`WHERE` node whitelist → JMS:** `=`, `<>`/`!=`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, parentheses,
`IN (…)` / `NOT IN (…)` of literals, `LIKE` / `NOT LIKE` with optional `ESCAPE`, `BETWEEN` / `NOT BETWEEN`,
`IS [NOT] NULL`, `+ - * /`, numeric and single-quoted string literals, `TRUE` / `FALSE`, and column
references (unquoted, double-quoted, or backtick-quoted; dotted for nested body fields).

**Rejected with a reason:** function calls (`LOWER`, `UPPER`, `CAST`, `COALESCE`, `SUBSTRING`, date
functions other than the lookback pattern), `||`, `CASE`, `ILIKE` / `SIMILAR TO` / regex operators,
subqueries and `EXISTS`, joins, `GROUP BY` / `HAVING` / aggregates, `DISTINCT`, `UNION` and other set
operations, `OFFSET`, parameters (`?`, `:name`), and multiple statements. Case-insensitive matching is
the one agents will ask for. The error says it isn't supported and suggests `IN` over the known
spellings from `get_schema`'s examples.

**Typing:** headers are always strings (README, "Important constraints" #1). When `get_schema` says a
field is a header and the SQL compares it to a number (`seq > 5`), translate as written but attach a
warning (the same `selector_warnings` the dry run produces). Don't rewrite to a string compare: `'10' > '9'`
is false, so a silent rewrite would change the answer.

## Architecture

```
mcp/entity-events/src/main/java/io/streamnative/filter/mcp/
  SqlTranslator.java        <-- SQL text → Translation(topic?, selector, lookback?, maxEvents?, notes, warnings)
  SqlTranslation.java       <-- the record above (or nested in SqlTranslator)
  EntityQuery.java          <-- parse(): accept "sql"; apply Translation, then today's checks
  SelectorCheckTool.java    <-- check_selector: accept "sql"; show the translated selector and lookback
  EntityEventsTool.java, EntityWatchTool.java  <-- tool schemas gain "sql"; descriptions explain it
mcp/entity-events/src/test/java/io/streamnative/filter/mcp/
  SqlTranslatorTests.java   <-- table-driven: SQL → expected selector/args, and rejections
```

Flow in `EntityQuery.parse`: if `sql` is present, `SqlTranslator.translate(sql, topics, fieldTypes)`.
Its `topic`/`lookback`/`max_events` fill in only arguments the call didn't also pass explicitly; a
conflict (e.g. `LIMIT 50` and `max_events: 10`) is an error. The resulting selector then goes through
the same length/syntax/field checks as a hand-written one. The result echoes `sql`, the generated
`selector`, and any `lookback` taken from the SQL, so the agent can see exactly what ran.

## Phases

### Phase 1 — `WHERE` only (≈1 day)
- [ ] Add the JSqlParser dependency to `mcp/entity-events/pom.xml`; confirm the shaded jar still builds
      and `mvn -o` resolves it.
- [ ] `SqlTranslator`: accept a bare `WHERE`-clause expression, or `SELECT * FROM t WHERE …`; walk the
      `Expression` AST with the whitelist; print JMS (`!=` → `<>`, identifiers unquoted, strings
      re-quoted with `''` escaping).
- [ ] Reject everything off the whitelist with the construct's name and a suggestion.
- [ ] `sql` argument on the event tools and `check_selector`; mutual exclusion with `selector`.
- [ ] Tests: ~40 table rows (translations plus rejections), plus a round-trip check that every emitted
      selector passes `Selector.compile` and references only the columns in the SQL.

### Phase 2 — the statement around the `WHERE` (≈1 day)
- [ ] `FROM` → `topic` through `Topics.resolve`.
- [ ] `LIMIT` → `max_events`; `ORDER BY <time> DESC` accepted, other orderings rejected.
- [ ] Time predicate → `lookback`: only as a top-level `AND` term on the time column; units → duration;
      reject the same term nested under `OR`/`NOT` (it can't become a window).
- [ ] Conflicts with explicit arguments are errors; results echo `sql`, `selector` and the derived arguments.
- [ ] Header-versus-number warnings, using `FieldCatalog` types.

### Phase 3 — projection and polish (≈0.5–1 day)
- [ ] `SELECT a, b`: trim each returned event to those fields (key and time always kept). This applies to
      the result only; the filter still reads whole events.
- [ ] README: a "SQL" section with the mapping table and rejection list; update the tool descriptions
      and server instructions ("selector is a SQL WHERE clause; or pass full SQL as sql").
- [ ] `check_selector` shows the SQL → selector translation and any notes.

**Total: about 2.5–3 days**, plus review.

## Testing

- Table-driven unit tests per phase (SQL in → selector and args out; SQL in → error text).
- A differential check reusing the oracle approach in `common` (`SelectorOracleTest`): for each
  translated selector, evaluate it and an equivalent hand-written selector against the same generated
  messages, and require identical results.
- An agent-in-the-loop smoke test with the window demo (`make window-demo`): the Demo 2 prompt, with the
  agent free to use `sql`. Confirm it reaches `order_confirmed` with a correct diagnosis.

## Risks and open questions

- **Dialect drift:** agents will write Postgres-isms (`ILIKE`, `::text`, `now() - interval '15 minutes'`).
  Decide which time-predicate spellings to accept beyond `INTERVAL 'n' UNIT`; start narrow and log the
  rejections from real use.
- **Time column name:** RTCE-style SQL will say `ts` or `timestamp`. Decide the accepted aliases, and
  whether a body field with that name should win (the filter's resolution order says it would).
- **JSqlParser and the offline build:** the dependency must be in the local repository or the mirror the
  team builds against.
- **Is `sql` worth it on stage?** The demo story is "RTCE-shaped tools". Showing an agent reuse the same
  `WHERE` fragment against both is a strong beat. If time is short, Phase 1 alone delivers it.
