# Python client SDK — backlog plan

## Context

Today the only SDK that speaks the `FilterRequest` / orchestrator wire format
is the Java `sdk/java/pulsar` module. The wire is JSON (see
`protocol/filter-request.schema.json`) — language-agnostic by design — but no
Python implementation exists. Python is the highest-priority second language
because:

- Data-science consumers tend to be Python-first.
- The Pulsar Python client (`pulsar-client`) is well-supported by Apache.
- The repo already restructured for multi-language SDKs (`sdk/java/…` under
  the assumption a `sdk/python/…` peer would land later).

This is a **backlog plan**. Not started. Surfacing the scope, locked
decisions, and what to verify before kicking off.

## Goals

1. Functional parity with the Java Pulsar SDK's public API
   (`Session.newConsumer().topic(…).selector(…).subscribe()` returning a
   filtered `Consumer` plus a `FilterControl`).
2. Idiomatic Python — context managers, type hints, asyncio support.
3. Same wire format (JSON `FilterRequest`) — no Python-specific dialect.
4. Same heartbeat semantics as the Java SDK (Phase 4 in
   `tasks/todo-sdk-heartbeat.md`).

## Open design questions (to resolve at kickoff, not now)

- **Sync vs async API.** Pulsar Python supports both. Likely answer: provide
  both, with the sync API as a thin wrapper around the async one. Validate
  with one or two prospective consumers first.
- **Schema-detection equivalent of `keyDeserializer` / `valueDeserializer`.**
  Java sends FQN class names; Python has no direct equivalent. Options:
  (a) accept any string and pass it through (operator-supplied marker);
  (b) use Pulsar Python's `Schema` class name or a stable identifier;
  (c) skip the schema-detection feature entirely on the Python side and
  rely on the orchestrator's null/match/upgrade tolerance. Cleanest is (a) —
  it lets Python consumers interoperate with Java consumers that DID declare
  schemas, and stays silent when they didn't.
- **Heartbeat scheduling primitive.** Threading vs asyncio task vs
  background coroutine. Depends on the sync-vs-async decision.

## API sketch (subject to kickoff review)

```python
from selector_filter import Session, PrivateTopicPolicy

with Session.builder(
        service_url="pulsar://broker:6650",
        admin_url="http://broker:8080",
        control_topic="persistent://public/filters/filter-requests",
        private_topic_policy=PrivateTopicPolicy(
            message_ttl_seconds=60,
            inactive_delete_seconds=60),
    ).build() as session:

    consumer, control = session.new_consumer() \
        .topic("persistent://public/in/trades") \
        .selector("region = 'US'") \
        .subscribe()

    for msg in consumer:
        process(msg.data())
        consumer.acknowledge(msg)

    control.set_selector("region IN ('US','UK')")  # MODIFY
    # consumer.__exit__ → session.retire_private_query → UNREGISTER
```

## Module shape

```
sdk/python/                                       -- new module
  pyproject.toml                                  -- hatch/poetry; package = "selector_filter"
  README.md
  src/selector_filter/
    __init__.py
    _protocol.py                                  -- FilterRequest serde (mirrors common/FilterRequest.java)
    _commands.py                                  -- Command enum, ClientType enum
    session.py                                    -- Session, SessionBuilder
    consumer.py                                   -- FilterableConsumer, FilterControl
    private_topic.py                              -- PrivateTopicPolicy, output-topic creation/teardown
    selectors.py                                  -- client-side selector syntax validation (or no-op)
    heartbeat.py                                  -- daemon scheduler
  tests/
    test_session.py
    test_consumer.py
    test_filter_request_serde.py                  -- parity with FilterRequestTests.java
    test_heartbeat.py
    integration/
      test_orchestrator_e2e.py                    -- testcontainers-python Pulsar IT
```

## Phases

| # | Phase | Outcome |
|---|---|---|
| 1 | Wire format | `_protocol.py` + `_commands.py`. Round-trip tests against the same JSON examples in `protocol/filter-request.schema.json`. |
| 2 | Session + producer | Session opens a Pulsar producer on the control topic. `register_private_query` / `update_private_query` / `retire_private_query` publish FilterRequests. Mocked unit tests. |
| 3 | FilterableConsumer + FilterControl | Builder, `subscribe()` returning a wrapped Pulsar consumer + control object. Mocked unit tests. |
| 4 | Heartbeat scheduler | Mirror the Java Session-level heartbeat: one daemon task per Session, sends one HEARTBEAT every 30s with all live queryIds. Opt-out via `Session.builder(..., disable_heartbeat=True)`. |
| 5 | Live IT against orchestrator | testcontainers-python spins up apachepulsar/pulsar:4.0.10 with the functions worker, uploads the filter NAR, deploys the orchestrator, drives the Python SDK end-to-end. Mirrors `OrchestratorFunctionIT` shape. |
| 6 | Packaging + CI | pyproject.toml metadata, ruff + mypy + pytest, wheel build, README. PyPI publication can be a separate decision. |

## Cross-cutting concerns

- **CI integration.** Maven reactor already runs Java + Kafka SDK tests. Python
  needs a separate runner (likely a top-level `make python-test` or a GitHub
  Actions job). Keep Maven out of the Python critical path.
- **Heartbeat alignment.** When the Java side ships heartbeat
  (`tasks/todo-sdk-heartbeat.md`), the Python SDK must follow the same wire
  format. Currently null since heartbeat is still mid-implementation.
- **Output-topic creation.** The Java SDK uses `PulsarAdmin` to set
  per-private-topic policies (TTL, retention, inactive-delete). The Python
  client's admin support is more limited; may need to fall back to namespace-
  level defaults or a small helper that talks to the broker REST API directly.
  Decide at Phase 3.
- **Logging.** Use `logging` module with a `selector_filter` logger root.
  Default level WARNING.
- **Type hints.** Strict mypy compliance. The wire schemas are stable enough
  that we can autogenerate dataclasses or `TypedDict`s if desired.

## Verification

- Round-trip serde tests use the same JSON fixtures the Java
  `FilterRequestTests` does — guarantees the two SDKs cannot diverge silently.
- The Python IT subscribes to a filter, drives input via a Java producer (or a
  vanilla Python producer), and asserts filtered output. Cross-SDK
  interoperability is implicit because both speak the same JSON wire.

## Risks

- **R1: Pulsar Python admin gaps.** `pulsar-client` (Python) does NOT bundle
  the full admin API surface that PulsarAdmin (Java) offers. Per-topic policy
  setting may need raw HTTP via `requests`. Mitigation: thin internal HTTP
  client for admin calls, mirroring the few endpoints we need.
- **R2: GIL + heartbeat thread.** A pure-threading heartbeat works fine for
  modest scale; asyncio integration requires more care. Mitigation: ship
  threading version first, asyncio as a follow-up.
- **R3: Maintaining wire parity.** Each new `FilterRequest` field added by
  the Java side has to land in `_protocol.py` too. Mitigation: a shared
  contract test that hits `protocol/filter-request.schema.json` directly and
  validates both Java and Python output against it.
