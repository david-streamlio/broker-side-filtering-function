# pulsar-selector-filter — Python SDK

Python client for the broker-side selector filter system on Apache Pulsar. Mirrors the public
API of the Java SDK at `sdk/java/pulsar/` and speaks the same JSON wire format. See the
[top-level README](../../README.md) for the architecture overview.

> **Status: alpha (0.1.x).** The full SDK surface is implemented and tested end-to-end against
> the Java orchestrator. The API is stable in shape but field-level details may shift before
> 1.0. Track progress in [`tasks/todo-python-sdk.md`](../../tasks/todo-python-sdk.md).

## Install

### From source (today)

```bash
pip install "git+https://github.com/david-streamlio/broker-side-filtering-function.git@main#subdirectory=sdk/python"
```

### From a GitHub release wheel (once published)

```bash
pip install https://github.com/.../releases/download/vX.Y.Z/pulsar_selector_filter-X.Y.Z-py3-none-any.whl
```

### From PyPI (once published)

```bash
pip install pulsar-selector-filter
```

## Supported Python versions

**3.9+**. The Pulsar Python client itself supports 3.8+, but the broader ecosystem (mypy,
pandas, numpy) has dropped 3.8. Tested in CI on 3.9, 3.10, 3.11, 3.12, 3.13.

## Quickstart

```python
import pulsar
from selector_filter import Session

client = pulsar.Client("pulsar://broker:6650")

with Session.builder() \
        .pulsar_client(client) \
        .control_topic("persistent://public/filters/filter-requests") \
        .build() as session:

    with session.new_consumer() \
            .topic("persistent://public/in/trades") \
            .selector("region = 'US'") \
            .subscribe() as consumer:

        msg = consumer.receive(timeout_millis=5000)
        print(msg.data(), msg.properties())
        consumer.acknowledge(msg)

        # Hot-reload the selector at runtime — same queryId.
        consumer.set_selector("region IN ('US','UK')")

client.close()
```

The Session publishes one `HEARTBEAT` message every 30 seconds covering every live queryId it
owns; the orchestrator uses these to detect SDK consumers that died without calling `close()`.
Opt out via `.disable_heartbeat()` if you have a controlled shutdown story or do not want the
chatter.

## Public surface

| Class / fn | Purpose |
|---|---|
| `Session` / `SessionBuilder` | Entry point; owns the producer on the orchestrator control topic and the daemon heartbeat scheduler |
| `FilteringConsumer` | Wraps a Pulsar consumer on the SDK-generated private output topic. Methods: `receive`, `acknowledge`, `negative_acknowledge`, iteration; control: `query_id`, `current_selector`, `set_selector` |
| `ConsumerBuilder` | Fluent builder returned by `Session.new_consumer()` |
| `PrivateQuery` | Handle returned by `Session.register_private_query` |
| `PrivateTopicPolicy` | Per-query private-output-topic settings (currently informational; see Limits) |
| `FilterRequest` / `Command` / `ClientType` | Wire-format types (mostly internal; exported for advanced users) |

## Limits

- **Per-topic policies are not applied to private topics.** The Pulsar Python client does not
  ship a `PulsarAdmin` equivalent, so the SDK does not set per-topic TTL / retention /
  inactive-delete policies. Operators must configure equivalent defaults at the namespace
  level. A follow-up may add a thin HTTP wrapper for the relevant REST endpoints.
- **Client-side JMS-selector syntax validation is a no-op.** The Java SDK uses the
  `pulsar-jms-filters` library to fail malformed selectors at register time. Python has no
  equivalent parser today; malformed selectors are rejected asynchronously by the orchestrator,
  so a bad selector means a quiet REGISTER followed by no records arriving on the private topic.
- **Sync-only.** The SDK is synchronous. An asyncio variant can be added in a later phase
  without changing the sync surface.

## Development

```bash
cd sdk/python
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"

# Lint + type-check
.venv/bin/ruff check src/ tests/
.venv/bin/mypy src/

# Run unit tests
.venv/bin/pytest tests/ --ignore=tests/integration

# Run the live IT (requires Docker + pre-built NARs)
.venv/bin/pip install -e ".[it]"
mvn -pl functions/selector-filter,functions/orchestrator package -DskipTests \
    -f ../../pom.xml
.venv/bin/pytest tests/integration/ -v

# Build wheel + sdist
.venv/bin/hatch build
```

## Cross-language wire parity

The Python SDK speaks the exact same JSON wire format as the Java SDK. Unit tests include a
cross-language parity suite (`tests/test_filter_request_serde.py::TestJavaWireParity`) that
round-trips the canonical examples from
[`protocol/filter-request.schema.json`](../../protocol/filter-request.schema.json) through the
Python implementation and asserts the on-wire shape matches what the Java side emits. The live
IT (`tests/integration/test_orchestrator_e2e.py`) proves end-to-end interop: Python consumer →
Java orchestrator → Java filter function → Python consumer receives filtered output.
