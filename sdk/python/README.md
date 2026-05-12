# selector-filter — Python SDK (alpha)

Python client SDK for the broker-side selector filter system on Apache Pulsar. Mirrors the
public API of the Java SDK at `sdk/java/pulsar/`. See the [top-level
README](../../README.md) for the architecture overview.

> **Status: Phase 1.** Only the wire-format types are implemented today
> (`FilterRequest`, `Command`, `ClientType`). `Session`, `FilterableConsumer`, `FilterControl`,
> and the heartbeat scheduler land in subsequent phases. Track progress in
> `tasks/todo-python-sdk.md` at the repo root.

## Install

### From source (today)

```bash
pip install "git+https://github.com/david-streamlio/broker-side-filtering-function.git@main#subdirectory=sdk/python"
```

### From a GitHub release wheel (Phase 6+)

```bash
pip install https://github.com/.../releases/download/vX.Y.Z/selector_filter-X.Y.Z-py3-none-any.whl
```

### From PyPI (once published)

```bash
pip install pulsar-selector-filter
```

## Supported Python versions

3.9+. The Pulsar Python client itself supports 3.8+, but the broader ecosystem (mypy, pandas,
numpy) has dropped 3.8.

## Development

```bash
cd sdk/python
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```
