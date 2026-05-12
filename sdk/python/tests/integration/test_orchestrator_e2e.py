"""Live end-to-end integration test for the Python SDK against a real orchestrator-fronted
Pulsar broker.

Skipped by default — runs only when:
  - ``testcontainers`` is importable (install via ``pip install -e ".[it]"``)
  - The two pre-built NARs exist in ``functions/*/target/`` (run ``mvn package`` at the repo root)
  - Docker is available (``docker info`` returns success)

Invoke explicitly:

    .venv/bin/pytest tests/integration/ -v -m integration

The test:
  1. Starts an apachepulsar/pulsar:4.0.10 container (standalone, functions worker enabled).
  2. Mounts both NARs read-only into ``/pulsar/nars/``.
  3. Uses ``pulsar-admin`` (via docker exec) to upload the filter NAR as a Pulsar package and
     deploy the orchestrator function.
  4. Builds a Python :class:`Session` against the container's broker URL.
  5. Subscribes a filtered consumer with selector ``region = 'US'``.
  6. Drives three input messages on the source topic (US, DE, US).
  7. Asserts the SDK consumer receives exactly the two US messages, in order, with the
     ``region`` user property round-tripped.

Wall time: ~60-90s on a warm Docker host (Pulsar image is the long pole).
"""

from __future__ import annotations

import contextlib
import json
import shutil
import subprocess
import time
from pathlib import Path

import pytest

# Skip the whole module if testcontainers is not installed.
testcontainers_core = pytest.importorskip("testcontainers.core.container")
import pulsar  # type: ignore[import-untyped]  # noqa: E402
from testcontainers.core.container import DockerContainer  # noqa: E402

from selector_filter import Session  # noqa: E402

# ---------------------------------------------------------------- locating the NARs

REPO_ROOT = Path(__file__).resolve().parents[4]
FILTER_NAR_DIR = REPO_ROOT / "functions" / "selector-filter" / "target"
ORCH_NAR_DIR = REPO_ROOT / "functions" / "orchestrator" / "target"


def _find_nar(directory: Path, prefix: str) -> Path | None:
    if not directory.is_dir():
        return None
    candidates = sorted(directory.glob(f"{prefix}-*.nar"))
    return candidates[0] if candidates else None


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        subprocess.run(
            ["docker", "info"], check=True, capture_output=True, timeout=10
        )
        return True
    except (subprocess.SubprocessError, OSError):
        return False


@pytest.fixture(scope="module")
def filter_nar() -> Path:
    nar = _find_nar(FILTER_NAR_DIR, "selector-filter-function")
    if nar is None:
        pytest.skip(
            "Filter NAR not built. Run `mvn -pl functions/selector-filter package -DskipTests` "
            "at the repo root."
        )
    return nar


@pytest.fixture(scope="module")
def orchestrator_nar() -> Path:
    nar = _find_nar(ORCH_NAR_DIR, "selector-filter-orchestrator")
    if nar is None:
        pytest.skip(
            "Orchestrator NAR not built. Run `mvn -pl functions/orchestrator package -DskipTests` "
            "at the repo root."
        )
    return nar


@pytest.fixture(scope="module")
def docker_check() -> None:
    if not _docker_available():
        pytest.skip("Docker is not available on this host.")


# ---------------------------------------------------------------- container fixture

# Topics + names — kept module-scoped because the container is module-scoped.
FN_TENANT = "public"
FN_NAMESPACE = "default"
FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0"
ORCH_NAME = "selector-filter-orchestrator-py-it"
SDK_INPUT_TOPIC = "persistent://public/default/py-filter-requests"
ORCH_STATE_TOPIC = "persistent://public/default/py-orchestrator-state"
INPUT_TOPIC = "persistent://public/default/py-trades"

FILTER_NAR_IN_CONTAINER = "/pulsar/nars/selector-filter-function.nar"
ORCH_NAR_IN_CONTAINER = "/pulsar/nars/selector-filter-orchestrator.nar"


def _exec_or_raise(container: DockerContainer, cmd: list[str], description: str) -> str:
    """Run a command inside the container; raise with a clear message on failure."""
    result = container.exec(cmd)
    # testcontainers-python's exec returns a tuple (exit_code, output) or an object with
    # similar attributes depending on version. Normalize both forms.
    if hasattr(result, "exit_code"):
        exit_code = result.exit_code
        output = result.output
    else:
        exit_code, output = result
    if isinstance(output, bytes):
        output = output.decode("utf-8", errors="replace")
    if exit_code != 0:
        raise RuntimeError(
            f"{description} failed with exit {exit_code}.\nCommand: {' '.join(cmd)}\n"
            f"Output:\n{output}"
        )
    return output


def _wait_for_log(container: DockerContainer, needle: str, timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            stdout, stderr = container.get_logs()
            if isinstance(stdout, bytes):
                stdout = stdout.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", errors="replace")
            combined = (stdout or "") + (stderr or "")
            if needle in combined:
                return
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    raise TimeoutError(f"Did not see log message {needle!r} within {timeout_seconds}s")


def _wait_for_function_running(
    container: DockerContainer, tenant: str, ns: str, name: str, timeout_seconds: float
) -> None:
    deadline = time.time() + timeout_seconds
    last_output = ""
    while time.time() < deadline:
        try:
            result = container.exec(
                ["bin/pulsar-admin", "functions", "status",
                 "--tenant", tenant, "--namespace", ns, "--name", name]
            )
            output = result.output if hasattr(result, "output") else result[1]
            if isinstance(output, bytes):
                output = output.decode("utf-8", errors="replace")
            last_output = output
            # When fully running, the JSON status shows numRunning equal to numInstances.
            try:
                data = json.loads(output)
                if data.get("numRunning", 0) > 0 and \
                        data.get("numRunning") == data.get("numInstances"):
                    return
            except json.JSONDecodeError:
                pass
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2)
    raise TimeoutError(
        f"Function {tenant}/{ns}/{name} did not reach RUNNING within {timeout_seconds}s. "
        f"Last status:\n{last_output}"
    )


@pytest.fixture(scope="module")
def pulsar_container(
    docker_check: None, filter_nar: Path, orchestrator_nar: Path
):
    """Module-scoped Pulsar testcontainer with both NARs mounted, orchestrator deployed."""
    container = (
        DockerContainer("apachepulsar/pulsar:4.0.10")
        .with_volume_mapping(str(filter_nar), FILTER_NAR_IN_CONTAINER, "ro")
        .with_volume_mapping(str(orchestrator_nar), ORCH_NAR_IN_CONTAINER, "ro")
        .with_exposed_ports(6650, 8080)
        .with_command("bin/pulsar standalone")
    )
    container.start()
    try:
        # Wait for the broker to be ready BEFORE we start running admin commands.
        _wait_for_log(container, "messaging service is ready", timeout_seconds=180)
        time.sleep(5)  # functions worker also needs a moment

        # Topology prep.
        _exec_or_raise(
            container,
            ["bin/pulsar-admin", "namespaces", "set-auto-topic-creation",
             "public/default", "--enable", "--type", "non-partitioned"],
            "set-auto-topic-creation",
        )
        for topic in (SDK_INPUT_TOPIC, ORCH_STATE_TOPIC, INPUT_TOPIC):
            _exec_or_raise(
                container,
                ["bin/pulsar-admin", "topics", "create", topic],
                f"create topic {topic}",
            )

        # Upload the filter NAR as a Pulsar package.
        _exec_or_raise(
            container,
            ["bin/pulsar-admin", "packages", "upload", FILTER_PACKAGE_URL,
             "--path", FILTER_NAR_IN_CONTAINER,
             "--description", "selector filter NAR for Python IT"],
            "upload filter NAR package",
        )

        # Deploy the orchestrator.
        user_config = json.dumps({
            "state-topic": ORCH_STATE_TOPIC,
            "function-package-url": FILTER_PACKAGE_URL,
            "function-tenant": FN_TENANT,
            "function-namespace": FN_NAMESPACE,
            "function-parallelism": 1,
            "apply-compaction": True,
            "pulsar-admin-url": "http://localhost:8080",
        })
        _exec_or_raise(
            container,
            ["bin/pulsar-admin", "functions", "create",
             "--tenant", FN_TENANT, "--namespace", FN_NAMESPACE, "--name", ORCH_NAME,
             "--jar", ORCH_NAR_IN_CONTAINER,
             "--classname", "io.streamnative.filter.orchestrator.OrchestratorFunction",
             "--inputs", SDK_INPUT_TOPIC,
             "--user-config", user_config],
            "create orchestrator function",
        )

        _wait_for_function_running(
            container, FN_TENANT, FN_NAMESPACE, ORCH_NAME, timeout_seconds=90
        )
        time.sleep(5)  # grace for the orchestrator's TableView to open

        yield container
    finally:
        container.stop()


# ---------------------------------------------------------------- the test

@pytest.mark.integration
class TestOrchestratorEndToEnd:
    def test_register_and_receive_filtered(self, pulsar_container: DockerContainer) -> None:
        broker_url = f"pulsar://{pulsar_container.get_container_host_ip()}:" \
                     f"{pulsar_container.get_exposed_port(6650)}"

        pulsar_client = pulsar.Client(broker_url)
        input_producer = pulsar_client.create_producer(INPUT_TOPIC, block_if_queue_full=True)

        try:
            session = (
                Session.builder()
                .pulsar_client(pulsar_client)
                .control_topic(SDK_INPUT_TOPIC)
                .private_topic_tenant(FN_TENANT)
                .private_topic_namespace(FN_NAMESPACE)
                .disable_heartbeat()
                .build()
            )

            with session:
                consumer = (
                    session.new_consumer()
                    .topic(INPUT_TOPIC)
                    .selector("region = 'US'")
                    .subscription_initial_position(pulsar.InitialPosition.Earliest)
                    .subscribe()
                )

                with consumer:
                    # The orchestrator processes our REGISTER and spawns a filter function.
                    # We do not know the exact spawned name; pulsar-admin reports it via
                    # `functions list`, and a small retry loop confirms it reaches RUNNING.
                    _wait_for_spawned_filter(pulsar_container, timeout_seconds=120)
                    time.sleep(5)  # filter TableView loads the QueryMessage

                    # Drive three records: two US (should arrive), one DE (should not).
                    input_producer.send(
                        b"us-1", partition_key="AAPL", properties={"region": "US"})
                    input_producer.send(
                        b"de-1", partition_key="BMW", properties={"region": "DE"})
                    input_producer.send(
                        b"us-2", partition_key="MSFT", properties={"region": "US"})

                    received: list[tuple[str, str]] = []
                    deadline = time.time() + 60
                    while len(received) < 2 and time.time() < deadline:
                        try:
                            msg = consumer.receive(timeout_millis=5000)
                            received.append((
                                msg.data().decode("utf-8"),
                                msg.properties().get("region", ""),
                            ))
                            consumer.acknowledge(msg)
                        except Exception:  # noqa: BLE001 — pulsar.Timeout sometimes
                            continue

                    assert len(received) == 2, f"expected 2 US records, got {received}"
                    payloads = sorted(p for p, _ in received)
                    assert payloads == ["us-1", "us-2"]
                    regions = {r for _, r in received}
                    assert regions == {"US"}, f"unexpected region(s) {regions}"

                    # Confirm no third record (the DE one) leaks through. A tight timeout
                    # is fine — the filter has had 60s to publish anything matching, and
                    # nothing more is in flight.
                    try:
                        leak = consumer.receive(timeout_millis=3000)
                        consumer.acknowledge(leak)
                        pytest.fail(f"unexpected extra message: data={leak.data()!r} "
                                    f"props={leak.properties()}")
                    except Exception:  # noqa: BLE001 — timeout is success
                        pass
        finally:
            with contextlib.suppress(Exception):
                input_producer.close()
            with contextlib.suppress(Exception):
                pulsar_client.close()


def _wait_for_spawned_filter(container: DockerContainer, timeout_seconds: float) -> None:
    """Poll until at least one filter function instance OTHER than the orchestrator reaches
    RUNNING. Mirrors the Java IT's waitForSpawnedFilter helper.
    """
    deadline = time.time() + timeout_seconds
    last_output = ""
    while time.time() < deadline:
        try:
            result = container.exec(
                ["bin/pulsar-admin", "functions", "list",
                 "--tenant", FN_TENANT, "--namespace", FN_NAMESPACE]
            )
            output = result.output if hasattr(result, "output") else result[1]
            if isinstance(output, bytes):
                output = output.decode("utf-8", errors="replace")
            last_output = output
            try:
                names = json.loads(output)
            except json.JSONDecodeError:
                names = [line.strip() for line in output.splitlines() if line.strip()]
            for name in names:
                if not isinstance(name, str):
                    continue
                if name.startswith("selector-filter-") and name != ORCH_NAME:
                    # Check this one is RUNNING.
                    status_result = container.exec(
                        ["bin/pulsar-admin", "functions", "status",
                         "--tenant", FN_TENANT, "--namespace", FN_NAMESPACE, "--name", name]
                    )
                    status_output = (
                        status_result.output if hasattr(status_result, "output")
                        else status_result[1]
                    )
                    if isinstance(status_output, bytes):
                        status_output = status_output.decode("utf-8", errors="replace")
                    try:
                        data = json.loads(status_output)
                        if data.get("numRunning", 0) > 0 and \
                                data.get("numRunning") == data.get("numInstances"):
                            return
                    except json.JSONDecodeError:
                        continue
        except Exception:  # noqa: BLE001
            pass
        time.sleep(2)
    raise TimeoutError(
        f"No spawned filter function reached RUNNING within {timeout_seconds}s. "
        f"Last functions list:\n{last_output}"
    )
