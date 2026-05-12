"""Heartbeat scheduling tests.

Mirrors the Heartbeat section of Java ``SessionTests``. Heartbeat interval is set to an
implausibly large value so the daemon scheduler does not fire during the test; tests then
invoke ``_send_heartbeat_quietly()`` directly to drive the publish path deterministically.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock

import pytest

from selector_filter import Command, Session


@pytest.fixture
def mock_pulsar_client() -> MagicMock:
    client = MagicMock(name="pulsar.Client")
    producer = MagicMock(name="pulsar.Producer")
    client.create_producer.return_value = producer
    return client


@pytest.fixture
def session_hb_enabled(mock_pulsar_client: MagicMock) -> Session:
    """Session with heartbeats enabled at a 1-hour interval — scheduler will not fire during
    the test; we drive ``_send_heartbeat_quietly`` directly.
    """
    s = (
        Session.builder()
        .pulsar_client(mock_pulsar_client)
        .control_topic("persistent://public/orchestrator/filter-requests")
        .heartbeat_interval_seconds(3600)
        .build()
    )
    yield s
    s.close()


def _last_send(session: Session) -> dict:
    producer = session._control_producer  # noqa: SLF001
    last = producer.send.call_args_list[-1]
    payload = last.kwargs.get("content") or last.args[0]
    return json.loads(payload)


def _last_key(session: Session) -> str:
    producer = session._control_producer  # noqa: SLF001
    last = producer.send.call_args_list[-1]
    return last.kwargs.get("partition_key")


# ---------------------------------------------------------------- enabled path

class TestHeartbeatEnabled:
    def test_publishes_all_live_queryids(self, session_hb_enabled: Session) -> None:
        pq1 = session_hb_enabled.register_private_query(["persistent://t/n/a"], "x = 1")
        pq2 = session_hb_enabled.register_private_query(["persistent://t/n/b"], "x = 2")

        session_hb_enabled._send_heartbeat_quietly()  # noqa: SLF001

        producer = session_hb_enabled._control_producer  # noqa: SLF001
        assert producer.send.call_count == 3  # 2 REGISTER + 1 HEARTBEAT
        hb = _last_send(session_hb_enabled)
        assert hb["command"] == Command.HEARTBEAT.value
        assert set(hb["queryIds"]) == {pq1.query_id, pq2.query_id}

    def test_heartbeat_key_starts_with_session_prefix(self, session_hb_enabled: Session) -> None:
        session_hb_enabled.register_private_query(["persistent://t/n/a"], "x = 1")
        session_hb_enabled._send_heartbeat_quietly()  # noqa: SLF001
        assert _last_key(session_hb_enabled).startswith("heartbeat-")

    def test_empty_live_set_publishes_nothing(self, session_hb_enabled: Session) -> None:
        session_hb_enabled._send_heartbeat_quietly()  # noqa: SLF001
        producer = session_hb_enabled._control_producer  # noqa: SLF001
        producer.send.assert_not_called()

    def test_retire_removes_queryid_from_heartbeat(self, session_hb_enabled: Session) -> None:
        pq1 = session_hb_enabled.register_private_query(["persistent://t/n/a"], "x = 1")
        pq2 = session_hb_enabled.register_private_query(["persistent://t/n/b"], "x = 2")
        session_hb_enabled.retire_private_query(pq1)
        session_hb_enabled._send_heartbeat_quietly()  # noqa: SLF001
        hb = _last_send(session_hb_enabled)
        assert hb["command"] == Command.HEARTBEAT.value
        assert hb["queryIds"] == [pq2.query_id]

    def test_publish_failure_does_not_propagate(self, session_hb_enabled: Session) -> None:
        session_hb_enabled.register_private_query(["persistent://t/n/a"], "x = 1")
        producer = session_hb_enabled._control_producer  # noqa: SLF001
        # Make every send AFTER the REGISTER fail. _send_heartbeat_quietly must swallow.
        producer.send.side_effect = RuntimeError("broker down")
        session_hb_enabled._send_heartbeat_quietly()  # noqa: SLF001 — no raise


# ---------------------------------------------------------------- disabled path

class TestHeartbeatDisabled:
    def test_disable_heartbeat_skips_scheduler(self, mock_pulsar_client: MagicMock) -> None:
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .disable_heartbeat()
            .build()
        )
        assert s._heartbeat_thread is None  # noqa: SLF001
        s.register_private_query(["persistent://t/n/a"], "x = 1")
        s.close()
        # Only the REGISTER published — no HEARTBEAT, no scheduler ran.
        producer = s._control_producer  # noqa: SLF001
        assert producer.send.call_count == 1
        req = _last_send(s)
        assert req["command"] == Command.REGISTER.value

    def test_zero_interval_disables_scheduler(self, mock_pulsar_client: MagicMock) -> None:
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .heartbeat_interval_seconds(0)
            .build()
        )
        assert s._heartbeat_thread is None  # noqa: SLF001
        s.close()

    def test_negative_interval_clamps_to_zero(self, mock_pulsar_client: MagicMock) -> None:
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .heartbeat_interval_seconds(-5)
            .build()
        )
        assert s._heartbeat_thread is None  # noqa: SLF001
        s.close()


# ---------------------------------------------------------------- lifecycle

class TestHeartbeatLifecycle:
    def test_close_stops_thread(self, mock_pulsar_client: MagicMock) -> None:
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .heartbeat_interval_seconds(3600)
            .build()
        )
        thread = s._heartbeat_thread  # noqa: SLF001
        assert thread is not None
        assert thread.is_alive()
        s.close()
        # Daemon thread should exit within a few seconds — close() joins with timeout=5.
        thread.join(timeout=2.0)
        assert not thread.is_alive()

    def test_thread_is_daemon(self, mock_pulsar_client: MagicMock) -> None:
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .heartbeat_interval_seconds(3600)
            .build()
        )
        assert s._heartbeat_thread.daemon is True  # noqa: SLF001
        s.close()


# ---------------------------------------------------------------- short-interval smoke

class TestHeartbeatShortInterval:
    def test_scheduler_fires_at_short_interval(self, mock_pulsar_client: MagicMock) -> None:
        """Real-clock smoke test: at 100ms interval, the scheduler fires at least once within
        ~500ms wall-clock.
        """
        s = (
            Session.builder()
            .pulsar_client(mock_pulsar_client)
            .control_topic("persistent://t/n/x")
            .heartbeat_interval_seconds(0.1)
            .build()
        )
        try:
            s.register_private_query(["persistent://t/n/a"], "x = 1")
            # Wait long enough that at least ONE heartbeat must have fired.
            time.sleep(0.5)
            producer = s._control_producer  # noqa: SLF001
            # >= 2 calls: 1 REGISTER + >=1 HEARTBEAT
            assert producer.send.call_count >= 2
            # The most-recent send must be a HEARTBEAT.
            hb = _last_send(s)
            assert hb["command"] == Command.HEARTBEAT.value
        finally:
            s.close()
