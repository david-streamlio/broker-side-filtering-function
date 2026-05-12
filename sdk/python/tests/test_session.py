"""Session unit tests — mocked Pulsar client.

Mirrors ``sdk/java/pulsar/src/test/java/io/streamnative/filter/client/SessionTests.java`` in
scope: register/update/retire publish the right FilterRequest shape, builder validation works,
live_query_ids is maintained correctly.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from selector_filter import (
    ClientType,
    Command,
    FilterRequest,
    PrivateTopicPolicy,
    Session,
)


# ---------------------------------------------------------------- fixtures

@pytest.fixture
def mock_pulsar_client() -> MagicMock:
    """A pulsar.Client mock returning a producer that records every send()."""
    client = MagicMock(name="pulsar.Client")
    producer = MagicMock(name="pulsar.Producer")
    client.create_producer.return_value = producer
    return client


@pytest.fixture
def session(mock_pulsar_client: MagicMock) -> Session:
    """A Session backed by a mocked Pulsar client and an explicit control topic.

    Heartbeats disabled so the daemon thread cannot contaminate verify() assertions; heartbeat
    behavior has dedicated coverage in ``test_heartbeat.py``.
    """
    return (
        Session.builder()
        .pulsar_client(mock_pulsar_client)
        .control_topic("persistent://public/orchestrator/filter-requests")
        .private_topic_tenant("public")
        .private_topic_namespace("filtered")
        .disable_heartbeat()
        .build()
    )


def _captured_sends(session: Session) -> list[dict]:
    """Pull the captured sends off the mocked producer; each call is decoded as a FilterRequest dict."""
    producer = session._control_producer  # noqa: SLF001 — test access
    sends: list[dict] = []
    for call in producer.send.call_args_list:
        payload = call.kwargs.get("content") if "content" in call.kwargs else call.args[0]
        sends.append(json.loads(payload))
    return sends


def _captured_keys(session: Session) -> list[str]:
    """Pull message keys from the mocked producer's send calls."""
    producer = session._control_producer  # noqa: SLF001
    keys: list[str] = []
    for call in producer.send.call_args_list:
        key = call.kwargs.get("partition_key")
        keys.append(key)
    return keys


# ---------------------------------------------------------------- SessionBuilder

class TestSessionBuilder:
    def test_missing_control_topic_raises(self, mock_pulsar_client: MagicMock) -> None:
        with pytest.raises(ValueError, match="control_topic"):
            Session.builder().pulsar_client(mock_pulsar_client).build()

    def test_neither_service_url_nor_client_raises(self) -> None:
        with pytest.raises(ValueError, match="service_url or pulsar_client"):
            Session.builder().control_topic("persistent://t/n/x").build()

    def test_uses_default_private_topic_tenant_namespace(self, session: Session) -> None:
        assert session.private_topic_tenant == "public"
        assert session.private_topic_namespace == "filtered"


# ---------------------------------------------------------------- producer wiring

class TestProducerWiring:
    def test_producer_is_created_on_control_topic(self, mock_pulsar_client: MagicMock,
                                                  session: Session) -> None:
        mock_pulsar_client.create_producer.assert_called_once()
        call = mock_pulsar_client.create_producer.call_args
        assert call.kwargs["topic"] == "persistent://public/orchestrator/filter-requests"
        assert call.kwargs["producer_name"].startswith("selector-filter-session-")
        # Batching off — control-plane is low-volume by design.
        assert call.kwargs.get("block_if_queue_full") is True


# ---------------------------------------------------------------- register_private_query

class TestRegisterPrivateQuery:
    def test_publishes_register_with_correct_fields(self, session: Session) -> None:
        pq = session.register_private_query(
            source_topics=["persistent://public/in/trades"],
            user_selector="region = 'US'",
        )

        sends = _captured_sends(session)
        assert len(sends) == 1
        req = sends[0]
        assert req["command"] == Command.REGISTER.value
        assert req["inputTopics"] == ["persistent://public/in/trades"]
        assert req["selector"] == "region = 'US'"
        assert req["clientType"] == ClientType.PULSAR.value
        assert req["outputTopic"].startswith("persistent://public/filtered/filtered-")
        # outputTopic ends with the queryId.
        assert req["outputTopic"].endswith(pq.query_id)

    def test_message_key_is_queryid(self, session: Session) -> None:
        pq = session.register_private_query(
            source_topics=["persistent://public/in/x"],
            user_selector=None,
        )
        keys = _captured_keys(session)
        assert keys == [pq.query_id]

    def test_returns_private_query_handle(self, session: Session) -> None:
        pq = session.register_private_query(
            source_topics=["persistent://public/in/a", "persistent://public/in/b"],
            user_selector="x = 1",
        )
        assert pq.query_id  # non-empty uuid
        assert len(pq.query_id) == 36  # uuid4 string form
        assert pq.private_topic == f"persistent://public/filtered/filtered-{pq.query_id}"
        assert pq.source_topics == ("persistent://public/in/a", "persistent://public/in/b")

    def test_empty_source_topics_raises(self, session: Session) -> None:
        with pytest.raises(ValueError, match="source topic"):
            session.register_private_query(source_topics=[], user_selector="x = 1")

    def test_null_selector_publishes_register(self, session: Session) -> None:
        session.register_private_query(
            source_topics=["persistent://public/in/x"],
            user_selector=None,
        )
        req = _captured_sends(session)[0]
        # Wire NON_NULL semantics: omit the selector field entirely when None.
        assert "selector" not in req

    def test_kafka_client_type_threaded_through(self, session: Session) -> None:
        session.register_private_query(
            source_topics=["persistent://public/in/x"],
            user_selector="region = 'US'",
            key_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
            value_deserializer="org.apache.kafka.common.serialization.ByteArrayDeserializer",
            client_type=ClientType.KAFKA,
        )
        req = _captured_sends(session)[0]
        assert req["clientType"] == "KAFKA"
        assert req["keyDeserializer"] == "org.apache.kafka.common.serialization.StringDeserializer"
        assert req["valueDeserializer"] == \
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"

    def test_adds_query_id_to_live_set(self, session: Session) -> None:
        assert session.live_query_ids == frozenset()
        pq1 = session.register_private_query(["persistent://t/n/a"], "x = 1")
        pq2 = session.register_private_query(["persistent://t/n/b"], "x = 2")
        assert session.live_query_ids == frozenset({pq1.query_id, pq2.query_id})


# ---------------------------------------------------------------- update_private_query

class TestUpdatePrivateQuery:
    def test_publishes_modify_on_same_queryid(self, session: Session) -> None:
        pq = session.register_private_query(["persistent://t/n/x"], "x = 1")
        session.update_private_query(pq, "x = 2")

        sends = _captured_sends(session)
        assert len(sends) == 2
        modify = sends[1]
        assert modify["command"] == Command.MODIFY.value
        assert modify["selector"] == "x = 2"
        assert _captured_keys(session)[1] == pq.query_id

    def test_blank_selector_raises_at_sdk(self, session: Session) -> None:
        pq = session.register_private_query(["persistent://t/n/x"], "x = 1")
        with pytest.raises(ValueError, match="selector"):
            session.update_private_query(pq, "   ")


# ---------------------------------------------------------------- retire_private_query

class TestRetirePrivateQuery:
    def test_publishes_unregister(self, session: Session) -> None:
        pq = session.register_private_query(["persistent://t/n/x"], "x = 1")
        session.retire_private_query(pq)

        sends = _captured_sends(session)
        assert sends[1]["command"] == Command.UNREGISTER.value
        assert _captured_keys(session)[1] == pq.query_id

    def test_removes_query_id_from_live_set_before_publish(self, session: Session) -> None:
        pq1 = session.register_private_query(["persistent://t/n/a"], "x = 1")
        pq2 = session.register_private_query(["persistent://t/n/b"], "x = 2")
        session.retire_private_query(pq1)
        assert session.live_query_ids == frozenset({pq2.query_id})

    def test_idempotent_on_send_failure(self, session: Session) -> None:
        # Make the next send raise. retire_private_query must NOT propagate (best-effort).
        pq = session.register_private_query(["persistent://t/n/x"], "x = 1")
        session._control_producer.send.side_effect = RuntimeError("network down")  # noqa: SLF001
        session.retire_private_query(pq)  # no raise


# ---------------------------------------------------------------- close

class TestClose:
    def test_close_closes_producer(self, session: Session) -> None:
        session.close()
        session._control_producer.close.assert_called_once()  # noqa: SLF001

    def test_close_does_not_close_borrowed_client(self, mock_pulsar_client: MagicMock,
                                                  session: Session) -> None:
        session.close()
        mock_pulsar_client.close.assert_not_called()

    def test_context_manager_closes(self, mock_pulsar_client: MagicMock) -> None:
        with Session.builder().pulsar_client(mock_pulsar_client) \
                .control_topic("persistent://t/n/x").disable_heartbeat().build() as s:
            assert s.control_topic == "persistent://t/n/x"
        s._control_producer.close.assert_called_once()  # noqa: SLF001


# ---------------------------------------------------------------- Java wire-shape parity

class TestWireParity:
    def test_register_wire_keys_match_java(self, session: Session) -> None:
        session.register_private_query(
            source_topics=["persistent://t/n/in"],
            user_selector="x = 1",
            key_deserializer="K",
            value_deserializer="V",
            client_type=ClientType.KAFKA,
        )
        req = _captured_sends(session)[0]
        # Critical: each key here MUST match what Jackson emits on the Java side.
        assert set(req.keys()) == {
            "command", "inputTopics", "outputTopic", "selector",
            "keyDeserializer", "valueDeserializer", "clientType",
        }
