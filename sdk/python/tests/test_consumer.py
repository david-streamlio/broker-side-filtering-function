# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""FilteringConsumer + ConsumerBuilder unit tests — mocked Pulsar client.

Covers the wire shape that ``new_consumer().topic(...).selector(...).subscribe()`` produces,
the FilterControl semantics (query_id, current_selector, set_selector), and the close path
(retract registered query, propagate close to underlying consumer, idempotent on second call).
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from selector_filter import (
    Command,
    FilteringConsumer,
    Session,
)


@pytest.fixture
def mock_pulsar_client() -> MagicMock:
    client = MagicMock(name="pulsar.Client")
    producer = MagicMock(name="pulsar.Producer")
    consumer = MagicMock(name="pulsar.Consumer")
    client.create_producer.return_value = producer
    client.subscribe.return_value = consumer
    return client


@pytest.fixture
def session(mock_pulsar_client: MagicMock) -> Session:
    return (
        Session.builder()
        .pulsar_client(mock_pulsar_client)
        .control_topic("persistent://public/orchestrator/filter-requests")
        .disable_heartbeat()
        .build()
    )


def _last_send(session: Session) -> dict:
    producer = session._control_producer  # noqa: SLF001
    last = producer.send.call_args_list[-1]
    payload = last.kwargs.get("content") or last.args[0]
    return json.loads(payload)


# ---------------------------------------------------------------- builder validation

class TestConsumerBuilder:
    def test_subscribe_without_topic_raises(self, session: Session) -> None:
        with pytest.raises(ValueError, match="topic"):
            session.new_consumer().subscribe()


# ---------------------------------------------------------------- subscribe wires everything

class TestSubscribe:
    def test_subscribe_publishes_register(self, session: Session) -> None:
        # Patch in fake Pulsar enums so the builder does not import pulsar at runtime.
        session._control_producer.send.return_value = None  # noqa: SLF001
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/trades")
            .selector("region = 'US'")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        req = _last_send(session)
        assert req["command"] == Command.REGISTER.value
        assert req["inputTopics"] == ["persistent://public/in/trades"]
        assert req["selector"] == "region = 'US'"
        # And a Pulsar consumer was created on the SDK-generated private topic.
        assert isinstance(consumer, FilteringConsumer)
        assert req["outputTopic"].endswith(consumer.query_id())

    def test_subscribe_uses_filtered_private_topic(self, mock_pulsar_client: MagicMock,
                                                   session: Session) -> None:
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("x = 1")
            .subscription_name("my-sub")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        mock_pulsar_client.subscribe.assert_called_once()
        call = mock_pulsar_client.subscribe.call_args
        assert call.kwargs["topic"].startswith("persistent://public/filtered/filtered-")
        assert call.kwargs["topic"].endswith(consumer.query_id())
        assert call.kwargs["subscription_name"] == "my-sub"
        assert call.kwargs["consumer_type"] == "FAILOVER_STUB"
        assert call.kwargs["initial_position"] == "LATEST_STUB"

    def test_subscribe_rollback_on_pulsar_subscribe_failure(self, mock_pulsar_client: MagicMock,
                                                            session: Session) -> None:
        mock_pulsar_client.subscribe.side_effect = RuntimeError("broker down")
        with pytest.raises(RuntimeError, match="broker down"):
            (
                session.new_consumer()
                .topic("persistent://public/in/x")
                .selector("x = 1")
                .subscription_type("FAILOVER_STUB")
                .subscription_initial_position("LATEST_STUB")
                .subscribe()
            )
        # Two control-plane publishes: the REGISTER, then a rollback UNREGISTER.
        producer = session._control_producer  # noqa: SLF001
        sends = [json.loads(c.kwargs.get("content") or c.args[0])
                 for c in producer.send.call_args_list]
        assert sends[0]["command"] == Command.REGISTER.value
        assert sends[1]["command"] == Command.UNREGISTER.value

    def test_subscribe_with_topics_list(self, mock_pulsar_client: MagicMock,
                                        session: Session) -> None:
        (
            session.new_consumer()
            .topics(["persistent://public/in/a", "persistent://public/in/b"])
            .selector("x = 1")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        req = _last_send(session)
        assert req["inputTopics"] == ["persistent://public/in/a", "persistent://public/in/b"]


# ---------------------------------------------------------------- FilterControl methods

class TestFilterControl:
    def _build_consumer(self, session: Session) -> FilteringConsumer:
        return (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("region = 'US'")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )

    def test_query_id_is_stable(self, session: Session) -> None:
        consumer = self._build_consumer(session)
        qid1 = consumer.query_id()
        consumer.set_selector("region = 'UK'")
        assert consumer.query_id() == qid1

    def test_set_selector_publishes_modify(self, session: Session) -> None:
        consumer = self._build_consumer(session)
        consumer.set_selector("region IN ('US','UK')")
        req = _last_send(session)
        assert req["command"] == Command.MODIFY.value
        assert req["selector"] == "region IN ('US','UK')"

    def test_current_selector_updates_after_set(self, session: Session) -> None:
        consumer = self._build_consumer(session)
        assert consumer.current_selector() == "region = 'US'"
        consumer.set_selector("region = 'UK'")
        assert consumer.current_selector() == "region = 'UK'"

    def test_current_selector_is_none_when_subscribe_passed_none(
        self, mock_pulsar_client: MagicMock, session: Session
    ) -> None:
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector(None)
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        assert consumer.current_selector() is None

    def test_set_selector_after_close_raises(self, session: Session) -> None:
        consumer = self._build_consumer(session)
        consumer.close()
        with pytest.raises(RuntimeError, match="closed"):
            consumer.set_selector("x = 1")


# ---------------------------------------------------------------- close path

class TestClose:
    def _build_consumer(self, session: Session) -> FilteringConsumer:
        return (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("x = 1")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )

    def test_close_calls_underlying_close_and_publishes_unregister(self, mock_pulsar_client:
                                                                   MagicMock,
                                                                   session: Session) -> None:
        consumer = self._build_consumer(session)
        underlying = mock_pulsar_client.subscribe.return_value
        consumer.close()
        underlying.close.assert_called_once()
        # Last wire publish must be UNREGISTER on the queryId.
        last = _last_send(session)
        assert last["command"] == Command.UNREGISTER.value

    def test_double_close_is_idempotent(self, mock_pulsar_client: MagicMock,
                                        session: Session) -> None:
        consumer = self._build_consumer(session)
        consumer.close()
        consumer.close()  # no raise
        underlying = mock_pulsar_client.subscribe.return_value
        underlying.close.assert_called_once()  # only the first close fired

    def test_context_manager_closes(self, mock_pulsar_client: MagicMock,
                                    session: Session) -> None:
        with self._build_consumer(session) as consumer:
            assert consumer.query_id()
        underlying = mock_pulsar_client.subscribe.return_value
        underlying.close.assert_called_once()


# ---------------------------------------------------------------- message access

class TestMessageAccess:
    def test_receive_delegates_to_underlying(self, mock_pulsar_client: MagicMock,
                                             session: Session) -> None:
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("x = 1")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        underlying = mock_pulsar_client.subscribe.return_value
        underlying.receive.return_value = "fake-message"
        assert consumer.receive(timeout_millis=5000) == "fake-message"
        underlying.receive.assert_called_once_with(timeout_millis=5000)

    def test_receive_without_timeout(self, mock_pulsar_client: MagicMock,
                                     session: Session) -> None:
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("x = 1")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        underlying = mock_pulsar_client.subscribe.return_value
        underlying.receive.return_value = "blocking-message"
        assert consumer.receive() == "blocking-message"
        underlying.receive.assert_called_once_with()

    def test_acknowledge_delegates(self, mock_pulsar_client: MagicMock,
                                   session: Session) -> None:
        consumer = (
            session.new_consumer()
            .topic("persistent://public/in/x")
            .selector("x = 1")
            .subscription_type("FAILOVER_STUB")
            .subscription_initial_position("LATEST_STUB")
            .subscribe()
        )
        underlying = mock_pulsar_client.subscribe.return_value
        consumer.acknowledge("msg")
        underlying.acknowledge.assert_called_once_with("msg")
