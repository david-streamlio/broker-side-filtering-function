"""FilteringConsumer — Pulsar consumer attached to the SDK-generated private output topic,
plus the FilterControl methods (``set_selector``, ``query_id``, ``current_selector``) needed to
mutate the registered query at runtime.

Mirrors :class:`io.streamnative.filter.client.FilteringPulsarConsumer` in the Java SDK. The
Python SDK exposes both the message-receive API (``receive``, ``acknowledge``) and the control
methods on a single object, matching the Java pattern where the returned ``Consumer<byte[]>``
also implements ``FilterControl``.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, Optional

from .private_query import PrivateQuery

if TYPE_CHECKING:
    import pulsar  # noqa: F401

    from .session import Session

LOG = logging.getLogger(__name__)


class FilteringConsumer:
    """A Pulsar consumer reading the SDK-generated private output topic, with FilterControl
    methods to mutate or retract the underlying registered query.

    Instances are constructed by :meth:`ConsumerBuilder.subscribe`; direct construction is
    reserved for tests injecting mocks.
    """

    def __init__(
        self,
        *,
        session: "Session",
        private_query: PrivateQuery,
        underlying: Any,
        current_selector: Optional[str],
    ) -> None:
        self._session = session
        self._private_query = private_query
        self._underlying = underlying
        self._current_selector = current_selector
        self._closed = False

    # ---------------------------------------------------------------- FilterControl

    def query_id(self) -> str:
        """The SDK-generated UUIDv4 for this registered query.

        Stable across :meth:`set_selector`.
        """
        return self._private_query.query_id

    def current_selector(self) -> Optional[str]:
        """The selector active on the orchestrator for this query, as last set by the SDK.

        Note: this is the SDK's local view. If a parallel SDK instance MODIFIED the same query
        (which shouldn't happen since queryIds are SDK-generated UUIDv4), this value would lag
        until the next call.
        """
        return self._current_selector

    def set_selector(self, new_selector: str) -> None:
        """Replace the active selector on the orchestrator. The orchestrator republishes the
        downstream QueryMessage on every per-function control topic this query is registered to,
        triggering a hot reload on the filter function.
        """
        if self._closed:
            raise RuntimeError("Consumer is closed")
        self._session.update_private_query(self._private_query, new_selector)
        self._current_selector = new_selector

    # ---------------------------------------------------------------- message access

    def receive(self, timeout_millis: Optional[int] = None) -> Any:
        """Block until the next message is available (or timeout). Delegates to the underlying
        Pulsar consumer; returns a ``pulsar.Message``.

        Raises ``pulsar.Timeout`` on timeout (Pulsar's native exception).
        """
        if timeout_millis is not None:
            return self._underlying.receive(timeout_millis=timeout_millis)
        return self._underlying.receive()

    def acknowledge(self, message: Any) -> None:
        self._underlying.acknowledge(message)

    def negative_acknowledge(self, message: Any) -> None:
        self._underlying.negative_acknowledge(message)

    def __iter__(self) -> "FilteringConsumer":
        return self

    def __next__(self) -> Any:
        """Iterator protocol — blocks until the next message arrives.

        Use with a wall-clock loop break in production; without a timeout this is an infinite
        iterator that never raises ``StopIteration``.
        """
        return self._underlying.receive()

    # ---------------------------------------------------------------- lifecycle

    def close(self) -> None:
        """Close the underlying Pulsar consumer AND retract the registered query.

        Calling close more than once is a no-op. Errors during retract are logged but do not
        propagate — match the Java SDK's best-effort retract behavior.
        """
        if self._closed:
            return
        self._closed = True
        try:
            self._underlying.close()
        except Exception as e:  # noqa: BLE001
            LOG.warning("close underlying consumer: %s", e)
        self._session.retire_private_query(self._private_query)

    def __enter__(self) -> "FilteringConsumer":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class ConsumerBuilder:
    """Fluent builder for :class:`FilteringConsumer`. Accessed via :meth:`Session.new_consumer`.

    Required: at least one input topic (``topic`` or ``topics``) and a selector (``selector``);
    a blank/None selector is allowed and means "match every record on the source topic(s)".
    """

    def __init__(self, session: "Session") -> None:
        self._session = session
        self._topics: list[str] = []
        self._selector: Optional[str] = None
        self._subscription_name: Optional[str] = None
        # Defaults match the Java FilterableConsumerBuilder: Failover sub, Latest position. Lazy
        # imports of pulsar.* so the package import does not pull in pulsar-client.
        self._subscription_type: Any = None
        self._subscription_initial_position: Any = None

    def topic(self, topic: str) -> "ConsumerBuilder":
        self._topics.append(topic)
        return self

    def topics(self, topics: list[str]) -> "ConsumerBuilder":
        self._topics.extend(topics)
        return self

    def selector(self, selector: Optional[str]) -> "ConsumerBuilder":
        self._selector = selector
        return self

    def subscription_name(self, name: str) -> "ConsumerBuilder":
        self._subscription_name = name
        return self

    def subscription_type(self, sub_type: Any) -> "ConsumerBuilder":
        """``pulsar.ConsumerType`` instance. Default: ``Failover``."""
        self._subscription_type = sub_type
        return self

    def subscription_initial_position(self, position: Any) -> "ConsumerBuilder":
        """``pulsar.InitialPosition`` instance. Default: ``Latest``."""
        self._subscription_initial_position = position
        return self

    def subscribe(self) -> FilteringConsumer:
        """Register the query with the orchestrator AND subscribe a Pulsar consumer to the
        SDK-generated private output topic.

        If the Pulsar-side subscribe fails, the SDK retracts the (just-issued) registration
        before re-raising — leaves no orphaned state on the orchestrator.
        """
        if not self._topics:
            raise ValueError("At least one topic is required")

        pq = self._session.register_private_query(self._topics, self._selector)
        sub_name = self._subscription_name or f"session-{uuid.uuid4()}"

        # Resolve Pulsar defaults lazily so tests that mock pulsar.* don't need real enums.
        sub_type = self._subscription_type
        if sub_type is None:
            import pulsar
            sub_type = pulsar.ConsumerType.Failover
        initial_position = self._subscription_initial_position
        if initial_position is None:
            import pulsar
            initial_position = pulsar.InitialPosition.Latest

        try:
            underlying = self._session.pulsar_client.subscribe(
                topic=pq.private_topic,
                subscription_name=sub_name,
                consumer_type=sub_type,
                initial_position=initial_position,
            )
        except Exception:
            # Roll back the just-issued REGISTER so we don't leak an orphan queryId in the
            # orchestrator's state.
            self._session.retire_private_query(pq)
            raise

        stored_selector = self._selector if self._selector and self._selector.strip() else None
        return FilteringConsumer(
            session=self._session,
            private_query=pq,
            underlying=underlying,
            current_selector=stored_selector,
        )
