"""Session — context for SDK consumers + the producer that talks to the orchestrator.

Mirrors :class:`io.streamnative.filter.client.Session` in the Java SDK. Owns the Pulsar client
handle, the producer on the orchestrator's input topic, and (Phase 4) the heartbeat scheduler.

Sessions are thread-safe; multiple consumer constructions may run concurrently. :meth:`close`
closes all resources owned by the session.
"""

from __future__ import annotations

import logging
import threading
import uuid
from typing import TYPE_CHECKING, Any, Optional

from ._commands import ClientType
from ._protocol import FilterRequest
from .private_query import PrivateQuery
from .private_topic_policy import DEFAULT as DEFAULT_POLICY
from .private_topic_policy import PrivateTopicPolicy
from .selectors import validate as validate_selector

if TYPE_CHECKING:
    # pulsar-client is a runtime dep but only the type stubs are needed here.
    import pulsar  # noqa: F401

LOG = logging.getLogger(__name__)


class Session:
    """Context for filtered consumers.

    Construct via :meth:`builder`. Direct construction is reserved for tests that inject mocks.
    """

    def __init__(
        self,
        *,
        pulsar_client: Any,
        owns_pulsar_client: bool,
        control_topic: str,
        private_topic_tenant: str,
        private_topic_namespace: str,
        private_topic_policy: PrivateTopicPolicy,
        heartbeat_interval_seconds: float = 30.0,
    ) -> None:
        self._pulsar_client = pulsar_client
        self._owns_pulsar_client = owns_pulsar_client
        self._control_topic = control_topic
        self._private_topic_tenant = private_topic_tenant
        self._private_topic_namespace = private_topic_namespace
        self._private_topic_policy = private_topic_policy
        self._heartbeat_interval = float(heartbeat_interval_seconds)

        # Stable identifier — used as the HEARTBEAT message key. Orchestrator does not key on it.
        self._session_id = str(uuid.uuid4())

        # Live queryIds for heartbeat fan-out. Updated under _lock on register/retire.
        self._live_query_ids: set[str] = set()
        self._lock = threading.Lock()

        # Create the producer on the orchestrator's input topic. Synchronous send, no batching
        # — REGISTER/MODIFY/UNREGISTER are control plane, low-volume by design.
        self._control_producer = self._pulsar_client.create_producer(
            topic=self._control_topic,
            producer_name=f"selector-filter-session-{self._session_id}",
            block_if_queue_full=True,
        )

        # Heartbeat scheduling. A daemon thread waits on an Event with a timeout matching the
        # interval; on Session.close() we set the event for clean shutdown without waiting up
        # to a full interval. Disabled when interval <= 0.
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        if self._heartbeat_interval > 0:
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"selector-filter-heartbeat-{self._session_id}",
                daemon=True,
            )
            self._heartbeat_thread.start()
            LOG.info("Session heartbeat scheduled: interval=%ss sessionId=%s",
                     self._heartbeat_interval, self._session_id)
        else:
            LOG.info("Session heartbeats disabled (sessionId=%s)", self._session_id)

    @staticmethod
    def builder() -> "SessionBuilder":
        return SessionBuilder()

    def new_consumer(self) -> "ConsumerBuilder":
        """Start building a filtered Pulsar consumer. Configure topic(s), selector, and optional
        subscription settings, then call :meth:`ConsumerBuilder.subscribe`.
        """
        # Import lazily to avoid a circular import at module load time.
        from .consumer import ConsumerBuilder
        return ConsumerBuilder(self)

    # ---------------------------------------------------------------- accessors

    @property
    def control_topic(self) -> str:
        return self._control_topic

    @property
    def private_topic_policy(self) -> PrivateTopicPolicy:
        return self._private_topic_policy

    @property
    def private_topic_tenant(self) -> str:
        return self._private_topic_tenant

    @property
    def private_topic_namespace(self) -> str:
        return self._private_topic_namespace

    @property
    def pulsar_client(self) -> Any:
        """The underlying ``pulsar.Client`` handle — used by :class:`FilteringConsumer` to
        subscribe to private output topics.
        """
        return self._pulsar_client

    @property
    def live_query_ids(self) -> frozenset[str]:
        """Snapshot of the queryIds this Session currently considers live.

        Used by the (Phase 4) heartbeat scheduler. Returns a defensive copy.
        """
        with self._lock:
            return frozenset(self._live_query_ids)

    # ---------------------------------------------------------------- public registration API

    def register_private_query(
        self,
        source_topics: list[str],
        user_selector: Optional[str],
        *,
        key_deserializer: Optional[str] = None,
        value_deserializer: Optional[str] = None,
        client_type: ClientType = ClientType.PULSAR,
    ) -> PrivateQuery:
        """Register a filtered query with the orchestrator.

        Generates a fresh queryId, derives a private output topic name, validates the selector,
        and publishes a REGISTER FilterRequest. Returns a :class:`PrivateQuery` handle that
        downstream callers attach a consumer to.
        """
        if not source_topics:
            raise ValueError("At least one source topic is required")
        validate_selector(user_selector)

        query_id = str(uuid.uuid4())
        private_topic = (
            f"persistent://{self._private_topic_tenant}/"
            f"{self._private_topic_namespace}/filtered-{query_id}"
        )

        # Phase 6 follow-up: actually apply per-topic policies via a thin HTTP wrapper for the
        # Pulsar admin REST API. Java SDK does this via PulsarAdmin; the Python client does not
        # ship an equivalent. For now we skip — operators must configure namespace defaults.

        req = FilterRequest.register_multi(
            input_topics=list(source_topics),
            output_topic=private_topic,
            selector=user_selector,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            client_type=client_type,
        )
        self._publish_filter_request(query_id, req)

        with self._lock:
            self._live_query_ids.add(query_id)

        LOG.info(
            "Registered query queryId=%s private=%s selector=[%s] clientType=%s",
            query_id, private_topic, user_selector, client_type,
        )
        return PrivateQuery(
            query_id=query_id,
            private_topic=private_topic,
            source_topics=tuple(source_topics),
        )

    def update_private_query(self, pq: PrivateQuery, new_user_selector: str) -> None:
        """Replace the active selector on an existing query (MODIFY)."""
        validate_selector(new_user_selector)
        req = FilterRequest.modify(new_user_selector)
        req.validate()  # MODIFY requires non-blank selector — fail at SDK, not at orchestrator
        self._publish_filter_request(pq.query_id, req)
        LOG.info("Updated query queryId=%s selector=[%s]", pq.query_id, new_user_selector)

    def retire_private_query(self, pq: PrivateQuery) -> None:
        """Retract a previously-registered query (UNREGISTER).

        Best-effort: errors during publish or topic-delete are logged but do not raise. Stops
        heartbeating this queryId BEFORE publishing UNREGISTER so a racing heartbeat tick
        cannot re-touch a queryId we are about to retract.
        """
        with self._lock:
            self._live_query_ids.discard(pq.query_id)
        try:
            self._publish_filter_request(pq.query_id, FilterRequest.unregister())
            LOG.info("Retracted query queryId=%s", pq.query_id)
        except Exception as e:  # noqa: BLE001  (broad on purpose — best-effort)
            LOG.warning("Failed to retract queryId=%s: %s", pq.query_id, e)
        # Phase 6 follow-up: delete the private topic if delete_on_close is True. Requires the
        # Python HTTP admin wrapper (same as create-and-configure).

    # ---------------------------------------------------------------- internals

    def _publish_filter_request(self, message_key: str, request: FilterRequest) -> None:
        payload = request.to_json()
        self._control_producer.send(
            content=payload,
            partition_key=message_key,
        )

    def close(self) -> None:
        # Stop the heartbeat thread FIRST so it cannot publish after the producer is gone.
        self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5.0)
        try:
            self._control_producer.close()
        except Exception as e:  # noqa: BLE001
            LOG.warning("close producer: %s", e)
        if self._owns_pulsar_client:
            try:
                self._pulsar_client.close()
            except Exception as e:  # noqa: BLE001
                LOG.warning("close client: %s", e)

    # ---------------------------------------------------------------- heartbeat

    def _heartbeat_loop(self) -> None:
        """Daemon-thread body. Waits up to ``heartbeat_interval`` on the stop event; if the wait
        timed out (i.e. no stop signal) emits one heartbeat. Returns when stop is signalled.
        """
        while not self._heartbeat_stop.is_set():
            if self._heartbeat_stop.wait(timeout=self._heartbeat_interval):
                return  # close() set the event; exit cleanly
            self._send_heartbeat_quietly()

    def _send_heartbeat_quietly(self) -> None:
        """Snapshot live queryIds and publish one HEARTBEAT message. Swallows all exceptions —
        transient publish failures must not kill the scheduler.

        Package-private (single-underscore prefix is the closest Python idiom): unit tests
        invoke this directly to drive the publish path without waiting on the timer.
        """
        try:
            with self._lock:
                snapshot = list(self._live_query_ids)
            if not snapshot:
                return
            req = FilterRequest.heartbeat(snapshot)
            self._publish_filter_request(f"heartbeat-{self._session_id}", req)
            LOG.debug("Published HEARTBEAT covering %d queryId(s) sessionId=%s",
                      len(snapshot), self._session_id)
        except Exception as e:  # noqa: BLE001
            # DEBUG not WARN — transient broker unavailability would otherwise spam every
            # heartbeat_interval. The orchestrator synthesizes UNREGISTERs on extended outages.
            LOG.debug("Heartbeat publish failed (will retry next tick): %s", e)

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()


class SessionBuilder:
    """Fluent builder for :class:`Session`."""

    def __init__(self) -> None:
        self._service_url: Optional[str] = None
        self._pulsar_client: Optional[Any] = None
        self._control_topic: Optional[str] = None
        self._private_topic_tenant: str = "public"
        self._private_topic_namespace: str = "filtered"
        self._private_topic_policy: PrivateTopicPolicy = DEFAULT_POLICY
        # Default 30s matches the orchestrator default heartbeat-max-stale-seconds=90
        # (three missed heartbeats before a queryId is declared dead).
        self._heartbeat_interval_seconds: float = 30.0

    def service_url(self, url: str) -> "SessionBuilder":
        self._service_url = url
        return self

    def pulsar_client(self, client: Any) -> "SessionBuilder":
        """Use an existing :class:`pulsar.Client`. The session uses but does NOT close it —
        caller retains ownership.
        """
        self._pulsar_client = client
        return self

    def control_topic(self, topic: str) -> "SessionBuilder":
        self._control_topic = topic
        return self

    def private_topic_tenant(self, tenant: str) -> "SessionBuilder":
        self._private_topic_tenant = tenant
        return self

    def private_topic_namespace(self, namespace: str) -> "SessionBuilder":
        self._private_topic_namespace = namespace
        return self

    def private_topic_policy(self, policy: PrivateTopicPolicy) -> "SessionBuilder":
        self._private_topic_policy = policy if policy is not None else DEFAULT_POLICY
        return self

    def heartbeat_interval_seconds(self, seconds: float) -> "SessionBuilder":
        """Set how often the Session publishes a HEARTBEAT covering its live queryIds.

        The orchestrator uses heartbeats to detect SDK consumers that died without calling
        close(); it synthesizes an UNREGISTER for any queryId whose lastSeen is older than its
        ``heartbeat-max-stale-seconds`` config (default 90).

        Set to ``0`` to disable. Defaults to ``30``.
        """
        self._heartbeat_interval_seconds = max(0.0, float(seconds))
        return self

    def disable_heartbeat(self) -> "SessionBuilder":
        """Convenience: turn off heartbeats entirely. Equivalent to ``heartbeat_interval_seconds(0)``."""
        self._heartbeat_interval_seconds = 0.0
        return self

    def build(self) -> Session:
        if self._control_topic is None:
            raise ValueError("control_topic is required")
        if self._pulsar_client is None and not self._service_url:
            raise ValueError("Provide either service_url or pulsar_client")
        if self._private_topic_tenant is None:
            raise ValueError("private_topic_tenant is required")
        if self._private_topic_namespace is None:
            raise ValueError("private_topic_namespace is required")

        owns_client = False
        client = self._pulsar_client
        if client is None:
            import pulsar  # imported lazily so the package import does not require pulsar-client
            client = pulsar.Client(self._service_url)
            owns_client = True

        return Session(
            pulsar_client=client,
            owns_pulsar_client=owns_client,
            control_topic=self._control_topic,
            private_topic_tenant=self._private_topic_tenant,
            private_topic_namespace=self._private_topic_namespace,
            private_topic_policy=self._private_topic_policy,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
        )
