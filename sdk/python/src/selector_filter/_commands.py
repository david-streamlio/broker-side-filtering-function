"""Wire-format enums shared with the Java SDK and orchestrator.

The string values MUST match the Java ``Command`` enum and ``ClientType`` enum exactly — they
are serialized verbatim onto the orchestrator's input topic. See
``protocol/filter-request.schema.json`` for the canonical contract.
"""

from __future__ import annotations

from enum import Enum


class Command(str, Enum):
    """Lifecycle command on a FilterRequest, derived from the SDK call site.

    Values mirror the Java :class:`io.streamnative.filter.common.Command` enum one-for-one.
    Subclassing :class:`str` makes JSON serialization just work — ``json.dumps`` emits the value.
    """

    REGISTER = "REGISTER"
    MODIFY = "MODIFY"
    UNREGISTER = "UNREGISTER"
    HEARTBEAT = "HEARTBEAT"


class ClientType(str, Enum):
    """The SDK family producing a FilterRequest.

    Recorded in the orchestrator's registry entry. v1 has no per-clientType behavior; reserved
    for future routing decisions (e.g., output-topic placement for Kafka clients needing
    KoP-mapped namespaces).
    """

    PULSAR = "PULSAR"
    KAFKA = "KAFKA"
