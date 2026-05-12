"""PrivateQuery — handle returned by :meth:`Session.register_private_query`.

Mirrors the Java :class:`PrivateQuery` record. The SDK uses this internally to keep the queryId
and the SDK-generated private output topic together, plus the source topics the user asked to
filter. The same handle is passed to :meth:`Session.update_private_query` and
:meth:`Session.retire_private_query`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PrivateQuery:
    """Immutable handle for a registered query."""

    query_id: str
    private_topic: str
    source_topics: tuple[str, ...]
