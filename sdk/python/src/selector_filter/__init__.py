"""Broker-side selector filter SDK for Apache Pulsar.

Public surface:
- :class:`Session` / :class:`SessionBuilder` — entry point, construct via :meth:`Session.builder`
- :class:`FilteringConsumer` / :class:`ConsumerBuilder` — filtered consumer, returned by
  :meth:`Session.new_consumer`
- :class:`PrivateQuery` — handle returned by :meth:`Session.register_private_query`
- :class:`PrivateTopicPolicy` — per-query private output topic settings
- Wire types: :class:`Command`, :class:`ClientType`, :class:`FilterRequest`
"""

from ._commands import ClientType, Command
from ._protocol import FilterRequest
from .consumer import ConsumerBuilder, FilteringConsumer
from .private_query import PrivateQuery
from .private_topic_policy import DEFAULT as DEFAULT_PRIVATE_TOPIC_POLICY
from .private_topic_policy import PrivateTopicPolicy
from .session import Session, SessionBuilder

__all__ = [
    "ClientType",
    "Command",
    "ConsumerBuilder",
    "DEFAULT_PRIVATE_TOPIC_POLICY",
    "FilterRequest",
    "FilteringConsumer",
    "PrivateQuery",
    "PrivateTopicPolicy",
    "Session",
    "SessionBuilder",
]
__version__ = "0.1.0"
