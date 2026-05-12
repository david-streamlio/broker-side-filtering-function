"""Broker-side selector filter SDK for Apache Pulsar.

Phase 1 currently exposes only the wire-format types. Higher-level abstractions (``Session``,
``FilterableConsumer``, ``FilterControl``) land in subsequent phases.
"""

from ._commands import ClientType, Command
from ._protocol import FilterRequest

__all__ = ["ClientType", "Command", "FilterRequest"]
__version__ = "0.1.0"
