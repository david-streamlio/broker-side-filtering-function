"""FilterRequest — SDK -> Orchestrator wire-format record.

Mirrors :class:`io.streamnative.filter.common.FilterRequest` in the Java SDK. Both serialize to
the same JSON shape declared by ``protocol/filter-request.schema.json``.

The class is immutable. ``validate()`` mirrors the Java implementation one-for-one: a request
that round-trips between SDKs and fails validation in one must fail in the other with the same
rule.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, Optional, Union

from ._commands import ClientType, Command


@dataclass(frozen=True)
class FilterRequest:
    """Immutable SDK -> Orchestrator wire record.

    Use the :meth:`register`, :meth:`register_pattern`, :meth:`register_multi`, :meth:`modify`,
    :meth:`unregister`, and :meth:`heartbeat` factory methods for typed call sites; only construct
    instances directly if you know exactly what command shape you need.
    """

    command: Optional[Command]
    input_topics: Optional[list[str]] = None
    topics_pattern: Optional[str] = None
    output_topic: Optional[str] = None
    selector: Optional[str] = None
    key_deserializer: Optional[str] = None
    value_deserializer: Optional[str] = None
    client_type: Optional[ClientType] = None
    query_ids: Optional[list[str]] = None

    # Python snake_case <-> Java camelCase field name mapping. The wire JSON is Java's camelCase
    # (per protocol/filter-request.schema.json) so we have to convert in both directions.
    _PY_TO_WIRE: ClassVar[dict[str, str]] = {
        "input_topics": "inputTopics",
        "topics_pattern": "topicsPattern",
        "output_topic": "outputTopic",
        "key_deserializer": "keyDeserializer",
        "value_deserializer": "valueDeserializer",
        "client_type": "clientType",
        "query_ids": "queryIds",
    }
    _WIRE_TO_PY: ClassVar[dict[str, str]] = {wire: py for py, wire in _PY_TO_WIRE.items()}

    # ------------------------------------------------------------------ factories

    @staticmethod
    def register(
        input_topic: str,
        output_topic: str,
        selector: Optional[str],
        key_deserializer: Optional[str] = None,
        value_deserializer: Optional[str] = None,
        client_type: ClientType = ClientType.PULSAR,
    ) -> FilterRequest:
        """Build a REGISTER request for a single input topic."""
        return FilterRequest(
            command=Command.REGISTER,
            input_topics=[input_topic],
            output_topic=output_topic,
            selector=selector,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            client_type=client_type,
        )

    @staticmethod
    def register_multi(
        input_topics: list[str],
        output_topic: str,
        selector: Optional[str],
        key_deserializer: Optional[str] = None,
        value_deserializer: Optional[str] = None,
        client_type: ClientType = ClientType.PULSAR,
    ) -> FilterRequest:
        """Build a REGISTER request for multiple input topics."""
        return FilterRequest(
            command=Command.REGISTER,
            input_topics=list(input_topics),
            output_topic=output_topic,
            selector=selector,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            client_type=client_type,
        )

    @staticmethod
    def register_pattern(
        topics_pattern: str,
        output_topic: str,
        selector: Optional[str],
        key_deserializer: Optional[str] = None,
        value_deserializer: Optional[str] = None,
        client_type: ClientType = ClientType.PULSAR,
    ) -> FilterRequest:
        """Build a REGISTER request matching a regex pattern."""
        return FilterRequest(
            command=Command.REGISTER,
            topics_pattern=topics_pattern,
            output_topic=output_topic,
            selector=selector,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
            client_type=client_type,
        )

    @staticmethod
    def modify(new_selector: str) -> FilterRequest:
        """Build a MODIFY request replacing the selector on an existing queryId."""
        return FilterRequest(command=Command.MODIFY, selector=new_selector)

    @staticmethod
    def unregister() -> FilterRequest:
        """Build an UNREGISTER request retracting an existing queryId."""
        return FilterRequest(command=Command.UNREGISTER)

    @staticmethod
    def heartbeat(query_ids: list[str]) -> FilterRequest:
        """Build a HEARTBEAT request listing every queryId the Session currently owns."""
        return FilterRequest(command=Command.HEARTBEAT, query_ids=list(query_ids))

    # ------------------------------------------------------------------ validation

    def validate(self) -> None:
        """Validate command-specific field requirements.

        Raises :class:`ValueError` with a message identifying the violated rule. Matches the
        ``protocol/filter-request.schema.json`` contract one-for-one — a request that fails here
        in Python must also fail server-side, and vice versa.
        """
        if self.command is None:
            raise ValueError("command is required")
        if self.command is Command.REGISTER:
            self._validate_register()
        elif self.command is Command.MODIFY:
            self._validate_modify()
        elif self.command is Command.UNREGISTER:
            pass  # No required fields. Extra fields tolerated.
        elif self.command is Command.HEARTBEAT:
            self._validate_heartbeat()

    def _validate_register(self) -> None:
        has_inputs = bool(self.input_topics)
        has_pattern = bool(self.topics_pattern and self.topics_pattern.strip())
        if has_inputs == has_pattern:
            raise ValueError(
                "REGISTER requires exactly one of input_topics (non-empty) or "
                "topics_pattern (non-blank)"
            )
        if has_inputs:
            for t in self.input_topics or []:
                if t is None or not t.strip():
                    raise ValueError("REGISTER input_topics contains a null/blank entry")
        if not self.output_topic or not self.output_topic.strip():
            raise ValueError("REGISTER requires output_topic")
        # selector may be null/blank — interpreted as "no user-side filter; match every record".
        # The orchestrator translates to JMS-selector identity ("1=1") downstream so the filter
        # function never sees a null selector.
        if self.client_type is None:
            raise ValueError("REGISTER requires client_type")

    def _validate_modify(self) -> None:
        if not self.selector or not self.selector.strip():
            raise ValueError("MODIFY requires selector")

    def _validate_heartbeat(self) -> None:
        if not self.query_ids:
            raise ValueError("HEARTBEAT requires a non-empty query_ids list")
        for qid in self.query_ids:
            if qid is None or not qid.strip():
                raise ValueError("HEARTBEAT query_ids contains a null/blank entry")

    # ------------------------------------------------------------------ serde

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict using the Java camelCase wire field names.

        Fields whose value is ``None`` are omitted (mirrors Java's ``@JsonInclude(NON_NULL)``).
        """
        out: dict[str, Any] = {}
        for py_name, value in asdict(self).items():
            if value is None:
                continue
            wire_name = self._PY_TO_WIRE.get(py_name, py_name)
            # asdict() already unwraps str-subclass Enums to their string value, but be
            # defensive: convert any remaining Enum to its raw value.
            if isinstance(value, (Command, ClientType)):
                value = value.value
            out[wire_name] = value
        return out

    def to_json(self) -> bytes:
        """Serialize to UTF-8 JSON bytes ready for ``producer.send``."""
        return json.dumps(self.to_dict(), separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FilterRequest:
        """Construct from a Java-camelCase dict. Unknown keys are silently ignored for
        forward-compatibility (mirrors Java's ``@JsonIgnoreProperties(ignoreUnknown=true)``).
        """
        field_names = frozenset(cls.__dataclass_fields__.keys())
        kwargs: dict[str, Any] = {}
        for wire_name, value in data.items():
            py_name = cls._WIRE_TO_PY.get(wire_name, wire_name)
            if py_name not in field_names:
                continue  # forward-compat: tolerate unknown fields
            if py_name == "command" and value is not None:
                value = Command(value)
            elif py_name == "client_type" and value is not None:
                value = ClientType(value)
            kwargs[py_name] = value
        # Default missing 'command' to None so validate() can complain consistently with Java.
        kwargs.setdefault("command", None)
        return cls(**kwargs)

    @classmethod
    def from_json(cls, payload: Union[bytes, str]) -> FilterRequest:
        """Parse from UTF-8 JSON bytes (or a string). Raises ``ValueError`` on malformed JSON."""
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8")
        return cls.from_dict(json.loads(payload))
