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

"""FilterRequest serde + validation tests.

Mirrors ``common/src/test/java/io/streamnative/filter/common/FilterRequestTests.java`` so the
Python and Java SDKs cannot drift apart on the wire. Adding a new field on either side without
the corresponding test here means the two implementations would silently disagree on validity.
"""

from __future__ import annotations

import json

import pytest

from selector_filter import ClientType, Command, FilterRequest

# ---------------------------------------------------------------- factory + getter parity

class TestFactories:
    def test_register_single_input_topic(self) -> None:
        req = FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=ClientType.KAFKA,
        )
        assert req.command is Command.REGISTER
        assert req.input_topics == ["persistent://t/n/in"]
        assert req.topics_pattern is None
        assert req.output_topic == "persistent://t/n/out"
        assert req.selector == "x = 1"
        assert req.client_type is ClientType.KAFKA

    def test_register_multi_input_topics(self) -> None:
        req = FilterRequest.register_multi(
            input_topics=["persistent://t/n/a", "persistent://t/n/b"],
            output_topic="persistent://t/n/out",
            selector="x = 1",
        )
        assert req.input_topics == ["persistent://t/n/a", "persistent://t/n/b"]
        assert req.client_type is ClientType.PULSAR  # default

    def test_register_pattern(self) -> None:
        req = FilterRequest.register_pattern(
            topics_pattern="persistent://t/n/.*",
            output_topic="persistent://t/n/out",
            selector="x = 1",
        )
        assert req.topics_pattern == "persistent://t/n/.*"
        assert req.input_topics is None

    def test_modify_carries_only_selector(self) -> None:
        req = FilterRequest.modify("region IN ('US','UK')")
        assert req.command is Command.MODIFY
        assert req.selector == "region IN ('US','UK')"
        assert req.input_topics is None
        assert req.output_topic is None
        assert req.client_type is None

    def test_unregister_carries_no_fields(self) -> None:
        req = FilterRequest.unregister()
        assert req.command is Command.UNREGISTER
        assert req.input_topics is None
        assert req.selector is None
        assert req.query_ids is None

    def test_heartbeat_carries_query_ids(self) -> None:
        req = FilterRequest.heartbeat(["a", "b", "c"])
        assert req.command is Command.HEARTBEAT
        assert req.query_ids == ["a", "b", "c"]


# ---------------------------------------------------------------- validate() — REGISTER

class TestValidateRegister:
    def test_without_inputs_or_pattern_throws(self) -> None:
        req = FilterRequest(
            command=Command.REGISTER,
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=ClientType.PULSAR,
        )
        with pytest.raises(ValueError, match="input_topics"):
            req.validate()

    def test_with_both_inputs_and_pattern_throws(self) -> None:
        req = FilterRequest(
            command=Command.REGISTER,
            input_topics=["persistent://t/n/a"],
            topics_pattern="persistent://t/n/.*",
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=ClientType.PULSAR,
        )
        with pytest.raises(ValueError, match="exactly one"):
            req.validate()

    def test_empty_inputs_list_throws(self) -> None:
        req = FilterRequest(
            command=Command.REGISTER,
            input_topics=[],
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=ClientType.PULSAR,
        )
        with pytest.raises(ValueError):
            req.validate()

    def test_blank_input_topic_throws(self) -> None:
        req = FilterRequest(
            command=Command.REGISTER,
            input_topics=["   "],
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=ClientType.PULSAR,
        )
        with pytest.raises(ValueError, match="blank"):
            req.validate()

    def test_missing_output_topic_throws(self) -> None:
        req = FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic=None,  # type: ignore[arg-type]
            selector="x = 1",
        )
        with pytest.raises(ValueError, match="output_topic"):
            req.validate()

    def test_null_selector_is_allowed(self) -> None:
        # null selector = match-everything. Orchestrator translates to JMS identity downstream.
        req = FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic="persistent://t/n/out",
            selector=None,
        )
        req.validate()  # no raise

    def test_blank_selector_is_allowed(self) -> None:
        req = FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic="persistent://t/n/out",
            selector="   ",
        )
        req.validate()  # no raise

    def test_missing_client_type_throws(self) -> None:
        req = FilterRequest(
            command=Command.REGISTER,
            input_topics=["persistent://t/n/in"],
            output_topic="persistent://t/n/out",
            selector="x = 1",
            client_type=None,
        )
        with pytest.raises(ValueError, match="client_type"):
            req.validate()

    def test_happy_path_passes(self) -> None:
        FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic="persistent://t/n/out",
            selector="x = 1",
        ).validate()

    def test_pattern_happy_path_passes(self) -> None:
        FilterRequest.register_pattern(
            topics_pattern="persistent://t/n/.*",
            output_topic="persistent://t/n/out",
            selector="x = 1",
            key_deserializer="K",
            value_deserializer="V",
            client_type=ClientType.KAFKA,
        ).validate()


# ---------------------------------------------------------------- validate() — MODIFY

class TestValidateModify:
    def test_without_selector_throws(self) -> None:
        req = FilterRequest(command=Command.MODIFY)
        with pytest.raises(ValueError, match="selector"):
            req.validate()

    def test_blank_selector_throws(self) -> None:
        with pytest.raises(ValueError, match="selector"):
            FilterRequest.modify("   ").validate()

    def test_happy_path_passes(self) -> None:
        FilterRequest.modify("x = 2").validate()

    def test_ignores_extraneous_fields(self) -> None:
        # MODIFY tolerates extra fields silently — orchestrator may strip or log them; validation
        # never throws.
        req = FilterRequest(
            command=Command.MODIFY,
            input_topics=["persistent://t/n/in"],
            output_topic="persistent://t/n/out",
            selector="x = 1",
            key_deserializer="K",
            value_deserializer="V",
            client_type=ClientType.KAFKA,
        )
        req.validate()


# ---------------------------------------------------------------- validate() — UNREGISTER

class TestValidateUnregister:
    def test_happy_path_passes(self) -> None:
        FilterRequest.unregister().validate()

    def test_ignores_extraneous_fields(self) -> None:
        req = FilterRequest(
            command=Command.UNREGISTER,
            input_topics=["persistent://t/n/x"],
            output_topic="persistent://t/n/out",
            selector="x = 1",
            key_deserializer="K",
            value_deserializer="V",
            client_type=ClientType.PULSAR,
        )
        req.validate()  # no raise


# ---------------------------------------------------------------- validate() — HEARTBEAT

class TestValidateHeartbeat:
    def test_happy_path_passes(self) -> None:
        FilterRequest.heartbeat(["uuid-1", "uuid-2"]).validate()

    def test_null_query_ids_throws(self) -> None:
        req = FilterRequest(command=Command.HEARTBEAT)
        with pytest.raises(ValueError, match="query_ids"):
            req.validate()

    def test_empty_query_ids_throws(self) -> None:
        with pytest.raises(ValueError, match="query_ids"):
            FilterRequest.heartbeat([]).validate()

    def test_blank_query_id_throws(self) -> None:
        with pytest.raises(ValueError, match="blank"):
            FilterRequest.heartbeat(["uuid-1", "   "]).validate()


# ---------------------------------------------------------------- validate() — command absent

class TestValidateCommand:
    def test_null_command_throws(self) -> None:
        req = FilterRequest(command=None)
        with pytest.raises(ValueError, match="command"):
            req.validate()


# ---------------------------------------------------------------- serde

class TestSerde:
    def test_register_round_trip(self) -> None:
        original = FilterRequest.register(
            input_topic="persistent://public/in/trades",
            output_topic="persistent://public/filtered/filtered-uuid",
            selector="region = 'US'",
            key_deserializer="org.apache.kafka.common.serialization.StringDeserializer",
            value_deserializer="org.apache.kafka.common.serialization.ByteArrayDeserializer",
            client_type=ClientType.KAFKA,
        )
        round_tripped = FilterRequest.from_json(original.to_json())
        assert round_tripped == original

    def test_unregister_round_trip(self) -> None:
        original = FilterRequest.unregister()
        assert FilterRequest.from_json(original.to_json()) == original

    def test_modify_round_trip(self) -> None:
        original = FilterRequest.modify("x = 2")
        assert FilterRequest.from_json(original.to_json()) == original

    def test_heartbeat_round_trip(self) -> None:
        original = FilterRequest.heartbeat(["a", "b", "c"])
        assert FilterRequest.from_json(original.to_json()) == original

    def test_wire_uses_java_camel_case(self) -> None:
        # CRITICAL: the wire JSON MUST use Java's camelCase field names so the Java orchestrator
        # can deserialize it. If any key here disagrees with Jackson's default field name on the
        # Java side, the two SDKs are silently incompatible.
        req = FilterRequest.register(
            input_topic="persistent://t/n/in",
            output_topic="persistent://t/n/out",
            selector="x = 1",
            key_deserializer="K",
            value_deserializer="V",
            client_type=ClientType.KAFKA,
        )
        wire = json.loads(req.to_json())
        assert wire == {
            "command": "REGISTER",
            "inputTopics": ["persistent://t/n/in"],
            "outputTopic": "persistent://t/n/out",
            "selector": "x = 1",
            "keyDeserializer": "K",
            "valueDeserializer": "V",
            "clientType": "KAFKA",
        }

    def test_heartbeat_wire_shape(self) -> None:
        req = FilterRequest.heartbeat(["a", "b"])
        wire = json.loads(req.to_json())
        assert wire == {"command": "HEARTBEAT", "queryIds": ["a", "b"]}

    def test_omits_null_fields(self) -> None:
        req = FilterRequest.unregister()
        wire = json.loads(req.to_json())
        assert wire == {"command": "UNREGISTER"}

    def test_unknown_fields_tolerated(self) -> None:
        # Forward-compat: the Java side might add a field we do not know about. Reading the
        # message must NOT raise; we just ignore the extra key.
        raw = {
            "command": "REGISTER",
            "inputTopics": ["persistent://t/n/in"],
            "outputTopic": "persistent://t/n/out",
            "selector": "x = 1",
            "clientType": "PULSAR",
            "someFutureField": {"nested": "value"},
        }
        req = FilterRequest.from_dict(raw)
        assert req.command is Command.REGISTER
        assert req.input_topics == ["persistent://t/n/in"]

    def test_from_json_accepts_string(self) -> None:
        s = '{"command":"UNREGISTER"}'
        assert FilterRequest.from_json(s).command is Command.UNREGISTER

    def test_from_json_accepts_bytes(self) -> None:
        b = b'{"command":"UNREGISTER"}'
        assert FilterRequest.from_json(b).command is Command.UNREGISTER

    def test_invalid_command_value_raises(self) -> None:
        # A wire message with an unknown command value would crash Java deserialization too.
        # We surface a ValueError at parse time.
        raw = '{"command":"NOT_A_COMMAND"}'
        with pytest.raises(ValueError):
            FilterRequest.from_json(raw)


# ---------------------------------------------------------------- cross-language parity

class TestJavaWireParity:
    """Validate against the literal wire examples that the Java tests use.

    If any of these break, the two SDKs are speaking incompatible dialects.
    """

    def test_canonical_register_example_matches_schema_example(self) -> None:
        # From protocol/filter-request.schema.json examples[0]:
        canonical = {
            "command": "REGISTER",
            "inputTopics": ["persistent://public/in/trades"],
            "outputTopic":
                "persistent://public/filtered/"
                "filtered-c4f9e7b1-d2a3-4f6e-9b1a-1234567890ab",
            "selector": "region = 'US'",
            "keyDeserializer": "org.apache.kafka.common.serialization.StringDeserializer",
            "valueDeserializer": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
            "clientType": "KAFKA",
        }
        req = FilterRequest.from_dict(canonical)
        req.validate()
        assert json.loads(req.to_json()) == canonical

    def test_canonical_heartbeat_example_matches_schema_example(self) -> None:
        canonical = {
            "command": "HEARTBEAT",
            "queryIds": [
                "c4f9e7b1-d2a3-4f6e-9b1a-1234567890ab",
                "0e7b1c4a-9b1a-4f6e-d2a3-7890abcdef12",
            ],
        }
        req = FilterRequest.from_dict(canonical)
        req.validate()
        assert json.loads(req.to_json()) == canonical
