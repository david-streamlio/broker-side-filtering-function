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
