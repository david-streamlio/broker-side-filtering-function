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

"""PrivateTopicPolicy — SDK-side policy applied to the per-query private output topics.

Java parity: matches the fields on :class:`io.streamnative.filter.client.PrivateTopicPolicy`.

The Pulsar Python client does NOT bundle the admin API surface (PulsarAdmin) that the Java SDK
uses to set per-topic policies. This Python SDK therefore stores the policy values but does
NOT actually apply them to the broker — operators must configure equivalent defaults at the
namespace level. A future Phase will add a thin HTTP wrapper for the per-topic policy REST
endpoints; until then the ``apply_per_topic_policies`` field is informational only.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PrivateTopicPolicy:
    """Operator-supplied policy for the SDK's per-query private output topics.

    Defaults match the Java SDK :class:`PrivateTopicPolicy.DEFAULT`.
    """

    message_ttl_seconds: int = 60
    retention_time_minutes: int = 1
    retention_size_mb: int = 8
    inactive_delete_seconds: int = 60
    delete_on_close: bool = True
    apply_per_topic_policies: bool = True


# Module-level default matching :class:`PrivateTopicPolicy.DEFAULT` in the Java SDK.
DEFAULT = PrivateTopicPolicy()
