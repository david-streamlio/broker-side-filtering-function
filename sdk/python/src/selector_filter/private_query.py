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
