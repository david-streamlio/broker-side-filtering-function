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
