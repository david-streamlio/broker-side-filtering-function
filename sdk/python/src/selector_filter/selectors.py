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

"""JMS-selector helpers.

The Java SDK validates JMS-selector syntax client-side via the
``io.streamnative:pulsar-jms-filters`` library so malformed selectors fail at the SDK call
site, not at the orchestrator or filter function. There is no equivalent Python parser today,
so this module currently performs only a light non-blank check; the orchestrator and filter
function still validate semantically.

If a Python user passes a malformed selector, they will see an asynchronous validation failure
on the orchestrator side rather than an immediate exception at register time. A future Phase
may add a real client-side JMS parser; for now, document this gap.
"""

from __future__ import annotations

from typing import Optional


def validate(selector: Optional[str]) -> None:
    """Light client-side selector check.

    A ``None`` or blank selector is allowed (interpreted as "match every record"). Non-blank
    selectors are not parsed; if you pass a syntactically malformed JMS selector the orchestrator
    will reject it asynchronously and your queryId will not register.

    Raises :class:`ValueError` only if ``selector`` is a non-string type other than ``None``.
    """
    if selector is None:
        return
    if not isinstance(selector, str):
        raise ValueError(f"selector must be a string or None, got {type(selector).__name__}")
