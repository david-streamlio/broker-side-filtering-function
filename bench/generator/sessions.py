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

"""Deterministic shopping-session event model (no I/O), for the checkout-troubleshooting demo.

Produces a stream of session events in *emit order*, keyed by customer: each shopper's session
walks a journey (browse, cart, checkout, payment with a 3-D Secure challenge, the occasional
decline), a few seconds between steps. Many sessions are in flight at once, so the stream
interleaves them the way a real e-commerce event stream would.

The hot customer (default 8675309) has a fixed, seed-independent history: checkout fails three
times on a 3-D Secure timeout, then they open the support chat. ``HOT_LIVE`` is what happens next,
sent live by the stage demo (``demo/window``) once the agent is watching: one more timeout, then
the customer approves the challenge in their bank app and the order goes through.

The stream has the same interface as ``orders.OrderStream`` (``(emit_time_s, event)`` pairs, the
same ``StreamStats``), so ``orders_gen.py`` runs either model with ``--model``. For that reason a
``SessionEvent``'s entity id is exposed as ``order_id``: here it's the customer id, the message key.
"""

from __future__ import annotations

import heapq
import json
import random
from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple

from orders import FIRST_ORDER_ID, StreamConfig, StreamStats

# Journeys a background session can take: (weight, steps). Each step is (event_type, detail).
_BROWSE = [("session_start", {}), ("page_view", {"path": "/"}), ("search", {}), ("product_view", {}),
           ("product_view", {}), ("session_end", {})]
_CART = _BROWSE[:-1] + [("add_to_cart", {}), ("view_cart", {}), ("session_end", {})]
_CHECKOUT = _CART[:-1] + [("checkout_started", {}), ("shipping_selected", {}), ("payment_submitted", {}),
                          ("3ds_challenge", {}), ("3ds_approved", {}), ("payment_approved", {}),
                          ("order_placed", {}), ("order_confirmed", {}), ("session_end", {})]
_DECLINED = _CART[:-1] + [("checkout_started", {}), ("payment_submitted", {}),
                          ("card_declined", {"reason": "insufficient_funds"}), ("session_end", {})]
JOURNEYS: List[Tuple[float, list]] = [(0.45, _BROWSE), (0.25, _CART), (0.25, _CHECKOUT), (0.05, _DECLINED)]
STEP_DELAY_S = (2, 40)   # between a session's steps, at lifecycle_scale = 1.0
EVENTS_PER_SESSION = 9.0

HOT_REGION = "r42"
HOT_SESSION = "s-8675309-01"
HOT_PLATFORM = "ios"
HOT_APP_VERSION = "5.2.0"
HOT_CART = [{"sku": "SKU-04417", "name": "Trail running shoes", "qty": 1, "unit_price": 129.99},
            {"sku": "SKU-11873", "name": "Running socks, 3-pack", "qty": 1, "unit_price": 54.98}]

# The hot customer's history: (event_type, delay s after the previous step, detail). Fixed, so the
# demo's events are identical in every run. Three checkout attempts time out on 3-D Secure.
_ATTEMPT_TIMEOUT = [("payment_submitted", 3, {"method": "visa", "last4": "4242"}),
                    ("3ds_challenge", 1, {"challenge": "bank_app_push"}),
                    ("3ds_timeout", 5, {"reason": "challenge_not_approved", "after_s": 120})]
HOT_HISTORY: List[Tuple[str, float, dict]] = [
    ("session_start", 0, {"path": "/"}),
    ("search", 2, {"query": "trail running shoes"}),
    ("product_view", 2, {"sku": "SKU-04417"}),
    ("add_to_cart", 2, {"sku": "SKU-04417"}),
    ("product_view", 2, {"sku": "SKU-11873"}),
    ("add_to_cart", 2, {"sku": "SKU-11873"}),
    ("view_cart", 2, {}),
    ("checkout_started", 2, {}),
    ("shipping_selected", 2, {"method": "standard"}),
    *_ATTEMPT_TIMEOUT,
    ("checkout_started", 3, {}),
    *_ATTEMPT_TIMEOUT,
    ("checkout_started", 3, {}),
    *_ATTEMPT_TIMEOUT,
    ("chat_opened", 2, {"channel": "in_app_support"}),
]

# What happens once the agent is watching (sent by the stage demo, a few seconds apart): one more
# timeout, the agent tells the customer to approve the prompt in their bank app, and it goes through.
HOT_LIVE: List[Tuple[str, dict]] = [
    ("checkout_started", {}),
    ("payment_submitted", {"method": "visa", "last4": "4242"}),
    ("3ds_challenge", {"challenge": "bank_app_push"}),
    ("3ds_timeout", {"reason": "challenge_not_approved", "after_s": 120}),
    ("payment_submitted", {"method": "visa", "last4": "4242"}),
    ("3ds_challenge", {"challenge": "bank_app_push"}),
    ("3ds_approved", {"approved_in": "bank_app"}),
    ("payment_approved", {"amount": 184.97}),
    ("order_placed", {"order_id": "ORD-20261105-0042"}),
    ("order_confirmed", {"order_id": "ORD-20261105-0042"}),
]

_PLATFORMS = ["web", "web", "ios", "android"]
_QUERIES = ["running shoes", "rain jacket", "headphones", "coffee grinder", "yoga mat", "backpack",
            "desk lamp", "water bottle"]


@dataclass(frozen=True)
class SessionEvent:
    customer_id: int
    session_id: str
    region: str
    platform: str
    seq: int
    event_type: str
    event_time_ms: int
    detail: Optional[dict] = None
    late: bool = False

    @property
    def order_id(self) -> int:
        """The entity id, named as ``orders.OrderEvent``'s so the generator handles both models."""
        return self.customer_id

    @property
    def key(self) -> str:
        return str(self.customer_id)

    def properties(self) -> dict:
        """Message properties (Pulsar) / headers (Kafka). Values are strings, as Pulsar requires."""
        return {
            "customer_id": str(self.customer_id),
            "session_id": self.session_id,
            "region": self.region,
            "platform": self.platform,
            "event_type": self.event_type,
            "seq": str(self.seq),
            "event_time": str(self.event_time_ms),
        }


@dataclass
class _Session:
    customer_id: int
    session_id: str
    region: str
    platform: str
    steps: list
    hot: bool = False
    next_step: int = 0


def hot_event(customer_id: int, seq: int, event_type: str, event_time_ms: int, detail: dict) -> SessionEvent:
    """One of the hot customer's events (history or live), in the generator's format."""
    return SessionEvent(customer_id, HOT_SESSION, HOT_REGION, HOT_PLATFORM, seq, event_type, event_time_ms, detail)


class SessionStream:
    """Iterate ``(emit_time_s, SessionEvent)`` pairs in emit order (see ``orders.OrderStream``)."""

    def __init__(self, cfg: StreamConfig):
        self.cfg = cfg
        self.stats = StreamStats()
        self._rng = random.Random(cfg.seed)
        self._heap: list = []
        self._tiebreak = 0
        self._next_customer = FIRST_ORDER_ID
        self._arrival_rate = cfg.event_rate / EVENTS_PER_SESSION  # sessions per simulated second
        self._weights = [w for w, _ in JOURNEYS]
        self._push(0.0, "arrival", None)
        if cfg.hot_order_id is not None:
            self._push(cfg.hot_at_s, "hot", None)

    def __iter__(self) -> Iterator[Tuple[float, SessionEvent]]:
        limit = self.cfg.events
        while self._heap and (limit is None or self.stats.events < limit):
            t, _, kind, item = heapq.heappop(self._heap)
            if kind == "arrival":
                self._push(t, "step", self._new_session())
                self._push(t + self._rng.expovariate(self._arrival_rate), "arrival", None)
            elif kind == "hot":
                steps = HOT_HISTORY[: self.cfg.hot_steps or len(HOT_HISTORY)]
                session = _Session(self.cfg.hot_order_id, HOT_SESSION, HOT_REGION, HOT_PLATFORM, steps, hot=True)
                self.stats.orders += 1
                self._push(t, "step", session)
            elif kind == "step":
                yield t, self._count(t, self._step(t, item))

    def _new_session(self) -> _Session:
        customer = self._next_customer
        self._next_customer += 1
        self.stats.orders += 1
        _, steps = self._rng.choices(JOURNEYS, weights=self._weights)[0]
        return _Session(customer, f"s-{customer}-01", f"r{self._rng.randrange(self.cfg.regions):02d}",
                        self._rng.choice(_PLATFORMS), steps)

    def _step(self, t: float, s: _Session) -> SessionEvent:
        """Emit the session's current step and schedule the next one."""
        if s.hot:
            event_type, _, detail = s.steps[s.next_step]
        else:
            event_type, detail = s.steps[s.next_step]
            if event_type == "search":
                detail = {"query": self._rng.choice(_QUERIES)}
        event = SessionEvent(s.customer_id, s.session_id, s.region, s.platform, s.next_step, event_type,
                             self.cfg.sim_start_ms + int(t * 1000), detail or None)
        s.next_step += 1
        if s.next_step < len(s.steps):
            delay = s.steps[s.next_step][1] if s.hot else self._rng.uniform(*STEP_DELAY_S)
            self._push(t + delay * self.cfg.lifecycle_scale, "step", s)
        return event

    def _count(self, t: float, event: SessionEvent) -> SessionEvent:
        s = self.stats
        s.events += 1
        s.last_sim_s = t
        s.by_type[event.event_type] = s.by_type.get(event.event_type, 0) + 1
        s.by_region[event.region] = s.by_region.get(event.region, 0) + 1
        s.by_customer[event.customer_id] = s.by_customer.get(event.customer_id, 0) + 1
        idx = event.customer_id - FIRST_ORDER_ID
        if idx < 0:
            s.hot_events += 1
        else:
            while len(s.per_order) <= idx:
                s.per_order.append(0)
            s.per_order[idx] += 1
        return event

    def _push(self, t: float, kind: str, item) -> None:
        self._tiebreak += 1
        heapq.heappush(self._heap, (t, self._tiebreak, kind, item))


class SessionPayloadBuilder:
    """Builds a ~``target_bytes`` JSON payload for a session event, deterministically from the seed.

    A session's device and cart come from its customer id, so its events agree on them. ``_pad``
    fills to the target size, like ``orders.PayloadBuilder``.
    """

    def __init__(self, seed: int, target_bytes: int = 1024):
        self._seed = seed ^ 0x5E55
        self.target_bytes = target_bytes

    def build(self, event: SessionEvent) -> bytes:
        rng = random.Random(self._seed * 1_000_003 + event.customer_id)
        if event.session_id == HOT_SESSION:
            cart, app_version = HOT_CART, HOT_APP_VERSION
        else:
            cart = [{"sku": f"SKU-{rng.randrange(100_000):05d}", "qty": rng.randint(1, 3),
                     "unit_price": round(rng.uniform(5, 300), 2)} for _ in range(rng.randint(1, 4))]
            app_version = f"5.{rng.randint(0, 2)}.{rng.randint(0, 9)}"
        body = {
            "customer_id": event.customer_id,
            "session_id": event.session_id,
            "seq": event.seq,
            "event_type": event.event_type,
            "event_time": event.event_time_ms,
            "platform": event.platform,
            "app_version": app_version if event.platform != "web" else None,
            "region": event.region,
            "cart": {"items": cart, "total": round(sum(i["qty"] * i["unit_price"] for i in cart), 2),
                     "currency": "USD"},
            "detail": event.detail or {},
        }
        raw = json.dumps(body, separators=(",", ":"))
        pad = self.target_bytes - len(raw) - len(',"_pad":""')
        if pad > 0:
            body["_pad"] = "x" * pad
            raw = json.dumps(body, separators=(",", ":"))
        return raw.encode()
