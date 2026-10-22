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

"""Deterministic order-lifecycle event model (no I/O).

Produces a stream of order events in *emit order*: the order a producer would send them.
Every order walks a lifecycle (created -> paid -> picked -> packed -> shipped ->
out_for_delivery -> delivered, with some cancellations and returns), each step a simulated
delay after the previous one. Many orders are in flight at once, so the stream interleaves
them the way a real order system would.

Determinism: the same parameters and seed produce the same events in the same order. Only
the wall-clock send times differ between runs. The log's own clock (broker receive time)
can't be set by a client, so simulated time lives in ``event_time_ms``.

Selectivity knobs, for the windowed-read measurements:

* ``order_id`` (message key and property): a handful of events per order. The hot order
  (default 8675309) has a fixed, seed-independent lifecycle of 7 events.
* ``customer_id``: Zipf-distributed, so a few customers have many orders.
* ``region``: uniform over ``regions`` values, so ``region = 'r07'`` selects ~1% at 100 regions.
"""

from __future__ import annotations

import bisect
import heapq
import json
import random
from array import array
from dataclasses import dataclass, field
from typing import Iterator, List, Optional, Tuple

# (event_type, min delay s, max delay s) after the previous step, at lifecycle_scale = 1.0.
LIFECYCLE: List[Tuple[str, int, int]] = [
    ("created", 0, 0),
    ("paid", 60, 600),
    ("picked", 600, 7_200),
    ("packed", 300, 3_600),
    ("shipped", 600, 7_200),
    ("out_for_delivery", 4 * 3_600, 24 * 3_600),
    ("delivered", 1_800, 6 * 3_600),
]
CANCEL_AFTER = "paid"
CANCEL_PROBABILITY = 0.03
RETURN_PROBABILITY = 0.05
RETURN_DELAY_S = (24 * 3_600, 5 * 24 * 3_600)

# The hot order's lifecycle: fixed delays (s) so its events are identical in every run.
HOT_LIFECYCLE: List[Tuple[str, int]] = [
    ("created", 0),
    ("paid", 240),
    ("picked", 2_100),
    ("packed", 1_200),
    ("shipped", 2_700),
    ("out_for_delivery", 3_600),
    ("delivered", 1_500),
]
HOT_CUSTOMER_ID = 104_242
HOT_REGION = "r42"

# Rough number of events an order emits within a run; used to turn a target event rate
# into an order-arrival rate. Runs stop on the event count, so this only shapes how much
# simulated time the run covers.
EVENTS_PER_ORDER = 5.5

FIRST_ORDER_ID = 10_000_000

_CITIES = ["Austin", "Berlin", "Chicago", "Denver", "Lisbon", "Madrid", "Oslo", "Paris",
           "Seattle", "Tokyo", "Toronto", "Zurich"]
_STREETS = ["Elm St", "Main St", "Oak Ave", "Pine Rd", "Harbor Way", "Market St", "Hill Rd"]
_CARRIERS = ["ups", "fedex", "dhl", "usps", "postnl"]


@dataclass(frozen=True)
class OrderEvent:
    order_id: int
    customer_id: int
    region: str
    seq: int
    event_type: str
    event_time_ms: int
    late: bool = False

    def properties(self) -> dict:
        """Message properties (Pulsar) / headers (Kafka). Values are strings, as Pulsar requires."""
        return {
            "order_id": str(self.order_id),
            "customer_id": str(self.customer_id),
            "region": self.region,
            "event_type": self.event_type,
            "seq": str(self.seq),
            "event_time": str(self.event_time_ms),
        }

    @property
    def key(self) -> str:
        return str(self.order_id)


@dataclass
class _Order:
    order_id: int
    customer_id: int
    region: str
    next_step: int = 0
    next_seq: int = 0
    hot: bool = False
    pending: Optional[str] = None  # one-off terminal event ("cancelled" / "returned")


@dataclass
class StreamConfig:
    seed: int = 42
    events: Optional[int] = 100_000          # None = unbounded (realtime mode)
    event_rate: float = 100_000 / 86_400     # target events per simulated second
    sim_start_ms: int = 0
    lifecycle_scale: float = 1.0
    customers: int = 50_000
    zipf_s: float = 0.8
    regions: int = 100
    hot_order_id: Optional[int] = 8_675_309
    hot_at_s: float = 0.0                    # simulated time the hot order is created
    hot_steps: Optional[int] = None          # emit only the first N hot lifecycle steps (None = all)
    late_fraction: float = 0.0
    late_delay_s: Tuple[int, int] = (30, 900)
    hot_order_late: bool = False


@dataclass
class StreamStats:
    events: int = 0
    orders: int = 0
    late: int = 0
    by_type: dict = field(default_factory=dict)
    by_region: dict = field(default_factory=dict)
    by_customer: dict = field(default_factory=dict)
    # Events emitted per regular order, indexed by order_id - FIRST_ORDER_ID. Seqs are
    # contiguous from 0, so a consumer that saw every event of an order saw seqs 0..count-1.
    per_order: array = field(default_factory=lambda: array("H"))
    hot_events: int = 0
    last_sim_s: float = 0.0


class OrderStream:
    """Iterate ``(emit_time_s, OrderEvent)`` pairs in emit order.

    ``emit_time_s`` is simulated seconds since the start of the run. In bulk mode the caller
    ignores it and sends as fast as allowed; in realtime mode the caller sleeps until the
    wall clock reaches it.
    """

    def __init__(self, cfg: StreamConfig):
        self.cfg = cfg
        self.stats = StreamStats()
        self._rng = random.Random(cfg.seed)
        self._heap: list = []
        self._tiebreak = 0
        self._next_order_id = FIRST_ORDER_ID
        self._arrival_rate = cfg.event_rate / EVENTS_PER_ORDER  # orders per simulated second
        self._zipf_cdf = _zipf_cdf(cfg.customers, cfg.zipf_s)
        self._push(0.0, "arrival", None)
        if cfg.hot_order_id is not None:
            self._push(cfg.hot_at_s, "hot", None)

    def __iter__(self) -> Iterator[Tuple[float, OrderEvent]]:
        limit = self.cfg.events
        while self._heap and (limit is None or self.stats.events < limit):
            t, _, kind, item = heapq.heappop(self._heap)
            if kind == "arrival":
                self._start_order(t, self._new_order())
                self._push(t + self._rng.expovariate(self._arrival_rate), "arrival", None)
            elif kind == "hot":
                order = _Order(self.cfg.hot_order_id, HOT_CUSTOMER_ID, HOT_REGION, hot=True)
                self.stats.orders += 1
                self._start_order(t, order)
            elif kind == "step":
                event = self._step(t, item)
                if self._is_late(item):
                    # Keep the event time, emit it later: a late arrival.
                    delay = self._rng.uniform(*self.cfg.late_delay_s) * self.cfg.lifecycle_scale
                    self._push(t + delay, "late", _replace_late(event))
                    continue
                yield t, self._count(t, event)
            elif kind == "late":
                yield t, self._count(t, item)

    # ---------- lifecycle ----------

    def _new_order(self) -> _Order:
        order_id = self._next_order_id
        self._next_order_id += 1
        self.stats.orders += 1
        customer = 100_000 + bisect.bisect_left(self._zipf_cdf, self._rng.random())
        region = f"r{self._rng.randrange(self.cfg.regions):02d}"
        return _Order(order_id, customer, region)

    def _start_order(self, t: float, order: _Order) -> None:
        self._push(t, "step", order)

    def _step(self, t: float, order: _Order) -> OrderEvent:
        """Emit the order's current step and schedule the next one."""
        if order.pending is not None:
            event_type, order.pending = order.pending, None
            next_delay = None
        else:
            event_type = (HOT_LIFECYCLE if order.hot else LIFECYCLE)[order.next_step][0]
            order.next_step += 1
            next_delay = self._next_delay(order, event_type)

        event = OrderEvent(order.order_id, order.customer_id, order.region, order.next_seq,
                           event_type, self.cfg.sim_start_ms + int(t * 1000))
        order.next_seq += 1
        if next_delay is not None:
            self._push(t + next_delay, "step", order)
        return event

    def _next_delay(self, order: _Order, event_type: str) -> Optional[float]:
        """Delay until the order's next event, or None when the lifecycle is over."""
        scale = self.cfg.lifecycle_scale
        if order.hot:
            if order.next_step >= (self.cfg.hot_steps or len(HOT_LIFECYCLE)):
                return None
            return HOT_LIFECYCLE[order.next_step][1] * scale
        if event_type == CANCEL_AFTER and self._rng.random() < CANCEL_PROBABILITY:
            order.pending = "cancelled"
            return self._rng.uniform(60, 3_600) * scale
        if order.next_step >= len(LIFECYCLE):
            if event_type == "delivered" and self._rng.random() < RETURN_PROBABILITY:
                order.pending = "returned"
                return self._rng.uniform(*RETURN_DELAY_S) * scale
            return None
        _, lo, hi = LIFECYCLE[order.next_step]
        return self._rng.uniform(lo, hi) * scale

    # ---------- helpers ----------

    def _is_late(self, order: _Order) -> bool:
        if self.cfg.late_fraction <= 0 or (order.hot and not self.cfg.hot_order_late):
            return False
        return self._rng.random() < self.cfg.late_fraction

    def _count(self, t: float, event: OrderEvent) -> OrderEvent:
        s = self.stats
        s.events += 1
        s.late += event.late
        s.last_sim_s = t
        s.by_type[event.event_type] = s.by_type.get(event.event_type, 0) + 1
        s.by_region[event.region] = s.by_region.get(event.region, 0) + 1
        s.by_customer[event.customer_id] = s.by_customer.get(event.customer_id, 0) + 1
        idx = event.order_id - FIRST_ORDER_ID
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


def _replace_late(event: OrderEvent) -> OrderEvent:
    return OrderEvent(event.order_id, event.customer_id, event.region, event.seq,
                      event.event_type, event.event_time_ms, late=True)


def _zipf_cdf(n: int, s: float) -> List[float]:
    weights = [1.0 / (k ** s) for k in range(1, n + 1)]
    total = sum(weights)
    cdf, acc = [], 0.0
    for w in weights:
        acc += w / total
        cdf.append(acc)
    cdf[-1] = 1.0
    return cdf


class PayloadBuilder:
    """Builds a ~``target_bytes`` JSON payload for an event, deterministically from the seed."""

    def __init__(self, seed: int, target_bytes: int = 1024):
        self._rng = random.Random(seed ^ 0x5EED)
        self.target_bytes = target_bytes

    def build(self, event: OrderEvent) -> bytes:
        rng = self._rng
        items = [{"sku": f"SKU-{rng.randrange(100_000):05d}", "qty": rng.randint(1, 4),
                  "unit_price": round(rng.uniform(2, 400), 2)}
                 for _ in range(rng.randint(1, 5))]
        body = {
            "order_id": event.order_id,
            "customer_id": event.customer_id,
            "region": event.region,
            "seq": event.seq,
            "event_type": event.event_type,
            "event_time": event.event_time_ms,
            "currency": "USD",
            "total": round(sum(i["qty"] * i["unit_price"] for i in items), 2),
            "items": items,
            "ship_to": {"street": f"{rng.randint(1, 9999)} {rng.choice(_STREETS)}",
                        "city": rng.choice(_CITIES), "postal_code": f"{rng.randrange(100_000):05d}"},
            "carrier": rng.choice(_CARRIERS),
            "tracking": f"TRK{rng.randrange(10 ** 12):012d}",
        }
        raw = json.dumps(body, separators=(",", ":"))
        pad = self.target_bytes - len(raw) - len(',"notes":""')
        if pad > 0:
            body["notes"] = "x" * pad
            raw = json.dumps(body, separators=(",", ":"))
        return raw.encode()
