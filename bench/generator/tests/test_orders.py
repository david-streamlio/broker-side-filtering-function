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

"""Order-lifecycle model tests: determinism and the properties the measurements rely on."""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orders_gen  # noqa: E402
from orders import (FIRST_ORDER_ID, HOT_LIFECYCLE, OrderStream, PayloadBuilder,  # noqa: E402
                    StreamConfig)

DAY = 86_400


def _events(n=20_000, **overrides):
    cfg = StreamConfig(events=n, event_rate=n / DAY, **overrides)
    stream = OrderStream(cfg)
    return [e for _, e in stream], stream


def test_same_seed_same_stream():
    a, _ = _events(5_000, seed=7)
    b, _ = _events(5_000, seed=7)
    c, _ = _events(5_000, seed=8)
    assert a == b
    assert a != c


def test_stops_at_event_count():
    events, stream = _events(12_345)
    assert len(events) == 12_345
    assert stream.stats.events == 12_345


def test_per_order_seqs_are_contiguous_and_in_emit_order_without_late_events():
    events, _ = _events()
    seen = defaultdict(list)
    for e in events:
        seen[e.order_id].append(e.seq)
    for seqs in seen.values():
        assert seqs == list(range(len(seqs)))


def test_emit_order_matches_event_time_without_late_events():
    events, _ = _events()
    times = [e.event_time_ms for e in events]
    assert times == sorted(times)


def test_hot_order_has_fixed_lifecycle_independent_of_seed():
    lifecycles = []
    for seed in (1, 2):
        events, _ = _events(seed=seed, hot_at_s=DAY * 0.5)
        hot = [e for e in events if e.order_id == 8_675_309]
        lifecycles.append([(e.seq, e.event_type, e.event_time_ms) for e in hot])
    assert lifecycles[0] == lifecycles[1]
    assert [t for _, t, _ in lifecycles[0]] == [name for name, _ in HOT_LIFECYCLE]


def test_hot_steps_stops_the_hot_order_early():
    events, stream = _events(hot_at_s=DAY * 0.5, hot_steps=4)
    hot = [e for e in events if e.order_id == 8_675_309]
    assert [e.event_type for e in hot] == [name for name, _ in HOT_LIFECYCLE[:4]]
    assert stream.stats.hot_events == 4


def test_lifecycle_types_and_terminal_events():
    events, _ = _events(50_000)
    by_order = defaultdict(list)
    for e in events:
        by_order[e.order_id].append(e.event_type)
    types = {t for ts in by_order.values() for t in ts}
    assert {"created", "paid", "delivered", "cancelled"} <= types
    for ts in by_order.values():
        assert ts[0] == "created"
        if "cancelled" in ts:
            assert ts == ["created", "paid", "cancelled"]


def test_late_events_keep_event_time_but_arrive_out_of_order():
    events, stream = _events(late_fraction=0.05)
    late = [e for e in events if e.late]
    assert 0.03 < len(late) / len(events) < 0.07
    assert stream.stats.late == len(late)
    times = [e.event_time_ms for e in events]
    assert times != sorted(times)
    # Seqs are assigned in lifecycle order, so a late event can follow a later seq of its order.
    assert all(0 <= e.seq for e in late)


def test_hot_order_is_not_late_by_default():
    events, _ = _events(late_fraction=0.5, hot_at_s=DAY * 0.5)
    assert not any(e.late for e in events if e.order_id == 8_675_309)


def test_region_is_roughly_uniform_and_customers_are_skewed():
    events, stream = _events(50_000)
    regions = stream.stats.by_region
    assert len(regions) == 100
    assert max(regions.values()) < 2.5 * min(regions.values())
    top = max(stream.stats.by_customer.values())
    assert top > 20 * (len(events) / 50_000)  # the most popular customer is far above uniform


def test_per_order_counts_match_events():
    events, stream = _events(hot_at_s=DAY * 0.5)
    counts = defaultdict(int)
    for e in events:
        counts[e.order_id] += 1
    assert stream.stats.hot_events == counts.pop(8_675_309)
    tracked = {FIRST_ORDER_ID + i: n for i, n in enumerate(stream.stats.per_order) if n}
    assert tracked == dict(counts)


def test_payload_is_json_near_target_size_and_deterministic():
    events, _ = _events(200)
    a = [PayloadBuilder(42, 1024).build(e) for e in events]
    b = [PayloadBuilder(42, 1024).build(e) for e in events]
    assert a == b
    for raw, e in zip(a, events):
        body = json.loads(raw)
        assert body["order_id"] == e.order_id and body["seq"] == e.seq
        assert 1000 <= len(raw) <= 1100


def test_properties_are_strings():
    events, _ = _events(100)
    for e in events:
        assert all(isinstance(v, str) for v in e.properties().values())
        assert e.key == e.properties()["order_id"]


def test_null_sink_run_writes_manifest_and_expected_counts(tmp_path, capsys):
    args = orders_gen.parse_args(["--sink", "null", "--events", "3000", "--out-dir", str(tmp_path),
                                  "--dump", "2", "--hot-at", "0.1"])
    manifest = orders_gen.run(args)
    assert manifest["events_sent"] == manifest["events_acked"] == 3_000
    assert manifest["send_errors"] == 0
    assert [h["event_type"] for h in manifest["hot_order"]["events"]][:1] == ["created"]
    with open(manifest["expected_counts_csv"]) as f:
        rows = f.read().strip().splitlines()[1:]
    assert sum(int(r.split(",")[1]) for r in rows) == 3_000
    dumped = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    assert len(dumped) == 2 and "properties" in dumped[0]


def test_parse_duration():
    assert orders_gen.parse_duration("90s") == 90
    assert orders_gen.parse_duration("15m") == 900
    assert orders_gen.parse_duration("24h") == DAY
    assert orders_gen.parse_duration("1.5d") == 1.5 * DAY
