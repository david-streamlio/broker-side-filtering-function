"""Session model tests: determinism, the hot customer's fixed script, and the generator's --model flag."""

from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import orders_gen  # noqa: E402
from orders import FIRST_ORDER_ID, StreamConfig  # noqa: E402
from sessions import HOT_HISTORY, HOT_LIVE, SessionPayloadBuilder, SessionStream  # noqa: E402

HOT = 8_675_309


def _events(n=20_000, **overrides):
    # 300 events/s: 20k events cover a few minutes, long enough for the hot history (about 63 s).
    cfg = StreamConfig(events=n, event_rate=300, lifecycle_scale=1.0, hot_order_id=HOT, hot_at_s=12.0,
                       **overrides)
    stream = SessionStream(cfg)
    return [e for _, e in stream], stream


def test_same_seed_same_stream():
    a, _ = _events(seed=7)
    b, _ = _events(seed=7)
    assert a == b


def test_events_are_keyed_by_customer_with_contiguous_seqs():
    events, _ = _events()
    seqs = defaultdict(list)
    for e in events:
        assert e.key == str(e.customer_id) == e.properties()["customer_id"]
        seqs[e.customer_id].append(e.seq)
    for customer, s in seqs.items():
        assert s == list(range(len(s))), customer


def test_hot_customer_history_is_fixed_and_independent_of_seed():
    want = [t for t, _, _ in HOT_HISTORY]
    for seed in (1, 2):
        events, _ = _events(seed=seed)
        assert [e.event_type for e in events if e.customer_id == HOT] == want


def test_hot_history_shows_three_3ds_timeouts_then_chat():
    types = [t for t, _, _ in HOT_HISTORY]
    assert types.count("3ds_timeout") == 3
    assert types[-1] == "chat_opened"


def test_live_script_fails_once_then_completes():
    types = [t for t, _ in HOT_LIVE]
    assert types.index("3ds_timeout") < types.index("3ds_approved") < types.index("order_confirmed")
    assert types[-1] == "order_confirmed"


def test_event_types_fit_the_demo_column():
    # WindowDemo prints event types in a 17-character column.
    names = {t for t, _, _ in HOT_HISTORY} | {t for t, _ in HOT_LIVE}
    assert max(len(n) for n in names) <= 17


def test_hot_steps_stops_the_history_early():
    events, _ = _events(hot_steps=5)
    assert [e.event_type for e in events if e.customer_id == HOT] == [t for t, _, _ in HOT_HISTORY[:5]]


def test_background_customers_start_at_first_id():
    events, stream = _events()
    background = {e.customer_id for e in events if e.customer_id != HOT}
    assert min(background) == FIRST_ORDER_ID
    assert stream.stats.hot_events == len(HOT_HISTORY)


def test_payload_is_padded_and_carries_the_detail():
    events, _ = _events()
    hot = next(e for e in events if e.customer_id == HOT and e.event_type == "3ds_timeout")
    raw = SessionPayloadBuilder(seed=42).build(hot)
    body = json.loads(raw)
    assert len(raw) == 1024
    assert body["detail"]["reason"] == "challenge_not_approved"
    assert body["cart"]["total"] == 184.97


def test_generator_runs_the_sessions_model(tmp_path):
    args = orders_gen.parse_args(["--model", "sessions", "--sink", "null", "--events", "5000",
                                  "--out-dir", str(tmp_path)])
    manifest = orders_gen.run(args)
    assert manifest["events_sent"] == 5000
    assert "3ds_challenge" in manifest["by_event_type"]
