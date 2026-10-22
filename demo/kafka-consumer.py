#!/usr/bin/env python3
"""
Python Kafka consumer for a broker-side filtered output topic.

Usage:
    python3 demo/kafka-consumer.py <topic>

    Where <topic> is the short Kafka topic name, e.g.:
        live-demo-daa22dd3
        modify-demo-b4128ee2

    Get the topic from live-query-demo.sh output or:
        http://localhost:3200  (Overview → click any query)

Requirements:
    pip install confluent-kafka python-dotenv
"""

import sys
import os
import json
import time
import statistics
from pathlib import Path

try:
    from confluent_kafka import Consumer, KafkaException
except ImportError:
    print("Install confluent-kafka:  pip install confluent-kafka")
    sys.exit(1)

# ── Configuration ─────────────────────────────────────────────────────────────

DEMO_DIR = Path(__file__).parent

# Load .env if present
env_file = DEMO_DIR / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

BOOTSTRAP    = os.environ["KAFKA_BOOTSTRAP"]
USERNAME     = os.environ.get("KAFKA_USERNAME", "stock-demo-app")
TOKEN_PATH   = os.environ.get("PULSAR_TOKEN_PATH", "")
TOKEN        = Path(TOKEN_PATH).read_text().strip() if TOKEN_PATH else ""

# ── Consumer config ───────────────────────────────────────────────────────────

def make_consumer(topic: str) -> Consumer:
    conf = {
        "bootstrap.servers":  BOOTSTRAP,
        "group.id":           f"python-demo-{int(time.time())}",  # fresh group each run
        "auto.offset.reset":  "latest",    # only new messages from now
        "enable.auto.commit": True,
    }

    if TOKEN:
        conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism":    "PLAIN",
            "sasl.username":     USERNAME,
            "sasl.password":     f"token:{TOKEN}",
        })
    else:
        conf["security.protocol"] = "PLAINTEXT"

    c = Consumer(conf)
    c.subscribe([topic])
    return c

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    # Accept both short name and full Pulsar FQN
    raw = sys.argv[1]
    topic = raw.replace("persistent://public/default/", "")

    print(f"╔══════════════════════════════════════════════════════╗")
    print(f"  Python Kafka Consumer — Broker-Side Filtered Topic")
    print(f"╠══════════════════════════════════════════════════════╣")
    print(f"  Bootstrap: {BOOTSTRAP}")
    print(f"  Username:  {USERNAME}")
    print(f"  Topic:     {topic}")
    print(f"╚══════════════════════════════════════════════════════╝")
    print()

    from datetime import datetime, timezone

    timeout_s = int(sys.argv[2]) if len(sys.argv) > 2 else 15

    consumer = make_consumer(topic)
    print(f"Subscribed. Running for {timeout_s}s then printing stats... (Ctrl-C to stop early)\n")
    print(f"  {'#':>5}  {'produce_time':<15} {'receive_time':<15} {'transit':>8}  {'key':<8}  {'properties':<40}  payload")
    print(f"  {'-'*5}  {'-'*15} {'-'*15} {'-'*8}  {'-'*8}  {'-'*40}  -------")

    count = 0
    t_first = None
    t_start = time.time()
    transit_samples = []   # all transit_ms values for percentile computation

    try:
        while time.time() - t_start < timeout_s:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            count += 1
            now_s  = time.time()
            now_ms = int(now_s * 1000)
            if t_first is None:
                t_first = now_s

            # ── Timestamps ────────────────────────────────────────────────
            ts_type, ts_ms = msg.timestamp()
            produce_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%H:%M:%S.%f")[:15] if ts_ms > 0 else "—"
            receive_dt = datetime.fromtimestamp(now_s, tz=timezone.utc).strftime("%H:%M:%S.%f")[:15]
            transit_ms = now_ms - ts_ms if ts_ms > 0 else -1
            transit    = f"{transit_ms}ms" if transit_ms >= 0 else "—"
            if transit_ms >= 0:
                transit_samples.append(transit_ms)

            # ── Key ───────────────────────────────────────────────────────
            key = msg.key().decode("utf-8", errors="replace") if msg.key() else "(no key)"

            # ── Headers / Properties ──────────────────────────────────────
            headers = {}
            for h_key, h_val in (msg.headers() or []):
                if h_val is not None and not h_key.startswith("__ksn"):
                    headers[h_key] = h_val.decode("utf-8", errors="replace")
            props_str = ", ".join(f"{k}={v}" for k, v in headers.items()) if headers else "(none)"

            # ── Payload ───────────────────────────────────────────────────
            try:
                payload = json.loads(msg.value().decode())
                parts = []
                for field in ["seq", "price", "volume", "bid", "ask", "side", "exchange", "sector"]:
                    if field in payload:
                        v = payload[field]
                        parts.append(f"{field}={v:.2f}" if isinstance(v, float) else f"{field}={v}")
                payload_str = "  ".join(parts[:5])
            except Exception:
                payload_str = (msg.value() or b"").decode("utf-8", errors="replace")[:60]

            print(f"  [{count:>5}]  {produce_dt:<15} {receive_dt:<15} {transit:>8}  {key:<8}  {props_str:<40}  {payload_str}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    # ── Latency statistics ────────────────────────────────────────────────────
    elapsed = int((time.time() - t_start) * 1000)
    print()
    print(f"╔══════════════════════════════════════════════╗")
    print(f"  End-to-end transit latency  ({timeout_s}s window)")
    print(f"╠══════════════════════════════════════════════╣")
    print(f"  Messages received : {count}")
    print(f"  Window duration   : {elapsed}ms")
    if transit_samples:
        s = sorted(transit_samples)
        n = len(s)
        def pct(p): return s[min(int(n * p / 100), n - 1)]
        print(f"  Samples           : {n}")
        print(f"  Min               : {s[0]}ms")
        print(f"  p50 (median)      : {pct(50)}ms")
        print(f"  p90               : {pct(90)}ms")
        print(f"  p99               : {pct(99)}ms")
        print(f"  Max               : {s[-1]}ms")
        print(f"  Mean              : {int(statistics.mean(s))}ms")
    else:
        print(f"  No timestamp data available")
    print(f"╚══════════════════════════════════════════════╝")
    print(f"  Consumer closed.")

if __name__ == "__main__":
    main()
