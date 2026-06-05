#!/usr/bin/env python3
"""
Local Kafka producer — sends stock-quote events to the stock-quotes topic
with key=LOCAL, so they can be filtered separately from ShadowTraffic.

Use this to show low-latency filtering when ShadowTraffic's network distance
to SN Cloud inflates the transit time. A local producer sending from the same
machine as this consumer will show near-zero network overhead.

Usage:
    python3 demo/local-producer.py [msg_per_sec]
    python3 demo/local-producer.py 10    # 10 msg/s (default)
    python3 demo/local-producer.py 50    # faster

Then in a second terminal, subscribe to the filtered output:
    ./demo/live-query-demo.sh "__key__ IN ('LOCAL')"
    python3 demo/kafka-consumer.py live-demo-xxxxx 15

Requirements:
    pip install confluent-kafka
"""

import sys
import os
import json
import time
import random
from pathlib import Path
from datetime import datetime, timezone

try:
    from confluent_kafka import Producer, KafkaException
except ImportError:
    print("Install confluent-kafka:  pip install confluent-kafka")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────

DEMO_DIR = Path(__file__).parent
env_file = DEMO_DIR / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

BOOTSTRAP  = os.environ["KAFKA_BOOTSTRAP"]
USERNAME   = os.environ.get("KAFKA_USERNAME", "stock-demo-app")
TOKEN_PATH = os.environ.get("PULSAR_TOKEN_PATH", "")
TOKEN      = Path(TOKEN_PATH).read_text().strip() if TOKEN_PATH else ""
TOPIC      = "stock-quotes"   # KoP short name for persistent://public/default/stock-quotes

SECTORS   = ["tech", "finance", "healthcare", "consumer", "energy", "industrial"]
EXCHANGES = ["NYSE", "NASDAQ"]
SIDES     = ["B", "S"]

def make_producer() -> Producer:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "acks":              "1",
        "enable.idempotence": False,
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
    return Producer(conf)

def make_message(seq: int) -> dict:
    price  = round(random.uniform(50, 500), 2)
    return {
        "seq":      seq,
        "exchange": random.choice(EXCHANGES),
        "sector":   random.choice(SECTORS),
        "price":    price,
        "bid":      round(price * random.uniform(0.98, 1.0), 2),
        "ask":      round(price * random.uniform(1.0, 1.02), 2),
        "bidSize":  random.choice([100, 500, 1000]),
        "askSize":  random.choice([100, 500, 1000]),
        "spread":   round(random.uniform(0.01, 0.50), 4),
        "volume":   random.randint(100_000, 50_000_000),
        "side":     random.choice(SIDES),
    }

def main():
    rate     = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    delay_s  = 1.0 / rate

    producer = make_producer()
    seq = 0

    print(f"╔══════════════════════════════════════════════════╗")
    print(f"  Local Kafka Producer → {TOPIC}")
    print(f"╠══════════════════════════════════════════════════╣")
    print(f"  Bootstrap : {BOOTSTRAP}")
    print(f"  Key       : LOCAL  (filter with __key__ IN ('LOCAL'))")
    print(f"  Rate      : {rate} msg/s")
    print(f"╚══════════════════════════════════════════════════╝")
    print()
    print("  Producing... (Ctrl-C to stop)")
    print()

    headers = []   # will be set per message

    try:
        while True:
            seq += 1
            msg     = make_message(seq)
            payload = json.dumps(msg).encode()
            now_ms  = int(time.time() * 1000)

            # Kafka headers become Pulsar message properties → filterable by JMS
            headers = [
                ("exchange", msg["exchange"].encode()),
                ("sector",   msg["sector"].encode()),
                ("side",     msg["side"].encode()),
            ]

            producer.produce(
                topic     = TOPIC,
                key       = "LOCAL",
                value     = payload,
                headers   = headers,
                timestamp = now_ms,
            )
            producer.poll(0)

            ts = datetime.now(tz=timezone.utc).strftime("%H:%M:%S.%f")[:12]
            print(f"  [{seq:>6}] {ts}  key=LOCAL  price={msg['price']:.2f}  "
                  f"exchange={msg['exchange']}  sector={msg['sector']}  side={msg['side']}")

            time.sleep(delay_s)

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"\n  Produced {seq} messages. Producer closed.")

if __name__ == "__main__":
    main()
