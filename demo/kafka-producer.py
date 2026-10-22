#!/usr/bin/env python3
"""
Python Kafka producer for the demo SOURCE topic (broker-side filtering input).

Produces stock-quote messages matching the shadowtraffic schema
(shadowtraffic/stock-quotes.json) into:

    persistent://public/default/stock-quotes

Every message produced here carries the Kafka key "LOCAL" (overridable) so
these manually-produced records are trivially distinguishable from the
shadowtraffic-generated traffic (which keys by ticker, e.g. AAPL) when you
watch a filtered output topic with kafka-consumer.py.

Usage:
    python3 demo/kafka-producer.py                 # 10 msgs, key=LOCAL, to stock-quotes
    python3 demo/kafka-producer.py -n 100          # 100 messages
    python3 demo/kafka-producer.py -n 0            # produce forever (Ctrl-C to stop)
    python3 demo/kafka-producer.py --key MYTAG     # custom key
    python3 demo/kafka-producer.py --topic public/default/stock-quotes
    python3 demo/kafka-producer.py --throttle-ms 200

Then, in another terminal, watch a filtered output topic:
    python3 demo/kafka-consumer.py public/default/modify-demo-xxxxxxxx

Requirements:
    pip install confluent-kafka python-dotenv

Connection is read from demo/.env (KAFKA_BOOTSTRAP, KAFKA_USERNAME,
PULSAR_TOKEN_PATH) — identical wiring to kafka-consumer.py.
"""

import sys
import os
import json
import time
import random
import argparse
from pathlib import Path

try:
    from confluent_kafka import Producer
except ImportError:
    print("Install confluent-kafka:  pip install confluent-kafka")
    sys.exit(1)

# ── Configuration ─────────────────────────────────────────────────────────────

DEMO_DIR = Path(__file__).parent

# Load .env if present (same loader as kafka-consumer.py)
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

DEFAULT_TOPIC = os.environ.get(
    "DEMO_SOURCE_TOPIC_STOCKS", "persistent://public/default/stock-quotes"
)

# ── Message generation — mirrors shadowtraffic/stock-quotes.json ────────────────

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD", "ADBE", "INTC",
    "JPM", "BAC", "WFC", "C", "GS", "MS", "V", "MA", "AXP", "BLK",
    "JNJ", "PFE", "MRK", "UNH", "ABT", "TMO", "LLY", "ABBV",
    "WMT", "HD", "NKE", "MCD", "KO", "SBUX", "PEP", "COST",
    "XOM", "CVX", "COP",
    "BA", "CAT", "GE", "HON",
]
EXCHANGES = ["NYSE", "NASDAQ"]
SECTORS   = ["tech", "finance", "healthcare", "consumer", "energy", "industrial"]
SIDES     = ["B", "S"]
SIZE_CHOICES = ([100] * 60) + ([500] * 30) + ([1000] * 8) + ([5000] * 2)

_seq = 0


def make_quote() -> dict:
    """One stock-quote value object, same field set as the shadowtraffic generator."""
    global _seq
    _seq += 1
    price = round(random.uniform(50.0, 500.0), 4)
    return {
        "seq":      _seq,
        "exchange": random.choice(EXCHANGES),
        "sector":   random.choice(SECTORS),
        "price":    price,
        "bid":      round(random.uniform(49.99, 499.95), 4),
        "ask":      round(random.uniform(50.01, 500.05), 4),
        "bidSize":  random.choice(SIZE_CHOICES),
        "askSize":  random.choice(SIZE_CHOICES),
        "spread":   round(random.uniform(0.01, 0.50), 4),
        "volume":   round(random.uniform(100000, 50000000), 2),
        "side":     random.choice(SIDES),
    }


# ── Producer config ─────────────────────────────────────────────────────────────

def make_producer() -> Producer:
    conf = {
        "bootstrap.servers":  BOOTSTRAP,
        "enable.idempotence": False,
        "acks":               "1",
        "client.id":          "kafka-producer-local",
    }
    if TOKEN:
        # SASL_SSL for the public cloud endpoint (:9093); SASL_PLAINTEXT for the
        # in-cluster KoP listener (stock-demo-broker:9092). Override via env.
        conf.update({
            "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
            "sasl.mechanism":    "PLAIN",
            "sasl.username":     USERNAME,
            "sasl.password":     f"token:{TOKEN}",
        })
    else:
        conf["security.protocol"] = os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    return Producer(conf)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--topic", default=DEFAULT_TOPIC,
                    help="target topic (default: %(default)s)")
    ap.add_argument("-n", "--count", type=int, default=10,
                    help="number of messages (0 = forever). Default 10.")
    ap.add_argument("--key", default="LOCAL",
                    help="Kafka message key for every record (default: LOCAL)")
    ap.add_argument("--throttle-ms", type=int, default=100,
                    help="delay between messages in ms (default 100)")
    args = ap.parse_args()

    # Accept full Pulsar FQN or short name; confluent expects the bare topic.
    topic = args.topic.replace("persistent://", "")

    print("╔══════════════════════════════════════════════════════╗")
    print("  Python Kafka Producer — Demo Source Topic")
    print("╠══════════════════════════════════════════════════════╣")
    print(f"  Bootstrap: {BOOTSTRAP}")
    print(f"  Username:  {USERNAME}")
    print(f"  Topic:     {topic}")
    print(f"  Key:       {args.key}")
    print(f"  Count:     {'∞ (forever)' if args.count == 0 else args.count}")
    print(f"  Throttle:  {args.throttle_ms}ms")
    print("╚══════════════════════════════════════════════════════╝\n")

    producer = make_producer()

    delivered = 0
    failed = 0

    def on_delivery(err, msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
            print(f"  ✗ delivery failed: {err}")
        else:
            delivered += 1

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            quote = make_quote()
            headers = [
                ("exchange", quote["exchange"].encode()),
                ("sector",   quote["sector"].encode()),
                ("side",     quote["side"].encode()),
            ]
            producer.produce(
                topic,
                key=args.key.encode(),
                value=json.dumps(quote).encode(),
                headers=headers,
                on_delivery=on_delivery,
            )
            sent += 1
            print(f"  → [{sent:>5}] key={args.key:<6} "
                  f"seq={quote['seq']:<6} {quote['exchange']:<6} {quote['sector']:<10} "
                  f"price={quote['price']:<10} side={quote['side']}")
            producer.poll(0)
            if args.throttle_ms:
                time.sleep(args.throttle_ms / 1000)
    except KeyboardInterrupt:
        print("\n  Interrupted — flushing...")
    finally:
        producer.flush(10)

    print("\n╔══════════════════════════════════════════════╗")
    print(f"  Sent      : {sent}")
    print(f"  Delivered : {delivered}")
    print(f"  Failed    : {failed}")
    print("╚══════════════════════════════════════════════╝")


if __name__ == "__main__":
    main()
