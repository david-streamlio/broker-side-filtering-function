#!/usr/bin/env python3
"""
In-cluster (toolset pod) Kafka producer for the demo SOURCE topic.

This is the kafka-python (pure-Python) sibling of kafka-producer.py — used on
stock-demo-toolset-0, where confluent-kafka has no prebuilt wheel. It talks to
the in-cluster KoP listener (stock-demo-broker:9092) using the pod's mounted
broker token (/mnt/client/token).

Every record carries key "LOCAL" (override with --key) so manually produced
messages stand out from shadowtraffic ticker-keyed traffic.

Config via env (sensible in-cluster defaults):
    KAFKA_BOOTSTRAP          default stock-demo-broker:9092
    KAFKA_SECURITY_PROTOCOL  default SASL_PLAINTEXT
    KAFKA_USERNAME           default stock-demo-app
    PULSAR_TOKEN_PATH        default /mnt/client/token
    DEMO_SOURCE_TOPIC_STOCKS default public/default/stock-quotes

Usage:
    python3 kafka-producer-pod.py -n 10
    python3 kafka-producer-pod.py -n 0 --throttle-ms 200   # forever
"""

import os
import json
import time
import random
import argparse
from pathlib import Path

from kafka import KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "stock-demo-broker:9092")
PROTOCOL  = os.environ.get("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
USERNAME  = os.environ.get("KAFKA_USERNAME", "stock-demo-app")
TOKEN     = Path(os.environ.get("PULSAR_TOKEN_PATH", "/mnt/client/token")).read_text().strip()
TOPIC     = os.environ.get("DEMO_SOURCE_TOPIC_STOCKS", "public/default/stock-quotes").replace("persistent://", "")

TICKERS = ["AAPL", "MSFT", "GOOGL", "NVDA", "JPM", "JNJ", "WMT", "XOM", "BA"]
EXCHANGES = ["NYSE", "NASDAQ"]
SECTORS   = ["tech", "finance", "healthcare", "consumer", "energy", "industrial"]
SIDES     = ["B", "S"]
SIZE_CHOICES = ([100] * 60) + ([500] * 30) + ([1000] * 8) + ([5000] * 2)

_seq = 0


def make_quote():
    global _seq
    _seq += 1
    return {
        "seq":      _seq,
        "exchange": random.choice(EXCHANGES),
        "sector":   random.choice(SECTORS),
        "price":    round(random.uniform(50.0, 500.0), 4),
        "bid":      round(random.uniform(49.99, 499.95), 4),
        "ask":      round(random.uniform(50.01, 500.05), 4),
        "bidSize":  random.choice(SIZE_CHOICES),
        "askSize":  random.choice(SIZE_CHOICES),
        "spread":   round(random.uniform(0.01, 0.50), 4),
        "volume":   round(random.uniform(100000, 50000000), 2),
        "side":     random.choice(SIDES),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-n", "--count", type=int, default=10, help="0 = forever")
    ap.add_argument("--key", default="LOCAL")
    ap.add_argument("--topic", default=TOPIC)
    ap.add_argument("--throttle-ms", type=int, default=100)
    args = ap.parse_args()

    print("== Kafka Producer (in-cluster, kafka-python) ==")
    print(f"  bootstrap={BOOTSTRAP}  protocol={PROTOCOL}")
    print(f"  user={USERNAME}  topic={args.topic}  key={args.key}  count={args.count or 'inf'}\n")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        security_protocol=PROTOCOL,
        sasl_mechanism="PLAIN",
        sasl_plain_username=USERNAME,
        sasl_plain_password=f"token:{TOKEN}",
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: json.dumps(v).encode(),
        acks=1,
        api_version_auto_timeout_ms=10000,
    )

    sent = ok = 0
    fail = 0

    def on_ok(md):
        nonlocal ok
        ok += 1

    def on_err(e):
        nonlocal fail
        fail += 1
        print(f"  x send failed: {e}")

    try:
        while args.count == 0 or sent < args.count:
            q = make_quote()
            headers = [
                ("exchange", q["exchange"].encode()),
                ("sector",   q["sector"].encode()),
                ("side",     q["side"].encode()),
            ]
            producer.send(args.topic, key=args.key, value=q, headers=headers).add_callback(on_ok).add_errback(on_err)
            sent += 1
            print(f"  -> [{sent:>4}] key={args.key} seq={q['seq']} {q['exchange']} {q['sector']} price={q['price']} side={q['side']}")
            if args.throttle_ms:
                time.sleep(args.throttle_ms / 1000)
    except KeyboardInterrupt:
        print("\n  interrupted, flushing...")
    finally:
        producer.flush(10)
        producer.close(5)

    print(f"\n  sent={sent} delivered={ok} failed={fail}")


if __name__ == "__main__":
    main()
