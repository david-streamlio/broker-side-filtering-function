#!/usr/bin/env python3
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

"""
Direct-consumer latency baseline — a plain Kafka consumer (no filter, no SDK) reading a KoP topic,
measuring per-message latency = now - record.timestamp() (the producer's CreateTime, the same
publishTime the filter and the observability UI use). Prints p50/p90/p99 over a rolling window.

Purpose: isolate the *KoP bridge* latency from the *filter's* latency. Run it on the raw source
topic to see what an ordinary Kafka consumer pays with no filtering at all, then compare to the
filtered topics' numbers in the UI:

    pip install kafka-python
    # raw source (the bridge floor a plain consumer pays — expect ~the UI's p50 minus the filter's send):
    python3 demo/baseline/latency_baseline.py stock-quotes
    # a filtered output topic (consumer-side view of the filtered path), same clock, apples-to-apples:
    python3 demo/baseline/latency_baseline.py filtered-1fea76bd-2d83-4c99-9b...

Config is read from demo/.env (KAFKA_BOOTSTRAP, KAFKA_USERNAME, PULSAR_TOKEN_PATH); override via env.

Note on clocks: this consumer runs on your host, so host-vs-source-producer is one clock domain
(ShadowTraffic also runs on your host). The UI's filtered metric is measured on the cloud broker, so
comparing this number to the UI carries any host<->cloud NTP offset (usually a few ms). For a
skew-free comparison, run this on BOTH the raw topic and a filtered-* topic — both host-measured.
"""
import os
import sys
import time
from collections import deque

try:
    from kafka import KafkaConsumer
except ImportError:
    sys.exit("kafka-python not installed — run: pip install kafka-python")


def load_env(path="demo/.env"):
    env = {}
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def pct(sorted_vals, p):
    if not sorted_vals:
        return float("nan")
    i = min(len(sorted_vals) - 1, int(round((p / 100.0) * (len(sorted_vals) - 1))))
    return sorted_vals[i]


def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "stock-quotes"
    env = load_env()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP") or env.get("KAFKA_BOOTSTRAP")
    username = os.environ.get("KAFKA_USERNAME") or env.get("KAFKA_USERNAME") or "stock-demo-app"
    token_path = os.environ.get("PULSAR_TOKEN_PATH") or env.get("PULSAR_TOKEN_PATH")
    if not bootstrap or not token_path:
        sys.exit("Missing KAFKA_BOOTSTRAP / PULSAR_TOKEN_PATH (set them or run from the repo root so demo/.env is found)")
    with open(token_path) as f:
        jwt = f.read().strip()

    print(f"→ consuming '{topic}' on {bootstrap} as {username}  (latency = now - record CreateTime)")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=username,
        sasl_plain_password=f"token:{jwt}",
        auto_offset_reset="latest",          # only new messages, like a live consumer
        enable_auto_commit=False,
        group_id=f"latency-baseline-{os.getpid()}",
        consumer_timeout_ms=10000,
    )

    window = deque(maxlen=20000)             # rolling sample window for percentiles
    last_report = time.time()
    total = 0
    try:
        for msg in consumer:
            if msg.timestamp is None or msg.timestamp <= 0:
                continue
            lat = int(time.time() * 1000) - msg.timestamp   # ms
            if lat < 0:
                continue                                    # clock skew guard
            window.append(lat)
            total += 1
            now = time.time()
            if now - last_report >= 5.0 and window:
                s = sorted(window)
                rate = len(window) / max(1e-9, (now - last_report)) if len(window) < window.maxlen else None
                print(f"n={len(s):>6} total={total:>8}  "
                      f"p50={pct(s,50):>4}ms  p90={pct(s,90):>4}ms  p99={pct(s,99):>5}ms  "
                      f"p99.9={pct(s,99.9):>5}ms  max={s[-1]:>5}ms")
                last_report = now
    except KeyboardInterrupt:
        pass
    finally:
        if window:
            s = sorted(window)
            print(f"\nFINAL  n={len(s)}  p50={pct(s,50)}ms  p90={pct(s,90)}ms  "
                  f"p99={pct(s,99)}ms  p99.9={pct(s,99.9)}ms  max={s[-1]}ms")
        consumer.close()


if __name__ == "__main__":
    main()
