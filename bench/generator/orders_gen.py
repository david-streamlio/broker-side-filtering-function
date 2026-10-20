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

"""Order-lifecycle load generator for the windowed-read demo and measurements.

Replaces ShadowTraffic. Two modes:

* ``bulk``: send a profile's worth of events as fast as allowed (``--rate`` caps it).
  ``event_time`` is spread over a simulated span (default 24h) ending now. The log's own
  clock (broker receive time) is real time, so a bulk run lands "a day of traffic by volume"
  in minutes of broker time.
* ``realtime``: paced to the wall clock at ``--rate`` events/s, ``event_time`` = now.
  Lifecycles are compressed (``--lifecycle-scale``, default 0.01) so orders progress within
  minutes. ``--backfill`` sends the first N simulated minutes immediately so the stream starts
  in steady state. Use it for the stage demo and long-running loads.

Every run writes a manifest (parameters, counts by type/region/customer, the hot order's
events with message IDs or offsets) and an expected-counts CSV (events per order) next to it.

Examples::

    python bench/generator/orders_gen.py --profile smoke --topic persistent://public/default/orders
    python bench/generator/orders_gen.py --mode realtime --rate 2000 --backfill 15m --duration 30m \\
        --topic persistent://public/default/orders
    python bench/generator/orders_gen.py --sink kafka --bootstrap 127.0.0.1:9092 --topic orders \\
        --prepare-topic --profile smoke
    python bench/generator/orders_gen.py --sink null --profile smoke --dump 5
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from orders import FIRST_ORDER_ID, OrderEvent, OrderStream, PayloadBuilder, StreamConfig  # noqa: E402

PROFILES = {"smoke": 100_000, "laptop-lite": 2_000_000, "laptop": 10_000_000}
PROGRESS_EVERY_S = 5.0


def parse_duration(text: str) -> float:
    """'90s', '15m', '24h', '7d' -> seconds."""
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([smhd])", text.strip())
    if not m:
        raise argparse.ArgumentTypeError(f"bad duration '{text}' (use e.g. 90s, 15m, 24h, 7d)")
    return float(m.group(1)) * {"s": 1, "m": 60, "h": 3_600, "d": 86_400}[m.group(2)]


# ---------- sinks ----------

class _Acks:
    """Ack/error counters plus the hot order's delivery record, shared by all sinks."""

    def __init__(self, hot_order_id):
        self.hot_order_id = hot_order_id
        self.acked = 0
        self.errors = 0
        self.first_error = None
        self.hot = []  # dicts, filled at send time and completed on ack

    def ok(self):
        self.acked += 1

    def failed(self, err):
        self.errors += 1
        self.first_error = self.first_error or str(err)


class PulsarSink:
    def __init__(self, args, acks: _Acks):
        import pulsar
        self._pulsar = pulsar
        self._acks = acks
        compression = {"none": pulsar.CompressionType.NONE, "lz4": pulsar.CompressionType.LZ4,
                       "zstd": pulsar.CompressionType.ZSTD}[args.compression]
        self._client = pulsar.Client(args.service_url, operation_timeout_seconds=30)
        self._producer = self._client.create_producer(
            args.topic,
            batching_enabled=args.batching,
            batching_max_publish_delay_ms=args.batch_delay_ms,
            compression_type=compression,
            block_if_queue_full=True,
            max_pending_messages=args.max_pending,
        )

    def _on_ack(self, res, _msg_id):
        if res == self._pulsar.Result.Ok:
            self._acks.ok()
        else:
            self._acks.failed(res)

    def send(self, event: OrderEvent, payload: bytes, hot_record=None):
        callback = self._on_ack
        if hot_record is not None:
            def callback(res, msg_id, rec=hot_record):
                rec["message_id"] = str(msg_id)
                self._on_ack(res, msg_id)
        self._producer.send_async(payload, callback, partition_key=event.key,
                                  properties=event.properties(), event_timestamp=event.event_time_ms)

    def close(self):
        self._producer.flush()
        self._producer.close()
        self._client.close()


def prepare_kafka_topic(admin_url: str, pulsar_topic: str, partitions: int = 1) -> dict:
    """Pre-create a KoP source topic so the filter function can read Kafka-produced records.

    KoP auto-creates Kafka topics as partitioned topics and reads kafkaEntryFormat from the
    parent topic's properties (not the namespace). Without pulsar_non_batched there, Kafka
    batches are stored in Pulsar batch format and the function's consumer discards every
    entry as corrupted (spike S2).
    """
    path = pulsar_topic.replace("persistent://", "")
    base = f"{admin_url.rstrip('/')}/admin/v2/persistent/{path}"

    def call(method, url, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None

    try:
        call("PUT", f"{base}/partitions", partitions)
    except urllib.error.HTTPError as e:
        if e.code != 409:  # 409: already exists, keep going and check the property
            raise
    call("PUT", f"{base}/properties", {"kafkaEntryFormat": "pulsar_non_batched"})
    props = call("GET", f"{base}/properties") or {}
    if props.get("kafkaEntryFormat") != "pulsar_non_batched":
        raise RuntimeError(f"kafkaEntryFormat not set on {pulsar_topic}: {props}")
    return props


class KafkaSink:
    """Native Kafka producer (e.g. via KoP). See prepare_kafka_topic for the setup the
    filter function needs; pass --prepare-topic to do it before producing."""

    def __init__(self, args, acks: _Acks):
        from confluent_kafka import Producer
        if args.prepare_topic:
            pulsar_topic = f"persistent://{args.kafka_namespace}/{args.topic}"
            props = prepare_kafka_topic(args.admin_url, pulsar_topic)
            print(f"[gen] prepared {pulsar_topic}: {props}", file=sys.stderr)
        self._acks = acks
        self._topic = args.topic
        self._producer = Producer({
            "bootstrap.servers": args.bootstrap,
            "linger.ms": args.batch_delay_ms if args.batching else 0,
            "compression.type": args.compression,
            "queue.buffering.max.messages": args.max_pending,
        })
        self._sent = 0

    def _on_delivery(self, err, _msg):
        if err is None:
            self._acks.ok()
        else:
            self._acks.failed(err)

    def send(self, event: OrderEvent, payload: bytes, hot_record=None):
        callback = self._on_delivery
        if hot_record is not None:
            def callback(err, msg, rec=hot_record):
                if err is None:
                    rec["partition"], rec["offset"] = msg.partition(), msg.offset()
                self._on_delivery(err, msg)
        headers = list(event.properties().items())
        while True:
            try:
                self._producer.produce(self._topic, key=event.key, value=payload, headers=headers,
                                       timestamp=event.event_time_ms, on_delivery=callback)
                break
            except BufferError:
                self._producer.poll(0.1)
        self._sent += 1
        if self._sent % 1_000 == 0:
            self._producer.poll(0)

    def close(self):
        self._producer.flush(60)


class NullSink:
    """Generate without sending: measures generator speed, or dumps sample events."""

    def __init__(self, args, acks: _Acks):
        self._acks = acks
        self._dump = args.dump

    def send(self, event: OrderEvent, payload: bytes, hot_record=None):
        if self._dump > 0:
            print(json.dumps({"key": event.key, "properties": event.properties(),
                              "payload": json.loads(payload)}))
            self._dump -= 1
        self._acks.ok()

    def close(self):
        pass


SINKS = {"pulsar": PulsarSink, "kafka": KafkaSink, "null": NullSink}


# ---------- run ----------

def build_config(args, now_ms: int) -> StreamConfig:
    if args.mode == "bulk":
        events = args.events or PROFILES[args.profile]
        span = args.sim_span
        return StreamConfig(
            seed=args.seed, events=events, event_rate=events / span,
            sim_start_ms=now_ms - int(span * 1000),
            lifecycle_scale=args.lifecycle_scale if args.lifecycle_scale is not None else 1.0,
            customers=args.customers, zipf_s=args.zipf, regions=args.regions,
            hot_order_id=args.hot_order, hot_at_s=args.hot_at * span,
            late_fraction=args.late_fraction)
    events = args.events
    if events is None and args.duration is not None:
        events = int(args.rate * (args.duration + args.backfill))
    return StreamConfig(
        seed=args.seed, events=events, event_rate=args.rate,
        sim_start_ms=now_ms - int(args.backfill * 1000),
        lifecycle_scale=args.lifecycle_scale if args.lifecycle_scale is not None else 0.01,
        customers=args.customers, zipf_s=args.zipf, regions=args.regions,
        hot_order_id=args.hot_order, hot_at_s=args.backfill + args.hot_at_realtime,
        late_fraction=args.late_fraction)


def run(args) -> dict:
    start_ms = int(time.time() * 1000)
    cfg = build_config(args, start_ms)
    stream = OrderStream(cfg)
    payloads = PayloadBuilder(args.seed, args.payload_bytes)
    acks = _Acks(args.hot_order)
    sink = SINKS[args.sink](args, acks)

    run_start = time.monotonic()
    deadline = run_start + args.duration if (args.mode == "realtime" and args.duration) else None
    min_interval = 1.0 / args.rate if (args.mode == "bulk" and args.rate) else 0.0
    next_send = run_start
    last_progress, sent, interrupted = run_start, 0, False
    try:
        for emit_s, event in stream:
            now = time.monotonic()
            if args.mode == "realtime":
                target = run_start + (emit_s - args.backfill)
                if target > now:
                    time.sleep(target - now)
                if deadline and time.monotonic() >= deadline:
                    break
            elif min_interval:
                next_send += min_interval
                if next_send > now:
                    time.sleep(next_send - now)

            hot_record = None
            if event.order_id == args.hot_order:
                hot_record = {"seq": event.seq, "event_type": event.event_type,
                              "event_time_ms": event.event_time_ms,
                              "sent_wall_ms": int(time.time() * 1000)}
                acks.hot.append(hot_record)
            sink.send(event, payloads.build(event), hot_record)
            sent += 1

            if now - last_progress >= PROGRESS_EVERY_S:
                elapsed = now - run_start
                print(f"[gen] sent={sent:,} acked={acks.acked:,} errors={acks.errors} "
                      f"rate={sent / elapsed:,.0f}/s sim={emit_s / 3600:.2f}h", file=sys.stderr)
                last_progress = now
    except KeyboardInterrupt:
        interrupted = True
    finally:
        sink.close()

    end_ms = int(time.time() * 1000)
    return write_outputs(args, cfg, stream, acks, sent, start_ms, end_ms, interrupted)


def write_outputs(args, cfg, stream, acks, sent, start_ms, end_ms, interrupted) -> dict:
    os.makedirs(args.out_dir, exist_ok=True)
    stem = f"{re.sub(r'[^A-Za-z0-9._-]+', '_', args.topic.rsplit('/', 1)[-1])}-{start_ms}"
    expected_path = os.path.join(args.out_dir, f"{stem}-expected.csv")
    with open(expected_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "events"])
        if args.hot_order is not None and stream.stats.hot_events:
            w.writerow([args.hot_order, stream.stats.hot_events])
        for idx, count in enumerate(stream.stats.per_order):
            if count:
                w.writerow([FIRST_ORDER_ID + idx, count])

    s = stream.stats
    top_customers = sorted(s.by_customer.items(), key=lambda kv: -kv[1])[:20]
    manifest = {
        "tool": "bench/generator/orders_gen.py",
        "git_sha": _git_sha(),
        "args": {k: v for k, v in vars(args).items()},
        "interrupted": interrupted,
        "start_wall_ms": start_ms,
        "end_wall_ms": end_ms,
        "elapsed_s": round((end_ms - start_ms) / 1000, 1),
        "sim_start_ms": cfg.sim_start_ms,
        "sim_covered_s": round(s.last_sim_s, 1),
        "events_sent": sent,
        "events_acked": acks.acked,
        "send_errors": acks.errors,
        "first_error": acks.first_error,
        "orders": s.orders,
        "late_events": s.late,
        "by_event_type": dict(sorted(s.by_type.items())),
        "by_region": dict(sorted(s.by_region.items())),
        "top_customers": [{"customer_id": c, "events": n} for c, n in top_customers],
        "hot_order": {"order_id": args.hot_order, "events": acks.hot},
        "expected_counts_csv": expected_path,
    }
    manifest_path = os.path.join(args.out_dir, f"{stem}-manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"[gen] done: sent={sent:,} acked={acks.acked:,} errors={acks.errors} "
          f"in {manifest['elapsed_s']}s; manifest {manifest_path}", file=sys.stderr)
    return manifest


def _git_sha():
    try:
        return subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True,
                              cwd=os.path.dirname(os.path.abspath(__file__)), check=True).stdout.strip()
    except Exception:  # noqa: BLE001 - provenance is best-effort
        return None


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["bulk", "realtime"], default="bulk")
    p.add_argument("--profile", choices=sorted(PROFILES), default="smoke",
                   help="bulk event count: smoke=100k, laptop-lite=2M, laptop=10M")
    p.add_argument("--events", type=int, help="explicit event count (overrides --profile)")
    p.add_argument("--rate", type=float, default=0.0,
                   help="events/s; bulk: cap (0 = unlimited); realtime: target (default 1000)")
    p.add_argument("--sim-span", type=parse_duration, default=parse_duration("24h"),
                   help="bulk: simulated time the events' event_time covers (default 24h)")
    p.add_argument("--duration", type=parse_duration, help="realtime: stop after this long")
    p.add_argument("--backfill", type=parse_duration, default=0.0,
                   help="realtime: send the first N simulated time immediately (e.g. 15m)")
    p.add_argument("--lifecycle-scale", type=float,
                   help="multiplier on lifecycle delays (default 1.0 bulk, 0.01 realtime)")

    p.add_argument("--sink", choices=sorted(SINKS), default="pulsar")
    p.add_argument("--topic", default="persistent://public/default/orders",
                   help="Pulsar topic, or Kafka topic name for --sink kafka")
    p.add_argument("--service-url", default="pulsar://localhost:6650")
    p.add_argument("--bootstrap", default="127.0.0.1:9092")
    p.add_argument("--prepare-topic", action="store_true",
                   help="kafka sink: pre-create the partitioned topic with kafkaEntryFormat=pulsar_non_batched")
    p.add_argument("--admin-url", default="http://localhost:8080", help="Pulsar admin URL for --prepare-topic")
    p.add_argument("--kafka-namespace", default="public/default",
                   help="Pulsar tenant/namespace KoP maps Kafka topics to (kafkaTenant/kafkaNamespace)")
    p.add_argument("--batching", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--batch-delay-ms", type=int, default=10)
    p.add_argument("--compression", choices=["none", "lz4", "zstd"], default="none")
    p.add_argument("--max-pending", type=int, default=10_000)

    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--customers", type=int, default=50_000)
    p.add_argument("--zipf", type=float, default=0.8, help="customer popularity skew (Zipf exponent)")
    p.add_argument("--regions", type=int, default=100)
    p.add_argument("--hot-order", type=int, default=8_675_309)
    p.add_argument("--hot-at", type=float, default=0.8,
                   help="bulk: when the hot order starts, as a fraction of --sim-span")
    p.add_argument("--hot-at-realtime", type=parse_duration, default=parse_duration("60s"),
                   help="realtime: when the hot order starts, after the backfill")
    p.add_argument("--late-fraction", type=float, default=0.0,
                   help="fraction of events emitted late (event_time kept, sent 30s-15m later)")
    p.add_argument("--payload-bytes", type=int, default=1024)

    p.add_argument("--out-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "out"))
    p.add_argument("--dump", type=int, default=0, help="null sink: print the first N events")
    args = p.parse_args(argv)
    if args.mode == "realtime" and not args.rate:
        args.rate = 1_000.0
    return args


if __name__ == "__main__":
    run(parse_args())
