#!/usr/bin/env python3
"""M4, broker arm: filtering at the broker with the selector EntryFilter, unbatched vs batched.

For each topic (--events order events: unbatched uncompressed, unbatched lz4, batched lz4) and each
selector (none, key `__key__ = '8675309'`, property `region = 'r07'`), a plain consumer drains a new
subscription whose `selector` subscription property makes the broker's EntryFilter filter it
(bench/clients' pulsar-drain mode). Per run:

  client:  records and bytes the subscription delivered
  broker:  CPU (cgroup), and the EntryFilter's own counters: entries evaluated, entries passed through
           unfiltered because they're batched, and nanoseconds spent evaluating

The EntryFilter logs its cumulative counters at most every 10 s, and only while entries flow through a
selector subscription. After each run the runner waits 10 s and drains a one-message "poke" topic
through a selector, which forces a summary line; consecutive summaries give the run's counts, less what
a poke itself adds (measured at startup with two back-to-back pokes; it's 2 evaluations, not 1).

Expected: on unbatched topics only the matches leave the broker, read from the entry's metadata without
touching the (possibly compressed) payload. Batched entries can't be filtered there: they all pass.

Usage, with the Apache bench broker up with the EntryFilter override (bench/docker-compose.entryfilter.yml):
  python3 bench/m4/run_m4_broker.py --events 1000000 --reps 3
"""
import argparse
import json
import os
import re
import statistics
import subprocess
import sys
import time

import pulsar

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "m1"))
import run_m1 as m1  # noqa: E402

TOPICS = {  # label: (generator options, topic name stem)
    "unbatched-none": (["--no-batching", "--compression", "none"], "orders-m4b-unbatched-none"),
    "unbatched-lz4": (["--no-batching", "--compression", "lz4"], "orders-m4b-unbatched-lz4"),
    "batched-lz4": (["--compression", "lz4"], "orders-m4-lz4"),  # the function arm's topic
}
SELECTORS = {"none": None, "key": "__key__ = '8675309'", "property": "region = 'r07'"}
POKE_TOPIC = "persistent://public/default/m4b-entry-filter-poke"
SUMMARY = re.compile(r"\[selector-entry-filter\] evaluated=(\d+) accepted=(\d+) rejected=(\d+) "
                     r"batchedUnfiltered=(\d+) invalidSelector=(\d+) avgEvalNanos=(\d+)")


def filter_counters(container):
    """The EntryFilter's latest cumulative summary: evaluated, batched-unfiltered, total eval nanos."""
    logs = subprocess.run(["docker", "logs", "--since", "15m", container], capture_output=True, text=True)
    found = SUMMARY.findall(logs.stdout + logs.stderr)
    if not found:
        return None
    evaluated, _, _, batched, _, avg = (int(x) for x in found[-1])
    return {"evaluated": evaluated, "batched_unfiltered": batched, "eval_nanos": evaluated * avg}


def poke(args):
    """Wait out the summary interval, then push one entry through a selector so a summary is logged."""
    time.sleep(10.5)
    m1.run_client(args, "pulsar-drain", extra=[f"--subscription-selector=__key__ = 'poke'"],
                  topic=POKE_TOPIC)
    time.sleep(1.0)  # let the log line flush
    return filter_counters(args.container)


def ensure_poke_topic(service_url):
    client = pulsar.Client(service_url, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
    try:
        producer = client.create_producer(POKE_TOPIC, batching_enabled=False)
        producer.send(b"poke", partition_key="poke")
        producer.close()
    finally:
        client.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=1_000_000, help="events per topic")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--topics", default=",".join(TOPICS))
    p.add_argument("--selectors", default=",".join(SELECTORS))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--container", default="bench-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--data-dir", default=os.path.join(m1.REPO, "bench", "out", "data"))
    p.add_argument("--out-dir", default=os.path.join(m1.REPO, "bench", "out", "m4"))
    args = p.parse_args()
    args.key, args.lookback_ms, args.profile = "8675309", 24 * 3600 * 1000, None
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)
    broker = m1.Broker(args)

    topics = {}
    for label in args.topics.split(","):
        gen_opts, stem = TOPICS[label]
        args.topic = f"persistent://public/default/{stem}-{args.events}"
        args.generator_args = gen_opts
        topics[label] = (args.topic, m1.ensure_data(broker, args))
    ensure_poke_topic(args.service_url)
    counters = poke(args)
    if counters is None:
        sys.exit("no [selector-entry-filter] summary in the broker log: is the EntryFilter loaded?")
    calibrated = poke(args)
    poke_cost = {k: calibrated[k] - counters[k] for k in counters}
    counters = calibrated
    m1.log(f"a poke adds {poke_cost}")

    runs = []
    for rep in range(1, args.reps + 1):
        for label, (topic, manifest) in topics.items():
            for sel_name in args.selectors.split(","):
                selector = SELECTORS[sel_name]
                args.topic = topic
                before = broker.snapshot(topic)
                extra = [f"--subscription-selector={selector}"] if selector else []
                client, _ = m1.run_client(args, "pulsar-drain", extra=extra)
                after = broker.snapshot(topic)
                new_counters = poke(args)
                ef = {k: new_counters[k] - counters[k] - poke_cost[k] for k in counters}
                counters = new_counters
                batched = label.startswith("batched")
                expected = (manifest["events_acked"] if selector is None or batched
                            else len(manifest["hot_order"]["events"]) if sel_name == "key"
                            else manifest["by_region"]["r07"])
                ok = client["recordsReceived"] == expected
                run = {"rep": rep, "topic_label": label, "selector_type": sel_name, "selector": selector,
                       "client": client, "expected_records": expected, "ok": ok,
                       "broker_cpu_s": round((after["cpu_usec"] - before["cpu_usec"]) / 1e6, 3),
                       "entry_filter": ef,
                       # Only where the run itself evaluated entries (a selector on unbatched entries).
                       "eval_ns_per_entry": round(ef["eval_nanos"] / ef["evaluated"], 1)
                       if ef["evaluated"] >= 1_000 else None}
                runs.append(run)
                m1.log(f"rep {rep} {label:14} {sel_name:8} {'OK ' if ok else 'BAD'} "
                       f"{client['recordsReceived']:>9,} records ({client['subscriptionBytesOut'] / 1e6:8.2f} MB) "
                       f"in {client['elapsedMs'] / 1000:5.1f}s; broker {run['broker_cpu_s']:5.1f} CPU-s; "
                       f"filter evaluated {ef['evaluated']:,}, batched-unfiltered {ef['batched_unfiltered']:,}, "
                       f"{run['eval_ns_per_entry']} ns/entry")

    def med(values):
        values = [v for v in values if v is not None]
        return statistics.median(values) if values else None

    summary = {}
    for label in topics:
        for sel_name in args.selectors.split(","):
            rs = [r for r in runs if r["topic_label"] == label and r["selector_type"] == sel_name]
            summary.setdefault(label, {})[sel_name] = {
                "records_to_client": med(r["client"]["recordsReceived"] for r in rs),
                "bytes_to_client": med(r["client"]["subscriptionBytesOut"] for r in rs),
                "elapsed_ms": med(r["client"]["elapsedMs"] for r in rs),
                "broker_cpu_s": med(r["broker_cpu_s"] for r in rs),
                "eval_ns_per_entry": med(r["eval_ns_per_entry"] for r in rs),
                "batched_unfiltered": med(r["entry_filter"]["batched_unfiltered"] for r in rs),
            }
    env = m1.environment(broker, args, next(iter(topics.values()))[1])
    env.update(events_per_topic=args.events, topics={label: t for label, (t, _) in topics.items()},
               storage_bytes={label: broker.topic_stats(t).get("storageSize") for label, (t, _) in topics.items()})
    env["poke_cost"] = poke_cost
    result = {"measurement": "M4-broker", "environment": env, "summary": summary,
              "all_correct": all(r["ok"] for r in runs), "runs": runs}
    path = os.path.join(args.out_dir, f"m4-broker-{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    m1.log(f"wrote {path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
