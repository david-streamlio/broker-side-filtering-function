#!/usr/bin/env python3
"""M5: the failure modes of a windowed read, shown as demos on the M1 topic.

Every demo reads the whole topic with the key selector `__key__ LIKE '%07'` (orders whose id ends in 07:
about 1% of orders, several events each) and checks the result against the generator's per-order
counts. The talk's scope is a key filter over a time window, so the demos use keys only.

  a  completion vs an empty poll: receives that return nothing before the completion record arrives
     don't mean the window is done; only the completion record does.
  b  a consumer paused past its private topic's TTL: with a 10 s TTL (standing in for the 300 s default
     of live queries) and a 90 s pause, the broker expires the results, and the completion record with
     them, so the client gets only what it had prefetched and never sees the window complete. The windowed default (1 h)
     survives the same pause.
  c  the filter killed mid-window: `kill -9` on its JVM once it has been sent about a quarter of the topic.
     The functions worker restarts it, and it re-reads from its last acknowledged position: matches whose
     acknowledgement hadn't been flushed arrive twice. (A graceful `functions restart` flushes the
     acknowledgements first and produces no duplicates.) Deduplicating on __src_msgid__ gives
     exactly the expected count.
  d  per-key order on one partition: no order's seq goes backwards (duplicates by source id excluded).

The broker must enforce TTLs often enough for demo b (bench/docker-compose.yml checks every minute).

Usage, with the Apache bench broker up and the M1 topic loaded:
  python3 bench/m5/run_m5.py
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "m1"))
import run_m1 as m1  # noqa: E402

SELECTOR = "__key__ LIKE '%07'"


def expected_matches(manifest):
    """Events of orders whose id ends in 07, from the generator's per-order counts."""
    with open(manifest["expected_counts_csv"]) as f:
        return sum(int(row["events"]) for row in csv.DictReader(f) if row["order_id"].endswith("07"))


def window_read(broker, args, manifest, *extra):
    m1.wait_for_window_filters_gone(broker)
    lo, hi = manifest["start_wall_ms"] - 60_000, manifest["end_wall_ms"] + 60_000
    client, _ = m1.run_client(args, "pulsar-window",
                              extra=[f"--selector={SELECTOR}", f"--window=between:{lo}:{hi}", *extra])
    return client


def kill_filter_mid_scan(broker, topic, after_messages, out):
    """kill -9 this read's windowed filter JVM once the broker has sent its subscription after_messages."""
    deadline = time.time() + 120
    while time.time() < deadline:
        subs = broker.topic_stats(topic).get("subscriptions", {})
        sent = max((s.get("msgOutCounter", 0) for n, s in subs.items() if n.startswith("win-")), default=0)
        pids = [pid for pid, (_, role) in broker.process_cpu().items() if role == "window-filter"]
        if pids and sent >= after_messages:
            subprocess.run(["docker", "exec", broker.container, "kill", "-9", *pids], check=False)
            out.update(killed_pids=pids, killed_after_messages=sent, killed_at_ms=int(time.time() * 1000))
            return
        time.sleep(0.2)


def summary_line(c):
    return (f"{c['recordsReceived']:,} records, {c.get('distinctSourceIds')} distinct source ids, "
            f"{c['orderViolations']} order violations, {c['emptyReceivesBeforeComplete']} empty receives before "
            f"completion, {'GAVE UP waiting for completion' if c['gaveUp'] else 'complete'}")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", default="persistent://public/default/orders-m1-laptop-lite")
    p.add_argument("--demos", default="a,b,c")
    p.add_argument("--pause-s", type=int, default=90)
    p.add_argument("--short-ttl-s", type=int, default=10)
    p.add_argument("--restart-after", type=float, default=0.25,
                   help="restart the filter once it has been sent this fraction of the topic")
    p.add_argument("--container", default="bench-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--data-dir", default=os.path.join(m1.REPO, "bench", "out", "data"))
    p.add_argument("--out-dir", default=os.path.join(m1.REPO, "bench", "out", "m5"))
    args = p.parse_args()
    args.key, args.lookback_ms, args.profile, args.seed = "8675309", 24 * 3600 * 1000, None, 42
    os.makedirs(args.out_dir, exist_ok=True)
    broker = m1.Broker(args)
    manifest = m1.ensure_data(broker, args)
    m1.ensure_orchestrator(broker, False)
    expected = expected_matches(manifest)
    demos = {}

    if "a" in args.demos:  # also d: per-key order on an undisturbed read
        c = window_read(broker, args, manifest)
        demos["a_and_d"] = {"client": c, "expected": expected,
                            "ok": (not c["gaveUp"] and c["matched"] == expected and c["orderViolations"] == 0
                                   and c["emptyReceivesBeforeComplete"] > 0)}
        m1.log(f"a/d undisturbed: {summary_line(c)} (expected {expected:,})")

    if "b" in args.demos:
        pause, give_up = args.pause_s * 1000, (args.pause_s + 60) * 1000
        short = window_read(broker, args, manifest, f"--private-ttl-seconds={args.short_ttl_s}",
                            f"--pause-ms={pause}", f"--give-up-ms={give_up}")
        m1.log(f"b TTL {args.short_ttl_s}s, paused {args.pause_s}s: {summary_line(short)} (expected {expected:,})")
        default = window_read(broker, args, manifest, f"--pause-ms={pause}", f"--give-up-ms={give_up}")
        m1.log(f"b windowed default TTL, paused {args.pause_s}s: {summary_line(default)} (expected {expected:,})")
        demos["b"] = {"short_ttl_s": args.short_ttl_s, "pause_s": args.pause_s, "short_ttl": short,
                      "default_ttl": default, "expected": expected,
                      "ok": short["recordsReceived"] < expected and not default["gaveUp"]
                      and default["matched"] == expected}

    if "c" in args.demos:
        info = {}
        killer = threading.Thread(target=kill_filter_mid_scan,
                                  args=(broker, args.topic, int(manifest["events_acked"] * args.restart_after), info),
                                  daemon=True)
        killer.start()
        c = window_read(broker, args, manifest)
        killer.join(timeout=5)
        duplicates = c["recordsReceived"] - (c.get("distinctSourceIds") or 0)
        m1.log(f"c filter JVM killed mid-scan (after {info.get('killed_after_messages', 'NOT killed')} messages): "
               f"{summary_line(c)}; "
               f"{duplicates:,} duplicates (expected {expected:,} distinct)")
        demos["c"] = {"client": c, "restart": info, "duplicates": duplicates, "expected": expected,
                      "showed_duplicates": duplicates > 0,
                      "ok": "killed_pids" in info and c.get("distinctSourceIds") == expected
                      and c["orderViolations"] == 0}

    env = m1.environment(broker, args, manifest)
    result = {"measurement": "M5", "environment": env, "selector": SELECTOR, "demos": demos,
              "all_correct": all(d["ok"] for d in demos.values())}
    path = os.path.join(args.out_dir, f"m5-{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    m1.log(f"wrote {path}")
    print(json.dumps({k: v["ok"] for k, v in demos.items()}, indent=2))


if __name__ == "__main__":
    main()
