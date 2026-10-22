#!/usr/bin/env python3
"""M4, function arm: the filter function's CPU per message, by compression and selector type.

For each compression (none, lz4, zstd) a topic of --events order events, batched as the generator sends
them, and for each selector type:

  key       __key__ = '8675309'     the message key: the payload isn't parsed
  property  order_id = '8675309'    a message property: the payload isn't parsed
  json      carrier = 'none'        a field of the JSON value: every payload is parsed (and none matches)

a windowed read over the whole topic. While it runs, the runner samples the filter JVM's CPU time and
the messages the broker has dispatched to the filter's subscription; the CPU per message is the slope of
one against the other between 10% and 90% of the scan, which leaves out the JVM's startup and the tail.
The function's consumer always decompresses batches; only a value-field selector parses the payload.

Usage, with the Apache bench broker up (bench/docker-compose.yml) and `mvn -DskipTests package` done:
  python3 bench/m4/run_m4.py --events 1000000 --reps 3
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "m1"))
import run_m1 as m1  # noqa: E402

SELECTORS = {
    "key": ("__key__ = '8675309'", 7),
    "property": ("order_id = '8675309'", 7),
    "json": ("carrier = 'none'", 0),
}
COMPRESSIONS = ("none", "lz4", "zstd")
# Window bounds come from the generator's wall clock; the broker stamps entries with its own. Pad the
# whole-topic window so a small skew can't cut it short (the half window's exact size doesn't matter).
PAD_MS = 60_000


def sample_filter(broker, topic, old_pids, samples, done):
    """Until done: append (filter JVM CPU seconds, messages dispatched to its win-* subscription)."""
    pid = None
    while not done.is_set():
        procs = broker.process_cpu()
        if pid is None:
            pid = next((p for p, (_, role) in procs.items() if role == "window-filter" and p not in old_pids), None)
        subs = broker.topic_stats(topic).get("subscriptions", {})
        dispatched = sum(s.get("msgOutCounter", 0) for name, s in subs.items() if name.startswith("win-"))
        if pid is not None and pid in procs:
            samples.append((procs[pid][0], dispatched))
        done.wait(0.2)


def slope(samples, total, lo=0.1, hi=0.9):
    """Least-squares CPU seconds per message over the samples between lo and hi of the scan."""
    pts = [(m, c) for c, m in samples if lo * total <= m <= hi * total]
    if len(pts) < 3:
        return None, len(pts)
    mean_m = sum(m for m, _ in pts) / len(pts)
    mean_c = sum(c for _, c in pts) / len(pts)
    var = sum((m - mean_m) ** 2 for m, _ in pts)
    return (sum((m - mean_m) * (c - mean_c) for m, c in pts) / var if var else None), len(pts)


def window_read(broker, args, topic, selector, from_ms, to_ms):
    """One windowed read, sampling the filter JVM's CPU against its progress."""
    m1.wait_for_window_filters_gone(broker)
    before = broker.snapshot(topic)
    samples, done = [], threading.Event()
    sampler = threading.Thread(target=sample_filter, args=(broker, topic, set(before["procs"]), samples, done),
                               daemon=True)
    sampler.start()
    try:
        client, _ = m1.run_client(args, "pulsar-window",
                                  extra=[f"--selector={selector}", f"--window=between:{from_ms}:{to_ms}"])
    finally:
        done.set()
        sampler.join()
    after = broker.snapshot(topic)
    roles = m1.cpu_by_role(before["procs"], after["procs"])
    per_msg, used = slope(samples, client["windowStats"]["scanned"])
    return {"client": client, "filter_cpu_s": roles.get("window-filter", 0.0), "cpu_s_by_role": roles,
            "broker_cpu_s": round((after["cpu_usec"] - before["cpu_usec"]) / 1e6, 3),
            "source_bytes_out": after["source_bytes_out"] - before["source_bytes_out"],
            "filter_cpu_us_per_msg": None if per_msg is None else round(per_msg * 1e6, 3),
            "samples_in_fit": used, "samples": samples}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=1_000_000, help="events per topic")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--compressions", default=",".join(COMPRESSIONS))
    p.add_argument("--selectors", default=",".join(SELECTORS))
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--container", default="bench-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--redeploy", action="store_true", help="deploy a fresh orchestrator and filter package")
    p.add_argument("--data-dir", default=os.path.join(m1.REPO, "bench", "out", "data"))
    p.add_argument("--out-dir", default=os.path.join(m1.REPO, "bench", "out", "m4"))
    args = p.parse_args()
    args.key, args.lookback_ms, args.profile = "8675309", 24 * 3600 * 1000, None
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)
    broker = m1.Broker(args)

    topics = {}
    for comp in args.compressions.split(","):
        args.topic = f"persistent://public/default/orders-m4-{comp}-{args.events}"
        args.generator_args = ["--compression", comp]
        topics[comp] = (args.topic, m1.ensure_data(broker, args))
    m1.ensure_orchestrator(broker, args.redeploy)

    runs = []
    for rep in range(1, args.reps + 1):
        for comp, (topic, manifest) in topics.items():
            args.topic = topic
            start, end = manifest["start_wall_ms"], manifest["end_wall_ms"]
            for sel_name in args.selectors.split(","):
                selector, expected = SELECTORS[sel_name]
                read = window_read(broker, args, topic, selector, start - PAD_MS, end + PAD_MS)
                ws = read["client"]["windowStats"]
                cpu_us = read["filter_cpu_us_per_msg"]
                ok = ws["scanned"] == manifest["events_acked"] and read["client"]["matched"] == expected
                runs.append({"rep": rep, "compression": comp, "selector_type": sel_name, "selector": selector,
                             "read": read, "filter_cpu_us_per_msg": cpu_us, "ok": ok})
                m1.log(f"rep {rep} {comp:4} {sel_name:8} {'OK ' if ok else 'BAD'} {ws['scanned']:,} scanned, "
                       f"{read['client']['matched']} matched in {ws['elapsedMs'] / 1000:.1f}s; filter "
                       f"{read['filter_cpu_s']:.2f} CPU-s total, {cpu_us} us/msg over {read['samples_in_fit']} samples")

    summary = {}
    for r in runs:
        cell = summary.setdefault(r["compression"], {}).setdefault(r["selector_type"], [])
        if r["filter_cpu_us_per_msg"] is not None:
            cell.append(r["filter_cpu_us_per_msg"])
    summary = {c: {s: {"median_us_per_msg": statistics.median(v) if v else None, "runs": v}
                   for s, v in sels.items()} for c, sels in summary.items()}
    env = m1.environment(broker, args, next(iter(topics.values()))[1])
    env.update(events_per_topic=args.events, topics={c: t for c, (t, _) in topics.items()})
    result = {"measurement": "M4-function", "environment": env, "summary": summary,
              "all_correct": all(r["ok"] for r in runs), "runs": runs}
    path = os.path.join(args.out_dir, f"m4-function-{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    m1.log(f"wrote {path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
