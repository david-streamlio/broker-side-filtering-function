#!/usr/bin/env python3
"""M2: where the windowed read wins and where it loses, by selectivity and window size.

On one loaded topic, for each window (the last 1 min, 1 h or 24 h of the simulated day) and each
selectivity (one entity, ~1% of events, everything), the same selector is read two ways:

  pulsar-scan     a Reader seeks to the window start and reads to its end; the client evaluates the
                  selector with the same engine the filter uses
  pulsar-window   the Pulsar SDK's windowed read; the server evaluates it

The generator's bulk mode squeezes a simulated day into a minute or two of broker time, so a window of W
minutes is the last W/1440 of the load's span on the log's clock (the broker-entry timestamps both arms
use). Each windowed read must return exactly as many matches as the scan of the same window.

Expected: the windowed read wins on bytes whenever the selector is selective, but loses on time for tiny
windows (starting a filter costs more than scanning a few thousand events) and on everything when the
selector matches everything (every event crosses the network twice).

Usage, with the Apache bench broker up and the M1 topic loaded (bench/m1/run_m1.py):
  python3 bench/m2/run_m2.py --reps 3
"""
import argparse
import json
import os
import statistics
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "m1"))
import run_m1 as m1  # noqa: E402

WINDOWS = {"1min": 1, "1h": 60, "24h": 1440}  # minutes of the simulated day
SELECTIVITY = {"entity": "__key__ = '8675309'", "1pct": "region = 'r07'", "all": "region LIKE 'r%'"}
PAD_MS = 60_000  # generator clock vs broker clock; only used at the ends of the topic


def bounds(manifest, minutes):
    """The last `minutes` of the simulated day, as broker-time bounds."""
    start, end = manifest["start_wall_ms"], manifest["end_wall_ms"]
    if minutes >= 1440:
        return start - PAD_MS, end + PAD_MS
    return end - round((end - start) * minutes / 1440), end + PAD_MS


def read(broker, args, arm, selector, lo, hi):
    if arm.endswith("window"):
        m1.wait_for_window_filters_gone(broker)
    before = broker.snapshot(args.topic)
    client, conns = m1.run_client(args, arm, extra=[f"--selector={selector}", f"--window=between:{lo}:{hi}"])
    after = broker.snapshot(args.topic)
    roles = m1.cpu_by_role(before.pop("procs"), after.pop("procs"))
    wire = client.get("privateTopicBytesOut") if arm.endswith("window") else after["source_bytes_out"] - before["source_bytes_out"]
    return {"client": client, "connections": conns, "wire_bytes_to_client": wire,
            "broker_cpu_s": round((after["cpu_usec"] - before["cpu_usec"]) / 1e6, 3), "cpu_s_by_role": roles,
            "source_bytes_out": after["source_bytes_out"] - before["source_bytes_out"]}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--topic", default="persistent://public/default/orders-m1-laptop-lite")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--windows", default=",".join(WINDOWS))
    p.add_argument("--selectivity", default=",".join(SELECTIVITY))
    p.add_argument("--container", default="bench-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--redeploy", action="store_true")
    p.add_argument("--data-dir", default=os.path.join(m1.REPO, "bench", "out", "data"))
    p.add_argument("--out-dir", default=os.path.join(m1.REPO, "bench", "out", "m2"))
    args = p.parse_args()
    args.key, args.lookback_ms, args.profile, args.seed = "8675309", 24 * 3600 * 1000, None, 42
    os.makedirs(args.out_dir, exist_ok=True)
    broker = m1.Broker(args)
    manifest = m1.ensure_data(broker, args)  # an existing topic needs its manifest in --data-dir
    m1.ensure_orchestrator(broker, args.redeploy)

    runs = []
    for rep in range(1, args.reps + 1):
        for win_name in args.windows.split(","):
            lo, hi = bounds(manifest, WINDOWS[win_name])
            for sel_name in args.selectivity.split(","):
                selector = SELECTIVITY[sel_name]
                scan = read(broker, args, "pulsar-scan", selector, lo, hi)
                window = read(broker, args, "pulsar-window", selector, lo, hi)
                ok = window["client"]["matched"] == scan["client"]["matched"]
                runs.append({"rep": rep, "window": win_name, "selectivity": sel_name, "selector": selector,
                             "bounds": [lo, hi], "scan": scan, "window_read": window, "ok": ok})
                m1.log(f"rep {rep} {win_name:4} {sel_name:6} {'OK ' if ok else 'BAD'} "
                       f"scan {scan['client']['recordsReceived']:>9,} read / {scan['client']['matched']:>9,} matched "
                       f"in {scan['client']['elapsedMs'] / 1000:5.2f}s | window "
                       f"{window['client']['windowStats']['scanned']:>9,} scanned / {window['client']['matched']:>9,} "
                       f"matched in {window['client']['elapsedMs'] / 1000:5.2f}s")

    def med(values):
        values = [v for v in values if v is not None]
        return statistics.median(values) if values else None

    summary = {}
    for win_name in args.windows.split(","):
        for sel_name in args.selectivity.split(","):
            rs = [r for r in runs if r["window"] == win_name and r["selectivity"] == sel_name]
            summary[f"{win_name}/{sel_name}"] = {
                arm: {"elapsed_ms": med(r[key]["client"]["elapsedMs"] for r in rs),
                      "records_to_client": med(r[key]["client"]["recordsReceived"] for r in rs),
                      "wire_bytes_to_client": med(r[key]["wire_bytes_to_client"] for r in rs),
                      "broker_cpu_s": med(r[key]["broker_cpu_s"] for r in rs),
                      "matched": med(r[key]["client"]["matched"] for r in rs)}
                for arm, key in (("scan", "scan"), ("window", "window_read"))}
    env = m1.environment(broker, args, manifest)
    result = {"measurement": "M2", "environment": env, "summary": summary,
              "all_correct": all(r["ok"] for r in runs), "runs": runs}
    path = os.path.join(args.out_dir, f"m2-{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    m1.log(f"wrote {path}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
