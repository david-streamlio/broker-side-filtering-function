#!/usr/bin/env python3
"""M3: how a time window maps onto the log's ledgers, and what reading hot vs cold ledgers costs.

On the tiered bench stack (bench/docker-compose.tiered.yml: sn-platform with tiered storage on MinIO):

1. Load --events unbatched order events into small ledgers (2,000 entries each) and offload all but the
   newest --keep to MinIO.
2. Record the window -> ledger timeline from `topics info-internal` (each ledger's entries, size, close
   timestamp, isOffloaded), which doesn't touch the tier.
3. Read windows with the windowed read and with a client scan of the same bounds, counting MinIO GETs
   and bytes per read (MinIO's cluster metrics, sampled until they settle):
     cold  a few offloaded ledgers in the middle
     hot   the newest closed ledgers, still in BookKeeper
     all   the whole topic, mostly offloaded
   Each cold read starts from a broker restart (empty caches) and a small warm-up read; the windowed
   read of the cold window then runs twice in a row, the second time from the tiered-storage cache.
4. Retention: on a second topic, a 1-minute retention trims a first phase of events before a second is
   loaded; a window that starts at the first phase must report TRUNCATED_BY_RETENTION. (If retention deletes everything, a window
   reports COMPLETE with nothing scanned: with no message left there's nothing to compare against.)

The broker runs emulated (amd64 on arm64) and local MinIO has no per-request latency, so cold cost shows
up as requests and bytes, not time (spike S3).

Usage:
  docker compose -f bench/docker-compose.tiered.yml up -d
  python3 bench/m3/run_m3.py --events 200000
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, "..", "m1"))
import run_m1 as m1  # noqa: E402

SELECTOR = "__key__ = '8675309'"


# ---------------------------------------------------------------- MinIO (from spike S3)

def minio_counters(url):
    """GET and other S3 request counts, plus bytes sent and received, from MinIO's cluster metrics."""
    with urllib.request.urlopen(url, timeout=10) as r:
        text = r.read().decode()
    reqs, sent, recv = {}, 0.0, 0.0
    for line in text.splitlines():
        if line.startswith("minio_s3_requests_total{"):
            api = re.search(r'api="([^"]+)"', line)
            if api:
                reqs[api.group(1)] = reqs.get(api.group(1), 0) + float(line.rsplit(" ", 1)[1])
        elif line.startswith("minio_s3_traffic_sent_bytes"):
            sent += float(line.rsplit(" ", 1)[1])
        elif line.startswith("minio_s3_traffic_received_bytes"):
            recv += float(line.rsplit(" ", 1)[1])
    return {"requests": reqs, "sent_bytes": sent, "received_bytes": recv}


def settled_counters(url, max_wait_s=120):
    """MinIO caches its cluster metrics for a while: poll until two samples 10 s apart agree."""
    prev = minio_counters(url)
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        time.sleep(10)
        cur = minio_counters(url)
        if cur == prev:
            return cur
        prev = cur
    return prev


def minio_delta(before, after):
    apis = set(before["requests"]) | set(after["requests"])
    reqs = {a: int(after["requests"].get(a, 0) - before["requests"].get(a, 0)) for a in apis}
    return {"requests": {a: n for a, n in reqs.items() if n},
            "gets": reqs.get("getobject", 0) + reqs.get("headobject", 0),
            "sent_bytes": int(after["sent_bytes"] - before["sent_bytes"])}


# ---------------------------------------------------------------- broker

def info_internal(broker, topic):
    return json.loads(broker.admin_cli("topics", "info-internal", topic))


def ledgers(broker, topic):
    """[{ledgerId, entries, size, timestamp (close, ms), offloaded}] for closed ledgers, oldest first."""
    out = []
    for l in info_internal(broker, topic).get("ledgers", []):
        if l.get("entries", 0) > 0 and l.get("timestamp", 0) > 0:
            out.append({"ledgerId": l["ledgerId"], "entries": l["entries"], "size": l.get("size"),
                        "timestamp": l["timestamp"], "offloaded": bool(l.get("isOffloaded"))})
    return out


def offload(broker, topic, keep):
    if any(l["offloaded"] for l in ledgers(broker, topic)):
        m1.log("already offloaded")
        return
    m1.log(f"offloading all but the newest {keep}")
    broker.admin_cli("topics", "offload", "-s", keep, topic)
    deadline = time.time() + 900
    while time.time() < deadline:
        status = broker.admin_cli("topics", "offload-status", topic, check=False)
        if "success" in status.lower():
            return
        if "error" in status.lower():
            raise RuntimeError(f"offload failed: {status}")
        time.sleep(3)
    raise RuntimeError("offload did not finish in 15 minutes")


def restart(broker, args):
    """Restart the broker (clears the entry and tiered-storage caches) and wait for the orchestrator.

    After a restart on this image the functions worker can try to start the orchestrator before the
    package service answers, fail with HTTP 503, and not retry; restarting the function then works.
    """
    m1.log("restarting the broker to clear caches")
    subprocess.run(["docker", "restart", broker.container], check=True, capture_output=True)
    deadline = time.time() + 600
    nudge_at = None
    while time.time() < deadline:
        try:
            orchs = [f for f in broker.functions() if f.startswith("bench-orchestrator")]
            if orchs:
                status = broker.admin_cli("functions", "status", "--tenant", "public", "--namespace", "default",
                                          "--name", orchs[0], check=False)
                if '"numRunning" : 1' in status:
                    time.sleep(5)
                    return
                nudge_at = nudge_at or time.time() + 30
                if time.time() > nudge_at:
                    m1.log("orchestrator didn't start after the restart; restarting the function")
                    broker.admin_cli("functions", "restart", "--tenant", "public", "--namespace", "default",
                                     "--name", orchs[0], check=False)
                    nudge_at = time.time() + 60
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError("broker or orchestrator did not come back")


def bounds_for(ledger_list, first, last):
    """Broker-time bounds covering ledgers [first, last] (indexes into the closed-ledger list)."""
    lo = ledger_list[first - 1]["timestamp"] + 1 if first > 0 else 0
    return lo, ledger_list[last]["timestamp"]


def measured_read(broker, args, arm, lo, hi, label):
    if arm.endswith("window"):
        m1.wait_for_window_filters_gone(broker)
    before = settled_counters(args.minio_metrics)
    client, _ = m1.run_client(args, arm, extra=[f"--selector={SELECTOR}", f"--window=between:{lo}:{hi}"])
    after = settled_counters(args.minio_metrics)
    minio = minio_delta(before, after)
    scanned = client["windowStats"]["scanned"] if arm.endswith("window") else client["recordsReceived"]
    m1.log(f"{label:14} {arm:13} {scanned:>8,} scanned, {client['matched']} matched in "
           f"{client['elapsedMs'] / 1000:6.2f}s; MinIO {minio['gets']} GETs, {minio['sent_bytes'] / 1e6:7.2f} MB")
    return {"label": label, "arm": arm, "bounds": [lo, hi], "client": client, "minio": minio, "scanned": scanned}


def warm_up(args, ledger_list):
    """A small scan of the newest closed ledger: topic load and JVM warm-up aren't charged to a read."""
    lo, hi = bounds_for(ledger_list, len(ledger_list) - 1, len(ledger_list) - 1)
    m1.run_client(args, "pulsar-scan", extra=[f"--selector={SELECTOR}", f"--window=between:{lo}:{hi}"])


# ---------------------------------------------------------------- retention

def retention_case(broker, args):
    """Trim a topic's first phase by retention; a window from its start must say truncated.

    Phase 1 is loaded, a 1-minute retention trims it, retention goes back to infinite, and only then is
    phase 2 loaded, so how long the trim takes can't matter. (A size retention isn't used: the broker
    requires it to exceed the namespace's 10 GB backlog quota.)
    """
    topic_args = argparse.Namespace(**vars(args))
    topic_args.topic = f"persistent://public/default/orders-m3-retention-{args.retention_events}-{int(time.time())}"
    topic_args.events = args.retention_events
    topic_args.generator_args = ["--no-batching", "--compression", "none"]
    first = m1.ensure_data(broker, topic_args)
    topic = topic_args.topic
    before = ledgers(broker, topic)
    broker.admin_cli("topics", "set-retention", "-t", "1m", "-s", "-1", topic)
    m1.log("waiting 75 s so phase 1 is older than the 1-minute retention")
    time.sleep(75)
    broker.admin_cli("topics", "trim-topic", topic, check=False)
    deadline = time.time() + 300
    after = before
    while time.time() < deadline:
        after = ledgers(broker, topic)
        if len(after) < len(before):
            break
        time.sleep(10)
    broker.admin_cli("topics", "set-retention", "-t", "-1", "-s", "-1", topic)
    m1.log(f"retention 1m trimmed phase 1: {len(before)} -> {len(after)} closed ledgers; retention back to infinite")
    gen = subprocess.run([sys.executable, m1.GEN, "--events", str(args.retention_events), "--no-batching",
                          "--compression", "none", "--topic", topic, "--service-url", broker.service_url,
                          "--seed", str(args.seed + 1), "--out-dir", args.data_dir], capture_output=True, text=True)
    if gen.returncode != 0:
        raise RuntimeError(f"phase 2 failed: {gen.stderr.strip()[-1000:]}")
    total = 2 * args.retention_events
    lo, hi = first["start_wall_ms"] - 60_000, int(time.time() * 1000) + 60_000
    m1.wait_for_window_filters_gone(broker)
    client, _ = m1.run_client(topic_args, "pulsar-window", extra=[f"--selector={SELECTOR}", f"--window=between:{lo}:{hi}"])
    stats = client["windowStats"]
    m1.log(f"window from phase 1's start: {stats['scanned']:,} of {total:,} scanned, status {stats['status']}")
    return {"topic": topic, "retention": "1m, then infinite before phase 2", "events": total,
            "ledgers_before_trim": len(before), "ledgers_after_trim": len(after), "window": [lo, hi],
            "scanned": stats["scanned"], "status": stats["status"], "client": client,
            "ok": stats["status"] == "TRUNCATED_BY_RETENTION" and 0 < stats["scanned"] < total}


# ---------------------------------------------------------------- main

def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--events", type=int, default=200_000)
    p.add_argument("--keep", default="25M", help="newest data left in BookKeeper by the offload")
    p.add_argument("--window-ledgers", type=int, default=5, help="ledgers in the cold and hot windows")
    p.add_argument("--retention-events", type=int, default=20_000)
    p.add_argument("--retention-only", metavar="RESULT_JSON",
                   help="run only the retention case and merge it into this M3 result file")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--container", default="bench-tiered-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--redeploy", action="store_true", help="deploy a fresh orchestrator and filter package")
    p.add_argument("--minio-metrics", default="http://localhost:19000/minio/v2/metrics/cluster")
    p.add_argument("--data-dir", default=os.path.join(m1.REPO, "bench", "out", "data-tiered"))
    p.add_argument("--out-dir", default=os.path.join(m1.REPO, "bench", "out", "m3"))
    args = p.parse_args()
    args.key, args.lookback_ms, args.profile = "8675309", 24 * 3600 * 1000, None
    args.topic = f"persistent://public/default/orders-m3-unbatched-{args.events}"
    args.generator_args = ["--no-batching", "--compression", "none"]
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)
    broker = m1.Broker(args)

    if args.retention_only:
        with open(args.retention_only) as f:
            result = json.load(f)
        m1.ensure_orchestrator(broker, args.redeploy)
        result["retention"] = retention_case(broker, args)
        result["checks"]["retention: TRUNCATED_BY_RETENTION"] = result["retention"]["ok"]
        result["all_correct"] = all(result["checks"].values())
        with open(args.retention_only, "w") as f:
            json.dump(result, f, indent=2)
        m1.log(f"updated {args.retention_only}")
        print(json.dumps(result["checks"], indent=2))
        return

    manifest = m1.ensure_data(broker, args)
    m1.ensure_orchestrator(broker, args.redeploy)
    offload(broker, args.topic, args.keep)
    timeline = ledgers(broker, args.topic)
    cold = [i for i, l in enumerate(timeline) if l["offloaded"]]
    hot = [i for i, l in enumerate(timeline) if not l["offloaded"]]
    if len(cold) < args.window_ledgers or len(hot) < args.window_ledgers:
        sys.exit(f"need {args.window_ledgers} cold and hot closed ledgers, have {len(cold)} cold, {len(hot)} hot")
    m1.log(f"{len(timeline)} closed ledgers: {len(cold)} offloaded, {len(hot)} in BookKeeper")
    mid = cold[len(cold) // 2]
    windows = {
        "cold": bounds_for(timeline, mid, mid + args.window_ledgers - 1),
        "hot": bounds_for(timeline, hot[-args.window_ledgers], hot[-1]),
        "all": (manifest["start_wall_ms"] - 60_000, manifest["end_wall_ms"] + 60_000),
    }

    reads = []
    restart(broker, args)
    warm_up(args, timeline)
    reads.append(measured_read(broker, args, "pulsar-window", *windows["cold"], "cold"))
    reads.append(measured_read(broker, args, "pulsar-window", *windows["cold"], "cold-again"))
    restart(broker, args)
    warm_up(args, timeline)
    reads.append(measured_read(broker, args, "pulsar-scan", *windows["cold"], "cold"))
    reads.append(measured_read(broker, args, "pulsar-window", *windows["hot"], "hot"))
    reads.append(measured_read(broker, args, "pulsar-scan", *windows["hot"], "hot"))
    restart(broker, args)
    warm_up(args, timeline)
    reads.append(measured_read(broker, args, "pulsar-window", *windows["all"], "all"))
    restart(broker, args)
    warm_up(args, timeline)
    reads.append(measured_read(broker, args, "pulsar-scan", *windows["all"], "all"))

    by = {(r["label"], r["arm"]): r for r in reads}
    checks = {f"{label}: window matches scan": by[(label, "pulsar-window")]["client"]["matched"]
              == by[(label, "pulsar-scan")]["client"]["matched"] for label in ("cold", "hot", "all")}
    env = m1.environment(broker, args, manifest)
    env.update(keep=args.keep, window_ledgers=args.window_ledgers)
    result = {"measurement": "M3", "environment": env, "timeline": timeline, "windows": windows,
              "reads": reads, "retention": None, "checks": checks}
    path = os.path.join(args.out_dir, f"m3-{time.strftime('%Y%m%d-%H%M%S')}.json")

    def write():
        result["all_correct"] = all(checks.values())
        with open(path, "w") as f:
            json.dump(result, f, indent=2)

    write()  # keep the reads even if the retention case fails
    try:
        result["retention"] = retention_case(broker, args)
        checks["retention: TRUNCATED_BY_RETENTION"] = result["retention"]["ok"]
    except Exception as e:  # noqa: BLE001
        m1.log(f"retention case failed: {e}")
        result["retention"] = {"error": str(e)}
        checks["retention: TRUNCATED_BY_RETENTION"] = False
    write()
    m1.log(f"wrote {path}")
    print(json.dumps(checks, indent=2))


if __name__ == "__main__":
    main()
