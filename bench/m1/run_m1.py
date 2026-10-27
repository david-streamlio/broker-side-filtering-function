#!/usr/bin/env python3
"""M1: one entity's events over a time window, client-side scan vs windowed filtered read.

For each arm and repetition, runs bench/clients' EntityReadClient against a loaded topic and records
what reached the client next to what the broker did for it:

  client:  records and payload bytes delivered, time to first match and to completion, TCP connections
  broker:  bytes and messages dispatched from the source topic (to the scanning client, or to the
           windowed filter), BookKeeper read operations, broker container CPU

Arms: pulsar-scan, pulsar-window (bench/docker-compose.yml) and kafka-scan, kafka-window (a KoP broker,
docker-compose.kop.yml). Arms are interleaved within each repetition. Every run checks the answer
against the generator's manifest: the hot order's events, all of them, in seq order.

Writes one JSON file per invocation (environment, per-run records, per-arm medians).

Usage, with the bench broker up and `mvn -DskipTests package` done:
  python3 bench/m1/run_m1.py --profile laptop-lite --arms pulsar-scan,pulsar-window --reps 3
"""
import argparse
import glob
import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
GEN = os.path.join(REPO, "bench", "generator", "orders_gen.py")
JAR = os.path.join(REPO, "bench", "clients", "target", "bench-clients.jar")
FILTER_NAR = "/nars/filter/selector-filter-function-1.0.0-SNAPSHOT.nar"
ORCH_NAR = "/nars/orchestrator/selector-filter-orchestrator-1.0.0-SNAPSHOT.nar"
REQUESTS_TOPIC = "persistent://public/default/filter-requests"
ARMS = ("pulsar-scan", "pulsar-window", "kafka-scan", "kafka-window")
PROFILES = {"smoke": 100_000, "laptop-lite": 2_000_000, "laptop": 10_000_000}

ORCH_CONFIG = """tenant: public
namespace: default
name: {name}
className: io.streamnative.filter.orchestrator.OrchestratorFunction
inputs:
  - {requests}
processingGuarantees: ATLEAST_ONCE
parallelism: 1
userConfig:
  state-topic: persistent://public/default/orchestrator-state
  function-package-url: {package}
  function-tenant: public
  function-namespace: default
  function-parallelism: 1
  apply-compaction: true
  pulsar-admin-url: http://localhost:8080
  idle-gc-enabled: true
  idle-gc-ttl-seconds: 5
  idle-gc-sweep-interval-seconds: 2
  existence-reconcile-enabled: false
"""


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


class Broker:
    """Admin, metrics and CPU access to one broker container."""

    def __init__(self, args):
        self.container = args.container
        self.admin = args.admin_url.rstrip("/")
        self.service_url = args.service_url

    def http(self, path):
        with urllib.request.urlopen(self.admin + path, timeout=30) as r:
            return r.read().decode()

    def admin_cli(self, *args, check=True, stdin=None):
        out = subprocess.run(["docker", "exec", "-i", self.container, "bin/pulsar-admin", *args],
                             capture_output=True, text=True, input=stdin)
        if check and out.returncode != 0:
            raise RuntimeError(f"pulsar-admin {' '.join(args)}: {out.stderr.strip()[-500:]}")
        return out.stdout.strip()

    def topic_stats(self, topic):
        return json.loads(self.http(f"/admin/v2/persistent/{topic.split('://', 1)[1]}/stats"))

    def topic_exists(self, topic):
        return topic in json.loads(self.http("/admin/v2/persistent/public/default"))

    def functions(self):
        return json.loads(self.http("/admin/v3/functions/public/default"))

    def bk_read_entries(self):
        text = self.http("/metrics/")
        m = re.search(r'^pulsar_managedLedger_client_bookkeeper_client_READ_ENTRY_count\{[^}]*success="true"[^}]*\}'
                      r'\s+([0-9.eE+]+)', text, re.M)
        return int(float(m.group(1))) if m else None

    def cpu_usec(self):
        out = subprocess.run(["docker", "exec", self.container, "cat", "/sys/fs/cgroup/cpu.stat"],
                             capture_output=True, text=True, check=True).stdout
        return int(re.search(r"^usage_usec (\d+)", out, re.M).group(1))

    def process_cpu(self):
        """{pid: (cpu seconds, role)} for the container's processes, from /proc/<pid>/stat.

        Roles: broker (the standalone JVM), window-filter (a windowed query's function instance, a
        separate JVM under ProcessRuntime), function (other function instances, e.g. the
        orchestrator), other. Under ThreadRuntime every function runs inside the broker JVM.
        """
        script = ('t=$(getconf CLK_TCK); for d in /proc/[0-9]*; do s=$(cat $d/stat 2>/dev/null) || continue; '
                  'c=$(tr "\\0" " " < $d/cmdline 2>/dev/null); echo "$t|${d#/proc/}|$s|$c"; done')
        out = subprocess.run(["docker", "exec", self.container, "sh", "-c", script],
                             capture_output=True, text=True, check=True).stdout
        procs = {}
        for line in out.splitlines():
            parts = line.split("|", 3)
            if len(parts) < 4 or ")" not in parts[2]:
                continue
            ticks, pid, stat, cmdline = int(parts[0]), parts[1], parts[2], parts[3]
            fields = stat.rsplit(")", 1)[1].split()  # fields from the 3rd (state) on
            cpu = (int(fields[11]) + int(fields[12])) / ticks  # utime + stime
            role = ("window-filter" if "selector-filter-win-" in cmdline
                    else "function" if "JavaInstanceMain" in cmdline
                    else "broker" if "PulsarStandaloneStarter" in cmdline else "other")
            procs[pid] = (cpu, role)
        return procs

    def snapshot(self, topic):
        st = self.topic_stats(topic)
        return {"source_bytes_out": st.get("bytesOutCounter", 0), "source_msgs_out": st.get("msgOutCounter", 0),
                "bk_read_entries": self.bk_read_entries(), "cpu_usec": self.cpu_usec(),
                "procs": self.process_cpu()}


def cpu_by_role(before, after):
    """CPU seconds used during the run, per role; a process that started during the run counts in full.

    A windowed filter that was already running (an earlier run's, not yet deleted by idle GC) is
    reported as window-filter-old, so window-filter is only this run's filter.
    """
    used = {}
    for pid, (cpu, role) in after.items():
        start = before[pid][0] if pid in before else 0.0
        if role == "window-filter" and pid in before:
            role = "window-filter-old"
        used[role] = round(used.get(role, 0.0) + cpu - start, 2)
    return used


def ensure_orchestrator(broker, redeploy, name=None):
    """Reuse a running bench orchestrator, or upload the filter package and deploy a fresh one.

    ``name`` fixes the orchestrator's function name (the default is ``bench-orchestrator-<timestamp>``),
    for tools that need to know it in advance, such as the observability UI in the window demo.
    """
    running = [f for f in broker.functions() if (f == name if name else f.startswith("bench-orchestrator"))]
    if running and not redeploy:
        log(f"using orchestrator {running[0]}")
        return running[0]
    for name in running:
        broker.admin_cli("functions", "delete", "--tenant", "public", "--namespace", "default", "--name", name)
    run_id = time.strftime("%Y%m%d%H%M%S")
    name, package = name or f"bench-orchestrator-{run_id}", f"function://public/default/selector-filter@bench-{run_id}"
    broker.admin_cli("packages", "upload", package, "--path", FILTER_NAR, "--description", "bench filter")
    config = ORCH_CONFIG.format(name=name, requests=REQUESTS_TOPIC, package=package)
    subprocess.run(["docker", "exec", "-i", broker.container, "bash", "-c", "cat > /tmp/bench-orch.yaml"],
                   input=config, text=True, check=True)
    broker.admin_cli("functions", "create", "--function-config-file", "/tmp/bench-orch.yaml", "--jar", ORCH_NAR)
    deadline = time.time() + 180
    while time.time() < deadline:
        status = broker.admin_cli("functions", "status", "--tenant", "public", "--namespace", "default",
                                  "--name", name, check=False)
        if '"numRunning" : 1' in status:
            time.sleep(5)  # the orchestrator opens its TableView in initialize()
            log(f"deployed orchestrator {name} with {package}")
            return name
        time.sleep(2)
    raise RuntimeError(f"orchestrator {name} did not start")


def ensure_data(broker, args):
    """Load the topic with the generator unless it exists with a manifest; return the manifest."""
    stem = args.topic.rsplit("/", 1)[-1]
    manifests = sorted(glob.glob(os.path.join(args.data_dir, f"{stem}-*-manifest.json")))
    if broker.topic_exists(args.topic):
        if not manifests:
            raise RuntimeError(f"{args.topic} exists but there's no manifest for it in {args.data_dir}; "
                               "delete the topic or pass --data-dir")
        log(f"{args.topic} already loaded ({os.path.basename(manifests[-1])})")
    else:
        size = ["--events", str(args.events)] if getattr(args, "events", None) else ["--profile", args.profile]
        extra = list(getattr(args, "generator_args", []))
        log(f"loading {args.topic} with {' '.join(size + extra)} (seed {args.seed})")
        gen = subprocess.run([sys.executable, GEN, *size, *extra, "--topic", args.topic,
                              "--service-url", broker.service_url, "--seed", str(args.seed),
                              "--out-dir", args.data_dir], capture_output=True, text=True)
        for line in gen.stderr.splitlines():  # keep the generator's own lines, not the client's logging
            if line.startswith("[gen]"):
                log(line)
        if gen.returncode != 0:
            raise RuntimeError(f"generator failed: {gen.stderr.strip()[-2000:]}")
        manifests = sorted(glob.glob(os.path.join(args.data_dir, f"{stem}-*-manifest.json")))
    with open(manifests[-1]) as f:
        manifest = json.load(f)
    manifest["_path"] = manifests[-1]
    return manifest


def wait_for_window_filters_gone(broker, timeout_s=180):
    """Idle GC deletes a windowed filter ~20 s after UNREGISTER; don't measure next to an old one."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        left = [f for f in broker.functions() if f.startswith("selector-filter-win-")]
        if not left:
            return
        time.sleep(3)
    log(f"warning: windowed filters still present after {timeout_s}s: {left}")


def run_client(args, arm, extra=(), topic=None):
    """Run one EntityReadClient read (plus any extra --name=value options) against topic (default
    args.topic); sample its TCP connections."""
    java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java"
    cmd = [java, "-jar", JAR, f"--mode={arm}", f"--topic={topic or args.topic}", f"--key={args.key}",
           f"--lookback-ms={args.lookback_ms}", f"--service-url={args.service_url}",
           f"--admin-url={args.admin_url}", f"--kafka-bootstrap={args.kafka_bootstrap}",
           f"--control-topic={REQUESTS_TOPIC}", *extra]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    conns = {"max_established": 0, "remote_ports": set()}
    done = threading.Event()

    def sample():
        while not done.is_set():
            out = subprocess.run(["lsof", "-a", "-p", str(proc.pid), "-iTCP", "-sTCP:ESTABLISHED", "-n", "-P"],
                                 capture_output=True, text=True).stdout
            remotes = re.findall(r"->[^:\s]+:(\d+)", out)
            conns["max_established"] = max(conns["max_established"], len(remotes))
            conns["remote_ports"].update(remotes)
            done.wait(0.25)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    stdout, stderr = proc.communicate()
    done.set()
    sampler.join()
    if proc.returncode != 0:
        raise RuntimeError(f"{arm} failed (exit {proc.returncode}): {stderr.strip()[-2000:]}")
    conns["remote_ports"] = sorted(conns["remote_ports"], key=int)
    return json.loads(stdout.strip().splitlines()[-1]), conns


def check(arm, client, manifest):
    """The answer must be exactly the hot order's events, in seq order; scans must see the whole window."""
    hot = len(manifest["hot_order"]["events"])
    checks = {"hot_order_events": hot, "matched": client["matched"],
              "seqs_in_order": client["matchedSeqs"] == list(range(hot))}
    if arm.endswith("scan"):
        checks["records_in_window"] = manifest["events_acked"]
        checks["whole_window_read"] = client["recordsReceived"] == manifest["events_acked"]
    else:
        stats = client.get("windowStats") or {}
        checks["only_matches_delivered"] = client["recordsReceived"] == client["matched"]
        checks["server_scanned"] = stats.get("scanned")
        checks["server_matched"] = stats.get("matched")
    checks["ok"] = (client["matched"] == hot and checks["seqs_in_order"]
                    and checks.get("whole_window_read", True) and checks.get("only_matches_delivered", True))
    return checks


def summarize(runs):
    def med(values):
        values = [v for v in values if v is not None]
        return statistics.median(values) if values else None

    summary = {}
    for arm in dict.fromkeys(r["arm"] for r in runs):
        rs = [r for r in runs if r["arm"] == arm]
        summary[arm] = {
            "runs": len(rs),
            "all_correct": all(r["checks"]["ok"] for r in rs),
            "elapsed_ms": med(r["client"]["elapsedMs"] for r in rs),
            "first_match_ms": med(r["client"].get("firstMatchMs") for r in rs),
            "records_to_client": med(r["client"]["recordsReceived"] for r in rs),
            "payload_bytes_to_client": med(r["client"]["payloadBytesReceived"] for r in rs),
            "wire_bytes_to_client": med(r["wire_bytes_to_client"] for r in rs),
            "max_connections": med(r["connections"]["max_established"] for r in rs),
            "source_bytes_dispatched": med(r["broker"]["source_bytes_out"] for r in rs),
            "bk_read_entries": med(r["broker"]["bk_read_entries"] for r in rs),
            "broker_cpu_s": med(r["broker"]["cpu_s"] for r in rs),
            "broker_cpu_s_by_role": {role: med(r["broker"].get("cpu_s_by_role", {}).get(role, 0.0) for r in rs)
                                     for role in ("broker", "window-filter", "function", "other")},
        }
    return summary


def environment(broker, args, manifest):
    def run(cmd):
        out = subprocess.run(cmd, capture_output=True, text=True)
        return (out.stdout or out.stderr).strip()

    info = json.loads(run(["docker", "info", "--format", "{{json .}}"]) or "{}")
    return {
        "repo_sha": run(["git", "-C", REPO, "rev-parse", "--short", "HEAD"]),
        "repo_dirty": bool(run(["git", "-C", REPO, "status", "--porcelain"])),
        "broker_image": run(["docker", "inspect", "-f", "{{.Config.Image}}", broker.container]),
        "docker_cpus": info.get("NCPU"), "docker_mem_bytes": info.get("MemTotal"),
        "docker_arch": info.get("Architecture"),
        "java": run([os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java",
                     "-version"]).splitlines()[0],
        "profile": args.profile, "topic": args.topic, "key": args.key, "lookback_ms": args.lookback_ms,
        "events": manifest["events_acked"], "seed": manifest["args"].get("seed"),
        "manifest": manifest["_path"], "generator_sha": manifest.get("git_sha"),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", choices=sorted(PROFILES), default="laptop-lite")
    p.add_argument("--topic", help="source topic (default persistent://public/default/orders-m1-<profile>)")
    p.add_argument("--arms", default="pulsar-scan,pulsar-window")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--key", default="8675309", help="the entity: the hot order id")
    p.add_argument("--lookback-ms", type=int, default=24 * 3600 * 1000, help="window: the last N ms")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--container", default="bench-pulsar")
    p.add_argument("--admin-url", default="http://localhost:18080")
    p.add_argument("--service-url", default="pulsar://localhost:16650")
    p.add_argument("--kafka-bootstrap", default="localhost:19092")
    p.add_argument("--redeploy", action="store_true", help="deploy a fresh orchestrator and filter package")
    p.add_argument("--data-dir", default=os.path.join(REPO, "bench", "out", "data"))
    p.add_argument("--out-dir", default=os.path.join(REPO, "bench", "out", "m1"))
    args = p.parse_args()
    args.topic = args.topic or f"persistent://public/default/orders-m1-{args.profile}"
    arms = [a.strip() for a in args.arms.split(",")]
    unknown = set(arms) - set(ARMS)
    if unknown:
        p.error(f"unknown arms {sorted(unknown)}; choose from {ARMS}")
    if not os.path.exists(JAR):
        p.error(f"{JAR} not found; run 'mvn -DskipTests package' first")
    os.makedirs(args.data_dir, exist_ok=True)
    os.makedirs(args.out_dir, exist_ok=True)

    broker = Broker(args)
    manifest = ensure_data(broker, args)
    if any(a.endswith("window") for a in arms):
        ensure_orchestrator(broker, args.redeploy)

    runs = []
    for rep in range(1, args.reps + 1):
        for arm in arms:
            if arm.endswith("window"):
                wait_for_window_filters_gone(broker)
            before = broker.snapshot(args.topic)
            client, conns = run_client(args, arm)
            after = broker.snapshot(args.topic)
            procs_before, procs_after = before.pop("procs"), after.pop("procs")
            delta = {k: (after[k] - before[k]) if after[k] is not None and before[k] is not None else None
                     for k in before}
            delta["cpu_s"] = round(delta.pop("cpu_usec") / 1e6, 3)
            delta["cpu_s_by_role"] = cpu_by_role(procs_before, procs_after)
            wire = client.get("kafkaBytesConsumed") if arm.startswith("kafka") else (
                client.get("privateTopicBytesOut") if arm.endswith("window") else delta["source_bytes_out"])
            run = {"arm": arm, "rep": rep, "client": client, "connections": conns, "broker": delta,
                   "wire_bytes_to_client": wire, "checks": check(arm, client, manifest)}
            runs.append(run)
            log(f"rep {rep} {arm:13} {'OK ' if run['checks']['ok'] else 'BAD'} "
                f"{client['elapsedMs'] / 1000:7.2f}s  to client: {client['recordsReceived']:>9,} records "
                f"{(wire or 0) / 1e6:9.2f} MB  broker: {delta['source_bytes_out'] / 1e6:9.2f} MB dispatched, "
                f"{delta['bk_read_entries']} BK reads, {delta['cpu_s']:.1f} CPU-s, "
                f"conns {conns['max_established']}")

    result = {"measurement": "M1", "started": runs[0]["client"]["startedAtMs"] if runs else None,
              "environment": environment(broker, args, manifest), "summary": summarize(runs), "runs": runs}
    path = os.path.join(args.out_dir, f"m1-{args.profile}-{time.strftime('%Y%m%d-%H%M%S')}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    log(f"wrote {path}")
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
