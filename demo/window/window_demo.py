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
"""The time-windowed read stage demo: "what happened to order 8675309?"

From a clean broker: start Pulsar, deploy the orchestrator, run order traffic, then ask for one
order's recent events three ways (bench/clients' WindowDemo):

  Act 1  client-side scan: read the whole window, keep one order's events
  Act 2  windowed filtered read: the server scans, the client gets only that order's events
  Act 3  history, then live: the same read, open-ended; the warehouse ships the order and its
         next events stream in

    make window-demo              # runs straight through (under 3 minutes)
    make window-demo PAUSE=1      # waits for Enter before each act, for presenting
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import types
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "bench", "m1"))
sys.path.insert(0, os.path.join(REPO, "bench", "generator"))
import run_m1 as m1  # noqa: E402
from orders import HOT_CUSTOMER_ID, HOT_LIFECYCLE, HOT_REGION, OrderEvent, PayloadBuilder  # noqa: E402

COMPOSE = ["docker", "compose", "-p", "window-demo", "-f", os.path.join(HERE, "docker-compose.yml")]
CONTAINER = "window-demo-pulsar"
ADMIN_URL = "http://localhost:18080"
SERVICE_URL = "pulsar://localhost:16650"
TOPIC = "persistent://public/default/orders"
JAR = os.path.join(REPO, "bench", "clients", "target", "bench-clients.jar")
NARS = [os.path.join(REPO, "functions", "selector-filter", "target", "selector-filter-function-1.0.0-SNAPSHOT.nar"),
        os.path.join(REPO, "functions", "orchestrator", "target", "selector-filter-orchestrator-1.0.0-SNAPSHOT.nar")]
OTHER_BROKERS = ("bench-pulsar", "bench-kop", "bench-tiered-pulsar")

ORDER = "8675309"
RATE = 3000          # target order events/s; the generator ramps toward it as orders progress
HOT_AT_S = 20        # the order is created this long after traffic starts...
HOT_STEPS = 4        # ...and the generator sends created, paid, picked, packed
WARMUP_S = 65        # traffic before Act 1: a 60 s window full of it
LIVE_GAP_S = 3       # between the warehouse's live events in Act 3

TTY = sys.stdout.isatty()
BOLD, DIM, CYAN, RESET = ("\033[1m", "\033[2m", "\033[36m", "\033[0m") if TTY else ("", "", "", "")


class Demo:
    def __init__(self, args):
        self.args = args
        self.t_up = None          # when docker compose up started
        self.t_traffic = None     # when the generator started
        self.generator = None
        self.workdir = tempfile.mkdtemp(prefix="window-demo-")

    # ---------------------------------------------------------------- output

    def note(self, msg):
        print(f"{DIM}[{self.clock()}]{RESET} {msg}", flush=True)

    def clock(self):
        s = int(time.time() - self.t_up) if self.t_up else 0
        return f"{s // 60}:{s % 60:02d}"

    def act(self, title, code):
        if self.args.pause:
            input(f"\n{DIM}(Enter for the next act){RESET}")
        print(f"\n{BOLD}{title}{RESET}")
        for line in code:
            print(f"  {CYAN}{line}{RESET}")
        print(flush=True)

    # ---------------------------------------------------------------- setup

    def preflight(self):
        missing = [p for p in [JAR, *NARS] if not os.path.exists(p)]
        if missing:
            sys.exit("missing build outputs (run `mvn -DskipTests package`):\n  " + "\n  ".join(missing))
        running = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True,
                                 check=True).stdout.split()
        busy = [c for c in running if c in OTHER_BROKERS]
        if busy:
            sys.exit(f"stop the bench broker first (same ports, and the laptop can't hold both): "
                     f"{', '.join(busy)}")

    def start_broker(self):
        subprocess.run([*COMPOSE, "down", "-v", "--remove-orphans"], capture_output=True, check=True)
        self.t_up = time.time()
        self.note("docker compose up (clean)")
        subprocess.run([*COMPOSE, "up", "-d", "--wait"], capture_output=True, check=True)
        self.note("Pulsar is up")

    def start_traffic(self):
        log = open(os.path.join(self.workdir, "generator.log"), "w")
        self.generator = subprocess.Popen(
            [sys.executable, os.path.join(REPO, "bench", "generator", "orders_gen.py"),
             "--mode", "realtime", "--rate", str(RATE), "--duration", "10m",
             "--hot-order", ORDER, "--hot-at-realtime", f"{HOT_AT_S}s", "--hot-steps", str(HOT_STEPS),
             "--topic", TOPIC, "--service-url", SERVICE_URL, "--out-dir", self.workdir],
            stdout=log, stderr=subprocess.STDOUT)
        self.t_traffic = time.time()
        self.note(f"order traffic started, order {ORDER} among it")

    def deploy_orchestrator(self):
        broker = m1.Broker(types.SimpleNamespace(container=CONTAINER, admin_url=ADMIN_URL, service_url=SERVICE_URL))
        m1.log = lambda msg: None  # keep the stage output to the demo's own lines
        m1.ensure_orchestrator(broker, redeploy=False)
        self.note("orchestrator deployed")

    def wait_for_traffic(self):
        until = self.t_traffic + WARMUP_S
        while time.time() < until:
            if self.generator.poll() is not None:
                sys.exit(f"the generator stopped; see {self.workdir}/generator.log")
            line = f"orders flowing: {self.events_in_topic():,} events in the topic"
            if TTY:
                print(f"\r{DIM}[{self.clock()}]{RESET} {line}", end="", flush=True)
            time.sleep(1)
        if TTY:
            print()
        events = self.events_in_topic()
        self.note(f"orders flowing: {events:,} events in the topic, {events / (time.time() - self.t_traffic):,.0f}/s")

    def events_in_topic(self):
        try:
            with urllib.request.urlopen(f"{ADMIN_URL}/admin/v2/persistent/public/default/orders/stats",
                                        timeout=5) as r:
                return json.load(r).get("msgInCounter", 0)
        except Exception:
            return 0

    def lookback_s(self):
        """The last N seconds, N a multiple of 10 that starts after the topic's first event (a window
        reaching before it would be reported as truncated by retention)."""
        return max(10, int((time.time() - self.t_traffic - 3) // 10 * 10))

    # ---------------------------------------------------------------- acts

    def run_agent(self, act, lookback, on_line=None):
        java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java"
        cmd = [java, "-cp", JAR, "io.streamnative.filter.bench.WindowDemo", act, f"--topic={TOPIC}",
               f"--key={ORDER}", f"--lookback-s={lookback}", f"--service-url={SERVICE_URL}",
               f"--admin-url={ADMIN_URL}"]
        err = open(os.path.join(self.workdir, f"{act}.log"), "w")
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=err, text=True, bufsize=1)
        for line in proc.stdout:
            print(line, end="", flush=True)
            if on_line:
                on_line(line)
        if proc.wait() != 0:
            sys.exit(f"{act} failed; see {self.workdir}/{act}.log")

    def act1_scan(self):
        n = self.lookback_s()
        self.act(f"Act 1: the client scans the last {n} s and keeps order {ORDER}",
                 [f"reader.seek(now - {n} s);",
                  "while (reader.hasMessageAvailable())",
                  f"    if (reader.readNext().getKey().equals(\"{ORDER}\")) keep(m);"])
        self.run_agent("scan", n)
        return n

    def act2_window(self, n):
        self.act(f"Act 2: the server scans the last {n} s; the client gets order {ORDER}",
                 ["session.newConsumer().topic(\"orders\")",
                  f"    .selector(\"__key__ = '{ORDER}'\")",
                  f"    .window(TimeWindow.last(Duration.ofSeconds({n})))",
                  "    .subscribe();"])
        self.run_agent("window", n)

    def act3_follow(self):
        n = self.lookback_s()
        self.act(f"Act 3: order {ORDER}'s history, then live",
                 ["session.newConsumer().topic(\"orders\")",
                  f"    .selector(\"__key__ = '{ORDER}'\")",
                  f"    .window(TimeWindow.since(Duration.ofSeconds({n})))",
                  "    .subscribe();"])
        warehouse = Warehouse(self)
        self.run_agent("follow", n, on_line=lambda line: "caught up" in line and warehouse.start())
        warehouse.join()

    # ---------------------------------------------------------------- run

    def run(self):
        self.preflight()
        try:
            self.start_broker()
            self.start_traffic()
            self.deploy_orchestrator()
            self.wait_for_traffic()
            n = self.act1_scan()
            self.act2_window(n)
            self.act3_follow()
            print()
            self.note(f"{BOLD}done{RESET}: {self.clock()} after a clean docker compose up")
        finally:
            if self.generator and self.generator.poll() is None:
                self.generator.send_signal(signal.SIGINT)
                self.generator.wait(timeout=30)
        print(f"{DIM}The broker is still running; stop it with `make window-demo-down`.{RESET}")


class Warehouse(threading.Thread):
    """Ships the order once the agent is live: sends the rest of its lifecycle, a few seconds apart,
    in the generator's format."""

    def __init__(self, demo):
        super().__init__(daemon=True)
        self.demo = demo
        self.started = False

    def start(self):
        if not self.started:
            self.started = True
            super().start()

    def join(self, timeout=None):
        if self.started:
            super().join(timeout)

    def run(self):
        import pulsar
        client = pulsar.Client(SERVICE_URL, logger=pulsar.ConsoleLogger(pulsar.LoggerLevel.Error))
        try:
            producer = client.create_producer(TOPIC, batching_enabled=False)
            payloads = PayloadBuilder(seed=42)
            for seq in range(HOT_STEPS, len(HOT_LIFECYCLE)):
                time.sleep(LIVE_GAP_S)
                event = OrderEvent(int(ORDER), HOT_CUSTOMER_ID, HOT_REGION, seq, HOT_LIFECYCLE[seq][0],
                                   int(time.time() * 1000))
                producer.send(payloads.build(event), partition_key=event.key, properties=event.properties(),
                              event_timestamp=event.event_time_ms)
                print(f"  {DIM}warehouse: {event.event_type}{RESET}", flush=True)
            producer.close()
        finally:
            client.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pause", action="store_true", help="wait for Enter before each act")
    Demo(p.parse_args()).run()


if __name__ == "__main__":
    main()
