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
"""The time-windowed read stage demo: "why can't customer 8675309 check out?"

A support agent triages a customer whose checkout keeps failing. From a clean broker: start
Pulsar, deploy the orchestrator, run shopping-session traffic (bench/generator, --model
sessions), then ask for one customer's recent session events three ways (bench/clients'
WindowDemo):

  Act 1  client-side scan: read the whole window, keep one customer's events
  Act 2  windowed filtered read: the server scans, the client gets only that customer's events:
         three checkout attempts, each timing out on the card's 3-D Secure challenge
  Act 3  history, then live: the same read, open-ended; the customer retries while the agent
         watches, it times out again, the agent says to approve the prompt in the bank app, and
         the order goes through

    make window-demo              # runs straight through (under 3 minutes), from a clean broker
    make window-demo PAUSE=1      # waits for Enter before each act, for presenting
    make window-demo KEEP=1       # on top of the broker's retained data instead of a clean broker

The broker's data lives in the `pulsar-data` volume. `make day-load` publishes a real day into it
(session traffic for this scene, and the order data set M1/M2 measure) and keeps it; `make day-measure`
runs M1 and M2 on that day; `make window-demo-stop` stops the broker without deleting it, and
`make window-demo-down` deletes it.
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
from sessions import HOT_LIVE, HOT_HISTORY, SessionPayloadBuilder, hot_event  # noqa: E402

COMPOSE = ["docker", "compose", "-p", "window-demo", "-f", os.path.join(HERE, "docker-compose.yml")]
CONTAINER = "window-demo-pulsar"
ORCHESTRATOR = "window-demo-orchestrator"  # the observability UI is configured with this name
UI_URL = "http://localhost:3200"
ADMIN_URL = "http://localhost:18080"
SERVICE_URL = "pulsar://localhost:16650"
TOPIC = "persistent://public/default/session_events"
ORDER_TOPIC = "persistent://public/default/orders-day"         # the measurement data set, over a real day
ORDER_DATA_DIR = os.path.join(REPO, "bench", "out", "data")     # where M1/M2 look for its manifest
HOT_ORDER_AT = 0.85                                             # the hot order starts this far into the day
JAR = os.path.join(REPO, "bench", "clients", "target", "bench-clients.jar")
NARS = [os.path.join(REPO, "functions", "selector-filter", "target", "selector-filter-function-1.0.0-SNAPSHOT.nar"),
        os.path.join(REPO, "functions", "orchestrator", "target", "selector-filter-orchestrator-1.0.0-SNAPSHOT.nar")]
OTHER_BROKERS = ("bench-pulsar", "bench-kop", "bench-tiered-pulsar")

CUSTOMER = "8675309"
RATE = 3000          # target session events/s; the generator ramps toward it as sessions start
HOT_AT_S = 12        # the customer's session starts this long after traffic does; its history
                     # (three failed checkouts, then the support chat) takes about 51 s
WARMUP_S = 76        # traffic before Act 1: a 70 s window full of it, the whole history inside
LIVE_GAP_S = 2.5     # between the customer's live events in Act 3
AGENT_PAUSE_S = 4    # after the live timeout, while the agent tells the customer what to do
UNTIL = HOT_LIVE[-1][0]  # Act 3 ends when the order is confirmed

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
        if not self.args.keep_data:
            subprocess.run([*COMPOSE, "down", "-v", "--remove-orphans"], capture_output=True, check=True)
        self.t_up = time.time()
        self.note("docker compose up (keeping the retained data)" if self.args.keep_data
                  else "docker compose up (clean)")
        subprocess.run([*COMPOSE, "up", "-d", "--wait", "pulsar"], capture_output=True, check=True)
        self.note("Pulsar is up")
        # The observability UI starts alongside; nothing waits for it.
        subprocess.run([*COMPOSE, "up", "-d", "observability"], capture_output=True, check=True)
        self.note(f"observability UI starting at {UI_URL} (Windowed reads panel)")

    def start_traffic(self):
        log = open(os.path.join(self.workdir, "generator.log"), "w")
        self.generator = subprocess.Popen(
            [sys.executable, os.path.join(REPO, "bench", "generator", "orders_gen.py"),
             "--model", "sessions", "--mode", "realtime", "--rate", str(RATE), "--duration", "10m",
             "--hot-order", CUSTOMER, "--hot-at-realtime", f"{HOT_AT_S}s",
             "--topic", TOPIC, "--service-url", SERVICE_URL, "--out-dir", self.workdir],
            stdout=log, stderr=subprocess.STDOUT)
        self.t_traffic = time.time()
        self.note(f"shopping sessions started, customer {CUSTOMER} among them")

    def deploy_orchestrator(self):
        broker = m1.Broker(types.SimpleNamespace(container=CONTAINER, admin_url=ADMIN_URL, service_url=SERVICE_URL))
        m1.log = lambda msg: None  # keep the stage output to the demo's own lines
        m1.ensure_orchestrator(broker, redeploy=False, name=ORCHESTRATOR)
        self.note("orchestrator deployed")

    def wait_for_traffic(self):
        until = self.t_traffic + WARMUP_S
        while time.time() < until:
            if self.generator.poll() is not None:
                sys.exit(f"the generator stopped; see {self.workdir}/generator.log")
            line = f"sessions flowing: {self.events_in_topic():,} events in the topic"
            if TTY:
                print(f"\r{DIM}[{self.clock()}]{RESET} {line}", end="", flush=True)
            time.sleep(1)
        if TTY:
            print()
        events = self.events_in_topic()
        self.note(f"sessions flowing: {events:,} events in the topic, {events / (time.time() - self.t_traffic):,.0f}/s")

    def events_in_topic(self):
        try:
            with urllib.request.urlopen(f"{ADMIN_URL}/admin/v2/{TOPIC.replace('://', '/')}/stats",
                                        timeout=5) as r:
                return json.load(r).get("msgInCounter", 0)
        except Exception:
            return 0

    def lookback_s(self):
        """The last N seconds, N a multiple of 10 that starts after the topic's first event (a window
        reaching before it would be reported as truncated by retention)."""
        return max(10, int((time.time() - self.t_traffic - 3) // 10 * 10))

    # ---------------------------------------------------------------- acts

    def run_agent(self, act, lookback, on_line=None, until=None):
        java = os.path.join(os.environ["JAVA_HOME"], "bin", "java") if os.environ.get("JAVA_HOME") else "java"
        cmd = [java, "-cp", JAR, "io.streamnative.filter.bench.WindowDemo", act, f"--topic={TOPIC}",
               f"--key={CUSTOMER}", f"--lookback-s={lookback}", f"--service-url={SERVICE_URL}",
               f"--admin-url={ADMIN_URL}"] + ([f"--until={until}"] if until else [])
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
        self.act(f"Act 1: the client scans the last {n} s and keeps customer {CUSTOMER}'s events",
                 [f"reader.seek(now - {n} s);",
                  "while (reader.hasMessageAvailable())",
                  f"    if (reader.readNext().getKey().equals(\"{CUSTOMER}\")) keep(m);"])
        self.run_agent("scan", n)
        return n

    def act2_window(self, n):
        self.act(f"Act 2: the server scans the last {n} s; the client gets customer {CUSTOMER}'s session",
                 ["session.newConsumer().topic(\"session_events\")",
                  f"    .selector(\"__key__ = '{CUSTOMER}'\")",
                  f"    .window(TimeWindow.last(Duration.ofSeconds({n})))",
                  "    .subscribe();"])
        self.run_agent("window", n)

    def act3_follow(self):
        n = self.lookback_s()
        self.act(f"Act 3: customer {CUSTOMER}'s session, then live while they retry",
                 ["session.newConsumer().topic(\"session_events\")",
                  f"    .selector(\"__key__ = '{CUSTOMER}'\")",
                  f"    .window(TimeWindow.since(Duration.ofSeconds({n})))",
                  "    .subscribe();"])
        customer = Customer(self)
        self.run_agent("follow", n, on_line=lambda line: "caught up" in line and customer.start(), until=UNTIL)
        customer.join()

    # ---------------------------------------------------------------- run

    def load(self):
        """Publish a real day into the retained broker: session traffic for the demo (session_events) and
        the order data set for the measurements (orders-day, manifest in bench/out/data), both with the
        daily traffic shape. Broker time is set on arrival, so the day has to really happen."""
        from orders_gen import parse_duration
        self.preflight()
        self.args.keep_data = True
        self.start_broker()
        duration_s = parse_duration(self.args.load)
        order_rate = self.args.order_events / duration_s
        for topic in (TOPIC, ORDER_TOPIC):
            n = self.retained_entries(topic)
            self.note(f"{n:,} entries already retained in {topic}" if n else f"{topic} is empty")
        self.note(f"publishing for {self.args.load}, daily shape {self.args.diurnal:g} (Ctrl-C stops; the data stays):")
        self.note(f"  {TOPIC}: sessions at {self.args.rate:g} events/s on average")
        self.note(f"  {ORDER_TOPIC}: {self.args.order_events:,} order events ({order_rate:.1f}/s on average), "
                  f"order {CUSTOMER} at {HOT_ORDER_AT:.0%} of the span")
        gen = os.path.join(REPO, "bench", "generator", "orders_gen.py")
        common = ["--mode", "realtime", "--duration", self.args.load, "--diurnal", str(self.args.diurnal),
                  "--service-url", SERVICE_URL]
        procs = [
            subprocess.Popen([sys.executable, gen, *common, "--model", "sessions", "--rate", str(self.args.rate),
                              "--no-hot-order", "--topic", TOPIC, "--out-dir", self.workdir],
                             stdout=subprocess.DEVNULL, stderr=open(os.path.join(self.workdir, "sessions.log"), "w")),
            subprocess.Popen([sys.executable, gen, *common, "--model", "orders", "--rate", str(order_rate),
                              "--lifecycle-scale", str(self.args.order_scale), "--hot-order", CUSTOMER,
                              "--hot-at-realtime", f"{int(duration_s * HOT_ORDER_AT)}s",
                              "--topic", ORDER_TOPIC, "--out-dir", ORDER_DATA_DIR],
                             stdout=subprocess.DEVNULL, stderr=open(os.path.join(self.workdir, "orders.log"), "w")),
        ]
        try:
            for p in procs:
                p.wait()
        except KeyboardInterrupt:
            for p in procs:
                p.send_signal(signal.SIGINT)
            for p in procs:
                p.wait(timeout=60)
        self.note(f"done: {self.retained_entries(TOPIC):,} entries in {TOPIC}, "
                  f"{self.retained_entries(ORDER_TOPIC):,} in {ORDER_TOPIC}; logs in {self.workdir}")
        self.note("next: `make window-demo-stop` keeps it; `make day-measure` runs M1 and M2 on it; "
                  "`make window-demo KEEP=1` runs the scene on it")

    def retained_entries(self, topic=TOPIC):
        """Entries the topic holds, from internal stats (survives broker restarts, unlike msgInCounter)."""
        try:
            with urllib.request.urlopen(f"{ADMIN_URL}/admin/v2/{topic.replace('://', '/')}/internalStats",
                                        timeout=10) as r:
                return json.load(r).get("numberOfEntries", 0)
        except Exception:
            return 0

    def checkout(self):
        """Play a customer's checkout story into the running broker, for the agent demo: the history now,
        then the live retry after a pause, so an agent watching the customer sees it happen."""
        key = self.args.checkout
        Customer(self, key=key, history=True, lead_s=self.args.lead_s).run()

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
            start = "docker compose up on the retained data" if self.args.keep_data else "a clean docker compose up"
            self.note(f"{BOLD}done{RESET}: {self.clock()} after {start}")
        finally:
            if self.generator and self.generator.poll() is None:
                self.generator.send_signal(signal.SIGINT)
                self.generator.wait(timeout=30)
        print(f"{DIM}The broker and the observability UI ({UI_URL}) are still running; stop them with "
              f"`make window-demo-stop` (keeps the data) or `make window-demo-down` (deletes it).{RESET}")


class Customer(threading.Thread):
    """The customer retries once the agent is live: sends HOT_LIVE a few seconds apart, in the
    generator's format. After the live timeout the agent tells them to approve the prompt in their
    bank app, and the order goes through."""

    def __init__(self, demo, key=CUSTOMER, history=False, lead_s=0):
        super().__init__(daemon=True)
        self.demo = demo
        self.key = key
        self.history = history  # also send the history first (the agent demo; the scene's generator sends it)
        self.lead_s = lead_s    # pause between the history and the live retry
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
            payloads = SessionPayloadBuilder(seed=42)
            if self.history:
                for i, (event_type, _, detail) in enumerate(HOT_HISTORY):
                    event = hot_event(int(self.key), i, event_type, int(time.time() * 1000), detail)
                    producer.send(payloads.build(event), partition_key=event.key, properties=event.properties(),
                                  event_timestamp=event.event_time_ms)
                print(f"  {DIM}customer {self.key}: history sent ({len(HOT_HISTORY)} events); "
                      f"the retry starts in {self.lead_s} s{RESET}", flush=True)
                time.sleep(self.lead_s)
            for i, (event_type, detail) in enumerate(HOT_LIVE):
                time.sleep(LIVE_GAP_S)
                event = hot_event(int(self.key), len(HOT_HISTORY) + i, event_type, int(time.time() * 1000), detail)
                producer.send(payloads.build(event), partition_key=event.key, properties=event.properties(),
                              event_timestamp=event.event_time_ms)
                print(f"  {DIM}customer: {event.event_type}{RESET}", flush=True)
                if event_type == "3ds_timeout":
                    time.sleep(1)  # let the agent's view show the timeout first
                    print(f"  {DIM}agent: \"Your bank is asking you to approve the payment. Open your bank app, "
                          f"approve it, then try again.\"{RESET}", flush=True)
                    time.sleep(AGENT_PAUSE_S - 1)
            producer.close()
        finally:
            client.close()


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--pause", action="store_true", help="wait for Enter before each act")
    p.add_argument("--keep-data", action="store_true",
                   help="run on top of the broker's retained data instead of starting clean")
    p.add_argument("--load", metavar="DURATION",
                   help="instead of the demo: publish a real DURATION (e.g. 24h) of session traffic and the order "
                        "data set into the retained broker")
    p.add_argument("--rate", type=float, default=50.0, help="--load: session events/s on average (default 50)")
    p.add_argument("--order-events", type=int, default=2_000_000,
                   help="--load: order events over the span, the measurement scale (default 2,000,000)")
    p.add_argument("--order-scale", type=float, default=0.1,
                   help="--load: order lifecycle scale (default 0.1: an order takes up to ~4 h)")
    p.add_argument("--checkout", metavar="CUSTOMER",
                   help="instead of the demo: play CUSTOMER's checkout story into the running broker (history, "
                        "then the live retry after --lead-s), for the agent demo")
    p.add_argument("--lead-s", type=int, default=30, help="--checkout: seconds between the history and the retry")
    p.add_argument("--diurnal", type=float, default=0.6,
                   help="--load: daily traffic shape amplitude, 0..1 (default 0.6: 0.4x at night, 1.6x mid-afternoon)")
    args = p.parse_args()
    demo = Demo(args)
    demo.load() if args.load else demo.checkout() if args.checkout else demo.run()


if __name__ == "__main__":
    main()
