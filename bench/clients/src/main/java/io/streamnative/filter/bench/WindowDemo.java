/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.streamnative.filter.bench;

import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.WindowStats;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The agent in the time-windowed read stage demo ({@code demo/window}): it asks for one order's events
 * over the last N seconds of a topic, one act per run.
 *
 * <ul>
 *   <li>{@code scan}: client-side scan. Seek to the window's start, read everything, keep the order's events.</li>
 *   <li>{@code window}: windowed filtered read. The server scans the window; the client gets the order's events.</li>
 *   <li>{@code follow}: the same read, open-ended: the order's history, then its new events as they happen,
 *       until one of type {@code --until} arrives.</li>
 * </ul>
 *
 * <pre>
 * java -cp bench-clients.jar io.streamnative.filter.bench.WindowDemo window --topic=orders --key=8675309 --lookback-s=60
 * </pre>
 *
 * Times shown are the events' {@code event_time} property, on the producer's clock.
 */
public final class WindowDemo {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final String topic;
    private final String key;
    private final Duration lookback;
    private final Map<String, String> opts;
    private final long deadline;

    private WindowDemo(Map<String, String> opts) {
        this.opts = opts;
        this.topic = opt("topic", "persistent://public/default/orders");
        this.key = opt("key", "8675309");
        this.lookback = Duration.ofSeconds(Long.parseLong(opt("lookback-s", "60")));
        this.deadline = System.currentTimeMillis() + 1000 * Long.parseLong(opt("timeout-s", "90"));
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 1; i < args.length; i++) {
            int eq = args[i].indexOf('=');
            if (!args[i].startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --name=value, got " + args[i]);
            }
            opts.put(args[i].substring(2, eq), args[i].substring(eq + 1));
        }
        WindowDemo demo = new WindowDemo(opts);
        switch (args.length > 0 ? args[0] : "") {
            case "scan" -> demo.scan();
            case "window" -> demo.window();
            case "follow" -> demo.follow();
            default -> throw new IllegalArgumentException("usage: WindowDemo scan|window|follow [--name=value ...]");
        }
        System.exit(0); // client libraries leave non-daemon threads behind
    }

    // ---------------------------------------------------------------- acts

    /** Act 1: read the whole window on the client and throw away every other order. */
    private void scan() throws Exception {
        long start = System.currentTimeMillis();
        long read = 0;
        long bytes = 0;
        long kept = 0;
        try (PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl()).build();
             Reader<byte[]> reader = client.newReader().topic(topic).startMessageId(MessageId.latest).create()) {
            reader.seek(start - lookback.toMillis());
            while (reader.hasMessageAvailable()) {
                Message<byte[]> m = reader.readNext(30, TimeUnit.SECONDS);
                if (m == null) {
                    throw new IllegalStateException("reader stalled with messages still available");
                }
                if (m.getBrokerPublishTime().orElse(m.getPublishTime()) > start) {
                    break; // the window ends when the read started; traffic keeps arriving
                }
                read++;
                bytes += m.getData().length;
                if (key.equals(m.getKey())) {
                    kept++;
                    printEvent(m, "");
                }
                checkDeadline();
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.printf("%n  client read %,d events (%s) to keep %d, in %s%n",
                read, size(bytes), kept, seconds(elapsed));
    }

    /** Act 2: the same question, answered on the server. */
    private void window() throws Exception {
        long start = System.currentTimeMillis();
        long received = 0;
        long bytes = 0;
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(topic)
                     .selector("__key__ = '" + key + "'")
                     .window(TimeWindow.last(lookback))
                     .subscribe()) {
            WindowControl window = (WindowControl) consumer;
            while (!window.isComplete()) {
                Message<byte[]> m = consumer.receive(1, TimeUnit.SECONDS);
                if (m != null) {
                    received++;
                    bytes += m.getData().length;
                    printEvent(m, "");
                    consumer.acknowledge(m);
                }
                checkDeadline();
            }
            long elapsed = System.currentTimeMillis() - start;
            System.out.printf("%n  client received %d events (%s), in %s%n", received, size(bytes), seconds(elapsed));
            printServerStats(window.stats());
        }
    }

    /** Act 3: history, then live. Stops at the first event of type {@code --until}. */
    private void follow() throws Exception {
        String until = opt("until", "delivered");
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(topic)
                     .selector("__key__ = '" + key + "'")
                     .window(TimeWindow.since(lookback))
                     .subscribe()) {
            WindowControl window = (WindowControl) consumer;
            boolean live = false;
            while (true) {
                Message<byte[]> m = consumer.receive(200, TimeUnit.MILLISECONDS);
                if (!live && window.isCaughtUp()) {
                    live = true;
                    System.out.println("  ── caught up: history delivered, now live ──");
                    printServerStats(window.stats());
                    System.out.println();
                }
                if (m != null) {
                    // Live events show how long after they happened they arrived (same host clock).
                    String after = live ? String.format("  +%d ms", System.currentTimeMillis() - eventTime(m)) : "";
                    printEvent(m, after);
                    consumer.acknowledge(m);
                    if (until.equals(m.getProperty("event_type"))) {
                        return;
                    }
                }
                checkDeadline();
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    private Session newSession() throws Exception {
        return Session.builder()
                .serviceUrl(serviceUrl())
                .adminUrl(opt("admin-url", "http://localhost:18080"))
                .controlTopic(opt("control-topic", "persistent://public/default/filter-requests"))
                .privateTopicNamespace("default")
                .build();
    }

    private static void printEvent(Message<byte[]> m, String suffix) {
        System.out.printf("  %s  %-17s seq %s%s%n", TIME.format(Instant.ofEpochMilli(eventTime(m))),
                m.getProperty("event_type"), m.getProperty("seq"), suffix);
    }

    private static void printServerStats(WindowStats stats) {
        if (stats == null) {
            return;
        }
        System.out.printf("  server scanned %,d events (%s) in %s, %s%n", stats.getScanned(),
                size(stats.getBytesScanned()), seconds(stats.getElapsedMs()), stats.getStatus());
    }

    private static long eventTime(Message<byte[]> m) {
        String t = m.getProperty("event_time");
        return t != null ? Long.parseLong(t) : m.getPublishTime();
    }

    private static String size(long bytes) {
        return bytes >= 1_000_000 ? String.format("%.1f MB", bytes / 1e6) : String.format("%.1f KB", bytes / 1e3);
    }

    private static String seconds(long ms) {
        return String.format("%.1f s", ms / 1000.0);
    }

    private void checkDeadline() {
        if (System.currentTimeMillis() > deadline) {
            throw new IllegalStateException("timed out after " + opt("timeout-s", "90") + " s");
        }
    }

    private String serviceUrl() {
        return opt("service-url", "pulsar://localhost:16650");
    }

    private String opt(String name, String def) {
        return opts.getOrDefault(name, def);
    }
}
