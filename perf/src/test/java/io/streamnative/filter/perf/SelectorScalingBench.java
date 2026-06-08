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

package io.streamnative.filter.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.FunctionInstanceStats;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Value-add latency benchmark, span (b): {@code process()} entry → output published, as a function
 * of registered-selector count. Unlike {@link FilterThroughputBench} this uses a <b>plain Pulsar
 * producer</b> (no ShadowTraffic, no license) at a fixed <i>sub-saturation</i> offered rate, so the
 * measurement isolates the function's own value-add latency rather than queueing delay at capacity.
 *
 * <h2>What it measures</h2>
 *
 * It reads the filter function's own per-message metrics (worker-rendered percentiles), not a
 * probe-side end-to-end latency:
 * <ul>
 *   <li>{@code filter.dispatch.time.ms} — (b1) entry → all sends dispatched (deserialize + eval
 *       loop + build + enqueue): the CPU value-add.</li>
 *   <li>{@code filter.match.processing.time.ms} — (b) entry → all sends acked: (b1) + output-send
 *       round-trip. {@code match − dispatch} isolates the send component.</li>
 * </ul>
 *
 * <h2>Experiment</h2>
 *
 * Registers {@code perf.selectorCount} selectors on one input topic — exactly one matching
 * ({@code region = 'US'}) and the rest non-matching — so every message is evaluated against all N
 * (the eval loop runs N times) while fanout stays at 1. Run at 10 and 100 selectors and compare:
 * if (b1) is flat across N, the eval loop is irrelevant and you're send-bound; if (b1) grows with
 * N, the eval loop is the lever.
 *
 * <h2>Tunables (system properties)</h2>
 * <ul>
 *   <li>{@code perf.selectorCount} — registered selectors (default 100).</li>
 *   <li>{@code perf.targetRate} — offered load in msg/s; keep well below saturation (default 2000).</li>
 *   <li>{@code perf.duration} — measured load seconds (default 60).</li>
 *   <li>{@code perf.payloadBytes} — message body size (default 256).</li>
 * </ul>
 *
 * Run: {@code mvn -Pperf -pl perf -am verify -Dit.test=SelectorScalingBench -Dperf.selectorCount=100}.
 * Needs a capable host (the broker + spawned function must cold-start in time).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectorScalingBench extends PerfHarnessIT {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorScalingBench.class);

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-scaling-bench";
    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/scaling-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/scaling-orchestrator-state";

    private static final String KAFKA_TOPIC = "scaling-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String INPUT_TOPIC = "persistent://public/default/" + KAFKA_TOPIC;

    private static final int  SELECTOR_COUNT = Integer.getInteger("perf.selectorCount", 100);
    private static final long TARGET_RATE    = Long.getLong("perf.targetRate", 2_000L);
    private static final int  DURATION_SEC   = Integer.getInteger("perf.duration", 60);
    private static final int  PAYLOAD_BYTES  = Integer.getInteger("perf.payloadBytes", 256);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Network network;
    private GenericContainer<?> pulsar;
    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeAll
    void setup() throws Exception {
        network = Network.newNetwork();
        pulsar = newPulsarKoPContainer(network);
        pulsar.start();

        client = PulsarClient.builder()
                .serviceUrl("pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(PULSAR_BINARY_PORT))
                .build();
        admin = PulsarAdmin.builder()
                .serviceHttpUrl("http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(PULSAR_HTTP_PORT))
                .build();

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_TOPIC);
        admin.namespaces().setAutoTopicCreation("public/default",
                AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true).topicType("non-partitioned").build());

        uploadFilterPackage(admin, findFilterNar(), FILTER_PACKAGE_URL);

        FunctionConfig orchConfig = FunctionConfig.builder()
                .tenant(FN_TENANT).namespace(FN_NAMESPACE).name(ORCH_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.orchestrator.OrchestratorFunction")
                .inputs(List.of(SDK_INPUT_TOPIC))
                .subscriptionPosition(SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .parallelism(1)
                .userConfig(Map.of(
                        "state-topic",          ORCH_STATE_TOPIC,
                        "function-package-url", FILTER_PACKAGE_URL,
                        "function-tenant",      FN_TENANT,
                        "function-namespace",   FN_NAMESPACE,
                        "function-parallelism", 1,
                        "apply-compaction",     true,
                        "pulsar-admin-url",     "http://localhost:8080"))
                .build();
        deployWithLeaderRetry(admin, orchConfig, findOrchestratorNar().toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 150_000L);
        Thread.sleep(5_000);
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (admin != null) admin.close();
        if (pulsar != null) pulsar.stop();
        if (network != null) network.close();
    }

    @Test
    void valueAddLatencyByScale() throws Exception {
        try (Session session = Session.builder()
                .pulsarClient(client).pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT).privateTopicNamespace(FN_NAMESPACE)
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(DURATION_SEC + 300)
                        .retentionTimeMinutes(Math.max(2, (DURATION_SEC / 60) + 5))
                        .retentionSizeMB(512)
                        .inactiveDeleteSeconds(DURATION_SEC + 600)
                        .build())
                .build()) {

            // Register N selectors on ONE input topic: 1 matching + (N-1) non-matching, so every
            // message is evaluated against all N (eval loop runs N times) but fanout stays at 1.
            session.registerPrivateQuery(List.of(INPUT_TOPIC), "region = 'US'");
            for (int i = 1; i < SELECTOR_COUNT; i++) {
                session.registerPrivateQuery(List.of(INPUT_TOPIC), "region = 'NO_" + i + "'");
            }
            LOG.info("Registered {} selectors on {}", SELECTOR_COUNT, INPUT_TOPIC);

            String filterName = waitForSpawnedFilter(150_000L);
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, filterName, 150_000L);
            waitForQueriesLoaded(filterName, SELECTOR_COUNT, 120_000L);
            LOG.info("Filter {} RUNNING with {} queries loaded", filterName, SELECTOR_COUNT);

            // Sub-saturation paced load: a fixed offered rate, fixed-size payload, region=US so the
            // single matching selector fires (fanout 1).
            byte[] payload = new byte[PAYLOAD_BYTES];
            try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                    .topic(INPUT_TOPIC).blockIfQueueFull(true).create()) {

                long intervalNanos = TARGET_RATE > 0 ? 1_000_000_000L / TARGET_RATE : 0L;
                long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(DURATION_SEC);
                long nextNanos = System.nanoTime();
                long sent = 0;
                while (System.nanoTime() < endNanos) {
                    producer.newMessage().key("k" + (sent & 63)).property("region", "US")
                            .value(payload).sendAsync();
                    sent++;
                    if (intervalNanos > 0) {
                        nextNanos += intervalNanos;
                        long park = nextNanos - System.nanoTime();
                        if (park > 0) LockSupport.parkNanos(park);
                    }
                }
                producer.flush();
                LOG.info("Produced {} messages at offered rate {} msg/s over {}s", sent, TARGET_RATE, DURATION_SEC);

                // Let the function drain in-flight work + the worker collect a fresh stats snapshot.
                awaitMessagesProcessed(filterName, (long) (sent * 0.95), 60_000L);

                Map<String, Double> dispatch = readUserMetrics(filterName, "filter.dispatch.time.ms");
                Map<String, Double> match    = readUserMetrics(filterName, "filter.match.processing.time.ms");
                Map<String, Double> proc     = readUserMetrics(filterName, "filter.processing.time.ms");

                logPercentiles("dispatch (b1: code value-add)", dispatch);
                logPercentiles("match    (b : code + output-send)", match);

                writeReport(sent, dispatch, match, proc);

                // Sanity: the value-add metrics were actually populated for matched records.
                assertThat(count(dispatch)).as("dispatch samples recorded").isGreaterThan(0.0);
                assertThat(count(match)).as("match-processing samples recorded").isGreaterThan(0.0);
            }
        }
    }

    // ---------------------------------------------------------------- helpers

    private String waitForSpawnedFilter(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (String fn : admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE)) {
                if (fn.startsWith("selector-filter-") && !fn.equals(ORCH_NAME)) {
                    return fn;
                }
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Orchestrator did not spawn a filter within " + timeoutMs + " ms");
    }

    /** Polls {@code filter.queries.loaded} (a +1-per-load counter) until it reaches {@code n}. */
    private void waitForQueriesLoaded(String filterName, int n, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        double loaded = 0;
        while (System.currentTimeMillis() < deadline) {
            loaded = metric(filterName, "user_metric_filter.queries.loaded_count");
            if (loaded >= n) return;
            Thread.sleep(1_000);
        }
        throw new IllegalStateException("Filter loaded only " + loaded + "/" + n + " queries within "
                + timeoutMs + " ms");
    }

    private void awaitMessagesProcessed(String filterName, long target, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (metric(filterName, "user_metric_filter.messages.in_sum") >= target) return;
            Thread.sleep(2_000);
        }
        LOG.warn("Message-processed wait timed out (target {}); reading metrics anyway", target);
    }

    /** Sum a single userMetric key across all instances (parallelism 1 → one instance). */
    private double metric(String filterName, String key) {
        try {
            FunctionStats stats = admin.functions().getFunctionStats(FN_TENANT, FN_NAMESPACE, filterName);
            double total = 0;
            for (FunctionInstanceStats inst : stats.getInstances()) {
                Map<String, Double> um = inst.getMetrics().getUserMetrics();
                if (um != null && um.get(key) != null) total += um.get(key);
            }
            return total;
        } catch (Exception e) {
            return 0.0;
        }
    }

    /** All userMetric entries (sum/count/percentiles) for a metric name, keyed by the suffix. */
    private Map<String, Double> readUserMetrics(String filterName, String metricName) {
        Map<String, Double> out = new LinkedHashMap<>();
        try {
            FunctionStats stats = admin.functions().getFunctionStats(FN_TENANT, FN_NAMESPACE, filterName);
            String prefix = "user_metric_" + metricName;
            for (FunctionInstanceStats inst : stats.getInstances()) {
                Map<String, Double> um = inst.getMetrics().getUserMetrics();
                if (um == null) continue;
                for (Map.Entry<String, Double> e : um.entrySet()) {
                    if (e.getKey().startsWith(prefix)) {
                        out.merge(e.getKey().substring("user_metric_".length()), e.getValue(), Double::sum);
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("readUserMetrics({}) failed: {}", metricName, e.getMessage());
        }
        return out;
    }

    private static double count(Map<String, Double> m) {
        return m.entrySet().stream().filter(e -> e.getKey().endsWith("_count"))
                .mapToDouble(Map.Entry::getValue).findFirst().orElse(0.0);
    }

    private void logPercentiles(String label, Map<String, Double> m) {
        LOG.info("[{}] selectors={} rate={}msg/s  count={}  p50={}  p99={}  p999={}  max={}",
                label, SELECTOR_COUNT, TARGET_RATE, count(m),
                pick(m, "_0.5"), pick(m, "_0.99"), pick(m, "_0.999"), pick(m, "_1.0"));
    }

    private static String pick(Map<String, Double> m, String suffix) {
        for (Map.Entry<String, Double> e : m.entrySet()) {
            if (e.getKey().endsWith(suffix)) return String.format("%.3f", e.getValue());
        }
        return "n/a";
    }

    private void writeReport(long sent, Map<String, Double> dispatch, Map<String, Double> match,
                             Map<String, Double> proc) throws Exception {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("benchmark", "SelectorScalingBench");
        report.put("selectorCount", SELECTOR_COUNT);
        report.put("targetRateMsgPerSec", TARGET_RATE);
        report.put("durationSec", DURATION_SEC);
        report.put("payloadBytes", PAYLOAD_BYTES);
        report.put("messagesProduced", sent);
        report.put("dispatch_b1_ms", dispatch);
        report.put("match_processing_b_ms", match);
        report.put("processing_ms", proc);
        Path dir = Paths.get(System.getProperty("user.dir"), "target", "reports");
        Files.createDirectories(dir);
        Path out = dir.resolve("SelectorScalingBench-" + SELECTOR_COUNT + "sel.json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(out.toFile(), report);
        LOG.info("Report written to {}", out);
    }
}
