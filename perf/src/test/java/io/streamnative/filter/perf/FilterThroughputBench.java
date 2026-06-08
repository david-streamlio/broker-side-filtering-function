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

import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.HdrHistogram.Histogram;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-3 throughput benchmark. Drives end-to-end load through the full SDK → orchestrator →
 * filter pipeline and records achieved rate plus latency percentiles to a JSON file under
 * {@code perf/target/reports/}.
 *
 * <h2>Topology</h2>
 *
 * <ol>
 *   <li>{@code @BeforeAll} brings up a KoP-enabled Pulsar broker and deploys the orchestrator.</li>
 *   <li>The test opens a {@link Session} and {@code subscribe()}s with a selector
 *       ({@code region = 'US'}); the orchestrator spawns a filter function for the input topic.</li>
 *   <li>Once the filter is RUNNING, ShadowTraffic is started against the input topic for
 *       {@link #DURATION_SEC} seconds.</li>
 *   <li>The SDK probe consumer reads matching records off its private output topic, sampling
 *       broker-publish → probe-receive latency into an {@link Histogram}.</li>
 *   <li>At end-of-run, throughput and percentiles are written as
 *       {@code FilterThroughputBench-&lt;git-sha&gt;.json}.</li>
 * </ol>
 *
 * <h2>Tunables (system properties)</h2>
 *
 * <ul>
 *   <li>{@code perf.duration} — total wall-clock seconds for the benchmark window (default 300).</li>
 *   <li>{@code perf.targetRate} — desired input-rate offered load in msg/s (default 50000). Used
 *       to compute {@code throttle_ms} for the ShadowTraffic config and to size {@code maxEvents}.</li>
 *   <li>{@code perf.matchSelector} — selector expression (default {@code region = 'US'}, which
 *       matches ~25% of the four-region payload distribution).</li>
 * </ul>
 *
 * <p>Latency caveat: this measures {@code probe.receive_time - msg.getPublishTime()}, where the
 * publish time is the broker's wall clock when the filter forwarded the matching record to the
 * output topic. That captures broker fanout + probe consumption, not the full input → filter
 * pipeline. Full end-to-end latency requires a correlation id embedded in the payload — see
 * tasks/todo-perf-shadowtraffic.md R5 for the methodology constraint.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(
        named = "SHADOWTRAFFIC_LICENSE_ID",
        matches = ".+",
        disabledReason = "ShadowTraffic license env vars not set; perf suite skipped.")
class FilterThroughputBench extends PerfHarnessIT {

    private static final Logger LOG = LoggerFactory.getLogger(FilterThroughputBench.class);

    // Tenant/namespace/function names — kept consistent with OrchestratorFunctionIT so the
    // broker setup steps mirror the proven flagship IT.
    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-bench";
    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/orchestrator-state";

    // System-property tunables
    private static final int    DURATION_SEC = Integer.getInteger("perf.duration", 300);
    private static final long   TARGET_RATE  = Long.getLong("perf.targetRate", 50_000L);
    private static final String SELECTOR     = System.getProperty("perf.matchSelector", "region = 'US'");

    // Per-run topic so reruns don't trip over a prior subscription. The Pulsar/Kafka topic on
    // KoP is named bare (no namespace prefix) on the Kafka side; KoP maps it under public/default.
    private static final String KAFKA_TOPIC  = "trades-perf-" + UUID.randomUUID().toString().substring(0, 8);
    private static final String INPUT_TOPIC  = "persistent://public/default/" + KAFKA_TOPIC;

    private Network network;
    private GenericContainer<?> pulsar;
    private GenericContainer<?> shadowTraffic;
    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeAll
    void setup() throws Exception {
        requireLicenseEnvOrFail();

        network = Network.newNetwork();
        pulsar = newPulsarKoPContainer(network);
        pulsar.start();
        LOG.info("Pulsar/KoP broker up; binary={}, http={}",
                pulsar.getMappedPort(PULSAR_BINARY_PORT),
                pulsar.getMappedPort(PULSAR_HTTP_PORT));

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
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        uploadFilterPackage(admin, findFilterNar(), FILTER_PACKAGE_URL);
        LOG.info("Uploaded filter NAR as package {}", FILTER_PACKAGE_URL);

        FunctionConfig orchConfig = FunctionConfig.builder()
                .tenant(FN_TENANT)
                .namespace(FN_NAMESPACE)
                .name(ORCH_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.orchestrator.OrchestratorFunction")
                .inputs(List.of(SDK_INPUT_TOPIC))
                .subscriptionPosition(SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .parallelism(1)
                .userConfig(Map.of(
                        "state-topic",           ORCH_STATE_TOPIC,
                        "function-package-url",  FILTER_PACKAGE_URL,
                        "function-tenant",       FN_TENANT,
                        "function-namespace",    FN_NAMESPACE,
                        "function-parallelism",  1,
                        "apply-compaction",      true,
                        "pulsar-admin-url",      "http://localhost:8080"))
                .build();
        deployWithLeaderRetry(admin, orchConfig, findOrchestratorNar().toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 90_000L);
        Thread.sleep(5_000); // orchestrator initialize() needs to open its TableView
        LOG.info("Orchestrator up; ready to register queries");
    }

    @AfterAll
    void teardown() throws Exception {
        if (shadowTraffic != null) shadowTraffic.stop();
        if (client != null) client.close();
        if (admin != null) admin.close();
        if (pulsar != null) pulsar.stop();
        if (network != null) network.close();
    }

    @Test
    void sustainedThroughputAndLatency() throws Exception {
        try (Session session = Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT)
                .privateTopicNamespace(FN_NAMESPACE)
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(DURATION_SEC + 120)
                        .retentionTimeMinutes(Math.max(2, (DURATION_SEC / 60) + 2))
                        .retentionSizeMB(1024)
                        .inactiveDeleteSeconds(DURATION_SEC + 300)
                        .build())
                .build();
             Consumer<byte[]> probe = session.newConsumer()
                     .topic(INPUT_TOPIC)
                     .selector(SELECTOR)
                     .subscribe()) {

            // Wait until the orchestrator has spawned the per-input-topic filter. The spawned
            // name starts with "selector-filter-<sanitized-prefix>-<hash>"; we match by prefix.
            String fnPrefix = "selector-filter-" + KAFKA_TOPIC;
            String spawnedFilterName = waitForSpawnedFilterFunction(fnPrefix, 90_000L);
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, spawnedFilterName, 90_000L);
            Thread.sleep(5_000); // filter's TableView needs to load the QueryMessage
            LOG.info("Filter {} is RUNNING; starting ShadowTraffic", spawnedFilterName);

            // Size the offered load: maxEvents = target_rate * duration, throttle ≈ 1000/rate.
            long offeredLoad = TARGET_RATE * DURATION_SEC;
            long throttleMs = TARGET_RATE > 0 ? Math.max(0L, 1000L / TARGET_RATE) : 0L;
            Path configPath = renderConfig("shadowtraffic/trades-throughput.json", Map.of(
                    "bootstrap",   PULSAR_NETWORK_ALIAS + ":" + KAFKA_PORT,
                    "topic",       KAFKA_TOPIC,
                    "records",     Long.toString(offeredLoad),
                    "throttle_ms", Long.toString(throttleMs)));
            shadowTraffic = newShadowTrafficContainer(network, configPath);
            shadowTraffic.start();
            long shadowStartNanos = System.nanoTime();

            // Sample loop: read output records, record latencies. Loop ends when wall-clock
            // duration elapses; we drain a small grace period so trailing records aren't lost.
            Histogram latencyMs = new Histogram(TimeUnit.SECONDS.toMillis(60), 3);
            long received = 0;
            long deadlineNanos = shadowStartNanos + TimeUnit.SECONDS.toNanos(DURATION_SEC);
            long drainDeadlineNanos = deadlineNanos + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < drainDeadlineNanos) {
                Message<byte[]> msg = probe.receive(2, TimeUnit.SECONDS);
                if (msg == null) {
                    if (System.nanoTime() > deadlineNanos) {
                        break; // drain timeout
                    }
                    continue;
                }
                long latency = Math.max(0, System.currentTimeMillis() - msg.getPublishTime());
                latencyMs.recordValue(Math.min(latency, latencyMs.getHighestTrackableValue()));
                received++;
                probe.acknowledge(msg);
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - shadowStartNanos);
            LOG.info("Run complete: received={} elapsed_ms={}", received, elapsedMs);

            // Achieved input rate is not directly observable without a side-channel probe on
            // the input topic; for the v1 result we report the *offered* load as the target,
            // and infer achieved input from the selector's known match fraction (1/4 for
            // region = 'US' over four uniform regions). Real ShadowTraffic counts can be
            // wired in via its metrics endpoint (port 9400) in a follow-up phase.
            double seconds      = elapsedMs / 1000.0;
            double outputRate   = received / seconds;
            double achievedRate = outputRate * 4.0;            // selector matches 1 of 4 regions
            BenchmarkResult.LatencyMs lat = new BenchmarkResult.LatencyMs(
                    latencyMs.getValueAtPercentile(50.0),
                    latencyMs.getValueAtPercentile(95.0),
                    latencyMs.getValueAtPercentile(99.0),
                    latencyMs.getMaxValue());
            BenchmarkResult result = BenchmarkResult.builder()
                    .benchmark("FilterThroughputBench")
                    .durationSec(DURATION_SEC)
                    .targetRate(TARGET_RATE)
                    .achievedRate(achievedRate)
                    .outputRate(outputRate)
                    .latencyMs(lat)
                    .notes("selector=" + SELECTOR + "; achieved_rate estimated from output_rate "
                            + "× 4 (uniform 4-region payload). Replace with ShadowTraffic /metrics "
                            + "scrape in a later phase. Probe received " + received + " records "
                            + "in " + elapsedMs + " ms wall-clock.")
                    .build();
            Path report = result.writeJson();
            LOG.info("Benchmark result written to {}", report);

            assertThat(received).as("probe must receive at least one record").isGreaterThan(0);
        }
    }

    /**
     * Polls the function-list endpoint for a function whose name starts with {@code prefix}.
     * Returns the matched name once found, or throws on timeout.
     */
    private String waitForSpawnedFilterFunction(String prefix, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (String fn : admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE)) {
                if (fn.startsWith(prefix)) {
                    return fn;
                }
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Orchestrator did not spawn a filter for prefix=" + prefix
                + " within " + timeoutMs + " ms");
    }
}
