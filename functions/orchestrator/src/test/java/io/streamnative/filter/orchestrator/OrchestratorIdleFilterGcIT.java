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

package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.FilterRequest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionInstanceStats;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live IT for idle-filter GC. Uses a dedicated testcontainer + orchestrator deployment with GC
 * enabled and short TTL/sweep intervals so the tests finish in well under a minute. Runs in its
 * own class so the GC-disabled tests in {@link OrchestratorFunctionIT} (which depend on filter
 * functions persisting across tests) are not affected.
 *
 * <p>Asserts the two scenarios that matter operationally:
 * <ol>
 *   <li>A filter function whose only queryId UNREGISTERs is deleted after the idle TTL elapses
 *       (function removed, control topic removed, registry entry retracted).</li>
 *   <li>A REGISTER arriving inside the TTL window resets the idle clock — the filter survives.</li>
 * </ol>
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrchestratorIdleFilterGcIT {

    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorIdleFilterGcIT.class);

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-gc-it";

    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/gc-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/gc-orchestrator-state";

    // Each test uses a distinct input topic so we have an unambiguous spawned-filter name to track.
    private static final String INPUT_GC      = "persistent://public/default/gc-input";
    private static final String INPUT_REVIVED = "persistent://public/default/gc-revived-input";
    private static final String INPUT_DEAD    = "persistent://public/default/gc-dead-input";

    // Tight enough to finish a test pass in <30s, loose enough that incidental jitter doesn't
    // flake the test 2 "revived" check. Heartbeat staleness is aggressive (15s = 5 missed
    // heartbeats at the test SDK interval below) so the dead-consumer test finishes in well
    // under a minute; existing tests keep their q: entries fresh by heartbeating during
    // the wait.
    private static final long IDLE_TTL_SECONDS              = 5L;
    private static final long SWEEP_INTERVAL_SECONDS        = 2L;
    private static final long HEARTBEAT_MAX_STALE_SECONDS   = 15L;
    private static final long SDK_HEARTBEAT_INTERVAL_SECONDS = 3L;

    private static final Path FILTER_NAR       = findFilterNar();
    private static final Path ORCHESTRATOR_NAR = findOrchestratorNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.1"))
            .withFunctionsWorker()
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PulsarClient client;
    private PulsarAdmin admin;
    private Producer<byte[]> producerGc;
    private Producer<byte[]> producerRevived;
    private Producer<byte[]> producerDead;
    /**
     * Producer for the orchestrator's input topic. Used by {@link #awaitMetricDelta} to publish
     * no-op HEARTBEATs after the Session under test has closed — that drives process() invocations
     * which drain the deferred sweeper-metrics map (R1 fallback path). Without this, post-GC
     * metric assertions would time out since no one else publishes to the orchestrator after the
     * Session closes.
     */
    private Producer<byte[]> orchestratorDrainProducer;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();

        // The Pulsar 4.2 image opens its admin port before the broker finishes provisioning
        // public/default, so creating topics immediately can fail with "Namespace not found".
        awaitDefaultNamespace();

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_GC);
        admin.topics().createNonPartitionedTopic(INPUT_REVIVED);
        admin.topics().createNonPartitionedTopic(INPUT_DEAD);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for GC IT")
                .contact("ci@localhost")
                .build();
        admin.packages().upload(pkg, FILTER_PACKAGE_URL, FILTER_NAR.toString());

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
                .userConfig(Map.ofEntries(
                        Map.entry(OrchestratorConfig.KEY_STATE_TOPIC,             ORCH_STATE_TOPIC),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PACKAGE_URL,    FILTER_PACKAGE_URL),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_TENANT,         FN_TENANT),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_NAMESPACE,      FN_NAMESPACE),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PARALLELISM,    1),
                        Map.entry(OrchestratorConfig.KEY_APPLY_COMPACTION,        true),
                        Map.entry(OrchestratorConfig.KEY_PULSAR_ADMIN_URL,        "http://localhost:8080"),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_ENABLED,         true),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_TTL_SECONDS,     IDLE_TTL_SECONDS),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_SWEEP_INTERVAL,  SWEEP_INTERVAL_SECONDS),
                        Map.entry(OrchestratorConfig.KEY_HEARTBEAT_MAX_STALE,     HEARTBEAT_MAX_STALE_SECONDS)))
                .build();
        deployWithLeaderRetry(admin, orchConfig, ORCHESTRATOR_NAR.toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 90_000L);
        Thread.sleep(5_000); // grace for TableView open + sweeper start

        producerGc      = client.newProducer(Schema.BYTES).topic(INPUT_GC).create();
        producerRevived = client.newProducer(Schema.BYTES).topic(INPUT_REVIVED).create();
        producerDead    = client.newProducer(Schema.BYTES).topic(INPUT_DEAD).create();
        orchestratorDrainProducer = client.newProducer(Schema.BYTES).topic(SDK_INPUT_TOPIC).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (producerGc != null) producerGc.close();
        if (producerRevived != null) producerRevived.close();
        if (producerDead != null) producerDead.close();
        if (orchestratorDrainProducer != null) orchestratorDrainProducer.close();
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    private Session newSession() throws Exception {
        return newSession(client);
    }

    private Session newSession(PulsarClient sessionClient) throws Exception {
        // Short heartbeat interval so an open Session keeps its q: entry fresh during the
        // existing-tests' wait windows (HEARTBEAT_MAX_STALE_SECONDS = 15s).
        return Session.builder()
                .pulsarClient(sessionClient)
                .pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT)
                .privateTopicNamespace(FN_NAMESPACE)
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(60)
                        .retentionTimeMinutes(1)
                        .retentionSizeMB(8)
                        .inactiveDeleteSeconds(60)
                        .build())
                .heartbeatIntervalSeconds(SDK_HEARTBEAT_INTERVAL_SECONDS)
                .build();
    }

    // ---------------------------------------------------------------- Tests

    @Test
    void idleFilterIsDeletedAfterTtl() throws Exception {
        String expectedFilterFn  = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_GC);
        String expectedCtrlTopic = "persistent://" + FN_TENANT + "/" + FN_NAMESPACE
                + "/ctrl-" + TopicKeySanitizer.forInputTopic(INPUT_GC);

        // Snapshot the sweeper metrics so we can assert deltas — PER_CLASS lifecycle means tests
        // share an orchestrator instance, and metric counters carry across tests.
        double baselineEvaluated = getOrchestratorUserMetric("orchestrator.gc.candidates_evaluated");
        double baselineDeleted   = getOrchestratorUserMetric("orchestrator.gc.functions_deleted");

        // Phase 1: REGISTER + drive a message so we know the filter is genuinely alive.
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_GC)
                     .selector("region = 'US'")
                     .subscribe()) {

            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000);

            producerGc.newMessage().key("AAPL").property("region", "US")
                    .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
            Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(m).as("filter must be live before UNREGISTER").isNotNull();
            assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("us-1");
            consumer.acknowledge(m);
        }
        // consumer.close() → orchestrator UNREGISTER → tombstone → activeQueryIds becomes empty
        // → idleSince stamped → after TTL the sweeper deletes function + control topic + t: entry.

        long deadline = System.currentTimeMillis()
                + TimeUnit.SECONDS.toMillis(IDLE_TTL_SECONDS + SWEEP_INTERVAL_SECONDS * 3 + 10);
        boolean functionGone = false;
        boolean topicGone    = false;
        while (System.currentTimeMillis() < deadline) {
            List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
            functionGone = !functions.contains(expectedFilterFn);
            try {
                admin.topics().getStats(expectedCtrlTopic);
                topicGone = false;
            } catch (PulsarAdminException.NotFoundException e) {
                topicGone = true;
            }
            if (functionGone && topicGone) break;
            Thread.sleep(500);
        }
        assertThat(functionGone).as("idle filter function should be GCed after TTL").isTrue();
        assertThat(topicGone).as("control topic should be GCed after TTL").isTrue();

        // R1 verification: the sweeper records via OrchestratorFunction.recordSweeperMetric which
        // accumulates into a pending map; the next process() invocation flushes to ctx.recordMetric
        // on the correct thread. If the wiring breaks, deltas stay at 0.
        awaitMetricDelta("orchestrator.gc.candidates_evaluated", baselineEvaluated, 1.0, 15_000);
        awaitMetricDelta("orchestrator.gc.functions_deleted",    baselineDeleted,   1.0, 15_000);
    }

    @Test
    void recentReRegisterPreventsGc() throws Exception {
        String expectedFilterFn = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_REVIVED);

        double baselineEvaluated = getOrchestratorUserMetric("orchestrator.gc.candidates_evaluated");
        double baselineDeleted   = getOrchestratorUserMetric("orchestrator.gc.functions_deleted");

        // Phase 1: REGISTER, then close (UNREGISTER). Filter goes idle.
        try (Session firstSession = newSession();
             Consumer<byte[]> firstConsumer = firstSession.newConsumer()
                     .topic(INPUT_REVIVED)
                     .selector("region = 'US'")
                     .subscribe()) {
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000);
        }

        // Phase 2: re-REGISTER within the TTL window. The orchestrator processes a new REGISTER,
        // adds the new queryId to activeQueryIds, clears idleSince. The sweeper must skip this
        // entry on every subsequent tick.
        try (Session secondSession = newSession();
             Consumer<byte[]> secondConsumer = secondSession.newConsumer()
                     .topic(INPUT_REVIVED)
                     .selector("region = 'UK'")
                     .subscribe()) {

            Thread.sleep(3_000); // orchestrator processes the new REGISTER, reuses the filter

            // Wait well past TTL + several sweep ticks. If GC was wrongly triggered, the function
            // is gone by now.
            Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_TTL_SECONDS + SWEEP_INTERVAL_SECONDS * 3 + 5));

            List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
            assertThat(functions).as("re-REGISTER inside the TTL window must keep the filter alive")
                    .contains(expectedFilterFn);

            // Sanity: the live filter still processes input. UK matches should flow.
            producerRevived.newMessage().key("BARC").property("region", "UK")
                    .value("uk-revived".getBytes(StandardCharsets.UTF_8)).send();
            Message<byte[]> m = secondConsumer.receive(30, TimeUnit.SECONDS);
            assertThat(m).isNotNull();
            assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("uk-revived");
            secondConsumer.acknowledge(m);

            // Metric assertions while filter is still active:
            // - candidates_evaluated grew: the sweeper definitely ran (multiple ticks during the
            //   TTL+sweep wait), and the messages we sent to handleHeartbeat / handleRegister gave
            //   process() multiple opportunities to flush the pending map.
            // - functions_deleted did NOT grow: re-REGISTER reset idleSince so no GC happened.
            //   Tests run sequentially under PER_CLASS so no other test could delete a filter
            //   inside this window.
            awaitMetricDelta("orchestrator.gc.candidates_evaluated", baselineEvaluated, 1.0, 15_000);
            double deletedNow = getOrchestratorUserMetric("orchestrator.gc.functions_deleted");
            assertThat(deletedNow - baselineDeleted)
                    .as("re-REGISTER inside the TTL window must not trigger GC")
                    .isEqualTo(0.0);
        }
    }

    @Test
    void deadConsumerTriggersSynthesizedUnregister() throws Exception {
        String expectedFilterFn  = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_DEAD);
        String expectedCtrlTopic = "persistent://" + FN_TENANT + "/" + FN_NAMESPACE
                + "/ctrl-" + TopicKeySanitizer.forInputTopic(INPUT_DEAD);

        double baselineSynthesized = getOrchestratorUserMetric("orchestrator.heartbeats.synthesized");
        double baselineDeleted     = getOrchestratorUserMetric("orchestrator.gc.functions_deleted");

        // Build a doomed PulsarClient that we will hard-close to simulate an SDK crash. Its
        // Session never gets close() called, so no clean UNREGISTER is published; instead the
        // orchestrator must detect the stale heartbeat and synthesize one.
        PulsarClient doomedClient = PulsarClient.builder()
                .serviceUrl(pulsar.getPulsarBrokerUrl())
                .build();

        Session doomedSession = newSession(doomedClient);
        try {
            doomedSession.registerPrivateQuery(List.of(INPUT_DEAD), "region = 'US'");
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000); // grace for filter TableView to load the QueryMessage

            // Drive one input message to confirm the filter is genuinely alive before we kill
            // the client. We do not consume the output — proving the wire is intact requires
            // only that the input publish succeeds.
            producerDead.newMessage().key("AAPL").property("region", "US")
                    .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
            Thread.sleep(2_000);
        } finally {
            // Hard-kill the client without going through Session.close() (which would call
            // retirePrivateQuery and publish a clean UNREGISTER). The Session is left dangling.
            // Heartbeats from its scheduler will fail silently against the closed client.
            doomedClient.close();
        }

        // Total time from kill to GC: max-stale (15s) + idle-ttl (5s) + sweep slack. Poll up to
        // a generous deadline.
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(
                HEARTBEAT_MAX_STALE_SECONDS + IDLE_TTL_SECONDS + SWEEP_INTERVAL_SECONDS * 4 + 15);
        boolean functionGone = false;
        boolean topicGone    = false;
        while (System.currentTimeMillis() < deadline) {
            List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
            functionGone = !functions.contains(expectedFilterFn);
            try {
                admin.topics().getStats(expectedCtrlTopic);
                topicGone = false;
            } catch (PulsarAdminException.NotFoundException e) {
                topicGone = true;
            }
            if (functionGone && topicGone) break;
            Thread.sleep(1_000);
        }
        assertThat(functionGone).as(
                "filter function should be GCed after heartbeat staleness + idle TTL").isTrue();
        assertThat(topicGone).as(
                "control topic should be GCed after heartbeat staleness + idle TTL").isTrue();

        // R1 verification on the heartbeat path: the synthesize-unregister metric fires on the
        // sweep tick that detected the stale q: entry, and the gc.functions_deleted metric fires
        // on the later sweep tick that GCed the now-idle t: entry. Both surface via the
        // deferred-flush path on the next process() call.
        awaitMetricDelta("orchestrator.heartbeats.synthesized", baselineSynthesized, 1.0, 15_000);
        awaitMetricDelta("orchestrator.gc.functions_deleted",   baselineDeleted,     1.0, 15_000);
    }

    // ---------------------------------------------------------------- helpers (mirrored from OrchestratorFunctionIT)

    private static void deployWithLeaderRetry(PulsarAdmin admin, FunctionConfig config, String pkg,
                                              String tenant, String ns, String name) throws Exception {
        String packageUrl = "function://" + tenant + "/" + ns + "/" + name + "@0";
        long deadline = System.currentTimeMillis() + 90_000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.functions().createFunction(config, pkg);
                return;
            } catch (PulsarAdminException e) {
                last = e;
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (msg.contains("Leader not yet ready") || msg.contains("Service Unavailable")
                        || msg.contains("already exists")) {
                    try {
                        admin.packages().delete(packageUrl);
                    } catch (Exception ignored) {}
                    Thread.sleep(1_000);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness: " + name, last);
    }

    /**
     * Sum the running counter for a userMetric across all instances of the orchestrator function.
     *
     * <p>Pulsar's function-worker renders every {@code ctx.recordMetric(name, value)} call as a
     * summary statistic — the {@code userMetrics} map exposes keys like
     * {@code user_metric_<name>_sum}, {@code _count}, {@code _0.5}, {@code _0.9}, {@code _0.99},
     * {@code _0.999}. The {@code _sum} variant is the sum of all values recorded, which is the
     * counter semantics we want. Returns 0.0 if the metric was never recorded or if the stats
     * fetch fails (typically just before the function worker has flushed instance stats — the
     * caller is expected to poll).
     */
    private double getOrchestratorUserMetric(String name) {
        String pulsarKey = "user_metric_" + name + "_sum";
        try {
            FunctionStats stats = admin.functions().getFunctionStats(FN_TENANT, FN_NAMESPACE, ORCH_NAME);
            double total = 0.0;
            for (FunctionInstanceStats inst : stats.getInstances()) {
                Map<String, Double> userMetrics = inst.getMetrics().getUserMetrics();
                if (userMetrics != null) {
                    Double v = userMetrics.get(pulsarKey);
                    if (v != null) total += v;
                }
            }
            return total;
        } catch (Exception e) {
            return 0.0;
        }
    }

    /**
     * Poll until {@code getOrchestratorUserMetric(name) - baseline >= minDelta}, or fail. Used at
     * the tail of each test to confirm sweeper metrics actually surfaced via the deferred-flush
     * path through {@code OrchestratorFunction.process()} — this is the R1 verification promised
     * by tasks/todo-sweeper-observability.md.
     *
     * <p>Drives a no-op HEARTBEAT into the orchestrator every 2s so process() runs and drains the
     * pending sweeper-metrics map. Without this, the closed-Session tests would hang: the sweeper
     * tick that incremented the counter happens on the scheduler thread, but
     * {@code ctx.recordMetric} only sticks when called from the process loop (R1).
     */
    private void awaitMetricDelta(String name, double baseline, double minDelta, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastDrain = 0L;
        double last = 0.0;
        while (System.currentTimeMillis() < deadline) {
            last = getOrchestratorUserMetric(name);
            if (last - baseline >= minDelta) return;
            if (System.currentTimeMillis() - lastDrain > 2_000) {
                publishNoOpHeartbeat();
                lastDrain = System.currentTimeMillis();
            }
            Thread.sleep(500);
        }
        // Dump every userMetric we can see, plus the function-instance counters that prove
        // process() ran at all. If even the process-thread metrics are absent here, the
        // function-worker's metricsCollectionInterval hasnt fired yet and the test needs a
        // longer timeout. If those are present but ours are not, the recordMetric path is broken.
        StringBuilder dump = new StringBuilder("userMetrics seen:\n");
        try {
            FunctionStats stats = admin.functions().getFunctionStats(FN_TENANT, FN_NAMESPACE, ORCH_NAME);
            dump.append("  receivedTotal=").append(stats.getReceivedTotal())
                .append(" processedSuccessfullyTotal=").append(stats.getProcessedSuccessfullyTotal())
                .append('\n');
            for (FunctionInstanceStats inst : stats.getInstances()) {
                Map<String, Double> userMetrics = inst.getMetrics().getUserMetrics();
                dump.append("  instance ").append(inst.getInstanceId()).append(" userMetrics=")
                    .append(userMetrics).append('\n');
            }
        } catch (Exception e) {
            dump.append("  failed to fetch diagnostics: ").append(e.getMessage());
        }
        throw new AssertionError(String.format(
                "metric %s did not grow by at least %s within %d ms; baseline=%s last=%s\n%s",
                name, minDelta, timeoutMs, baseline, last, dump));
    }

    /**
     * Publish a HEARTBEAT with a single unknown queryId. handleHeartbeat treats unknown queryIds
     * as a silent no-op (counted under {@code orchestrator.heartbeats.unknown}), but the act of
     * invoking process() drains pending sweeper metrics through the proper thread.
     */
    private void publishNoOpHeartbeat() {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(
                    FilterRequest.heartbeat(List.of("metric-drain-" + System.nanoTime())));
            orchestratorDrainProducer.newMessage()
                    .key("drain")
                    .value(payload)
                    .send();
        } catch (Exception e) {
            // Swallow — the next iteration of awaitMetricDelta will retry; if drains keep failing
            // the deadline check will surface a meaningful assertion failure.
            LOG.debug("no-op HEARTBEAT publish failed: {}", e.getMessage());
        }
    }

    private static void waitForFunctionRunning(PulsarAdmin admin, String tenant, String ns,
                                               String name, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                FunctionStatus status = admin.functions().getFunctionStatus(tenant, ns, name);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {}/{}/{} is RUNNING", tenant, ns, name);
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        throw new IllegalStateException("Function did not reach RUNNING within " + timeoutMs + " ms: " + name);
    }

    /**
     * Poll until {@code public/default} is provisioned. The 4.2 testcontainer's admin port
     * comes up before the broker finishes bootstrapping the default namespace.
     */
    private void awaitDefaultNamespace() throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            try {
                if (admin.namespaces().getNamespaces("public").contains("public/default")) {
                    return;
                }
            } catch (PulsarAdminException e) {
                // admin not fully ready yet — keep polling
            }
            TimeUnit.SECONDS.sleep(1);
        }
        throw new IllegalStateException("public/default namespace not available after 60s");
    }

    private static Path findFilterNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "..", "selector-filter", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-function-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Filter NAR not found in " + target);
        }
        return files[0].toPath();
    }

    private static Path findOrchestratorNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "target");
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-orchestrator-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Orchestrator NAR not found in " + target);
        }
        return files[0].toPath();
    }
}
