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

package io.streamnative.filter.client;

import io.streamnative.filter.api.FilterControl;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end IT for {@code .window(TimeWindow)} on the Pulsar SDK, against a live broker with the
 * orchestrator deployed (same setup as {@link SessionIT}).
 *
 * <p>Each test seeds its own source topic with three phases (A, B, C) separated by gaps, records a
 * timestamp in each gap ({@code t1}, {@code t2}), then reads a window and checks the exact matches in
 * order, the end-of-history marker (never returned as data), the server's stats, and that an empty
 * receive before the marker doesn't report the window done.
 *
 * <p>Requires Docker and pre-built NARs — see {@link SessionIT}.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WindowedSessionIT {

    private static final Logger LOG = LoggerFactory.getLogger(WindowedSessionIT.class);

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-windowit";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";

    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/window-it-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/window-it-orchestrator-state";

    /** Every third message of a phase has {@code region = 'r07'}. */
    private static final String SELECTOR  = "region = 'r07'";
    private static final int    PER_PHASE = 30;
    private static final long   GAP_MS    = 1_500L;

    private static final Path FILTER_NAR       = findNar("selector-filter", "selector-filter-function-");
    private static final Path ORCHESTRATOR_NAR = findNar("orchestrator", "selector-filter-orchestrator-");

    // A seeded source topic has no producer or subscription until the window's subscription is
    // created; keep the broker from deleting it as inactive on a slow runner.
    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.1"))
            .withFunctionsWorker()
            .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();
        waitForDefaultNamespace(admin);

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for WindowedSessionIT")
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
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .parallelism(1)
                .userConfig(Map.of(
                        "state-topic",            ORCH_STATE_TOPIC,
                        "function-package-url",   FILTER_PACKAGE_URL,
                        "function-tenant",        FN_TENANT,
                        "function-namespace",     FN_NAMESPACE,
                        "function-parallelism",   1,
                        "apply-compaction",       true,
                        "pulsar-admin-url",       "http://localhost:8080"))
                .build();
        deployWithLeaderRetry(orchConfig, ORCHESTRATOR_NAR.toString());
        waitForFunctionRunning(ORCH_NAME, 90_000L);
        Thread.sleep(5_000); // orchestrator initialize() needs to open its TableView
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    // ---------------------------------------------------------------- Tests

    @Test
    void boundedWindowDeliversExactlyItsMatchesThenCompletes() throws Exception {
        Seeded s = seed("bounded");
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(s.topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.between(Instant.ofEpochMilli(s.t1), Instant.ofEpochMilli(s.t2)))
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            assertThat(window.isOpenEnded()).isFalse();
            assertThatThrownBy(() -> ((FilterControl) consumer).setSelector("region = 'r01'"))
                    .isInstanceOf(UnsupportedOperationException.class);

            Drain d = drainUntil(consumer, window::isComplete, 120_000L);

            assertThat(d.emptyBeforeDone)
                    .as("empty receives before the completion record must not report the window done")
                    .isPositive();
            assertThat(d.values).containsExactlyElementsOf(s.phaseB);
            assertThat(d.missingSourceProps).isZero();

            WindowStats stats = window.stats();
            assertThat(stats).isNotNull();
            assertThat(stats.getMatched()).isEqualTo(s.phaseB.size());
            assertThat(stats.getScanned()).isGreaterThanOrEqualTo(PER_PHASE);
            assertThat(stats.getEndedBy()).isEqualTo(WindowStats.EndedBy.WINDOW_END);
            assertThat(stats.getStatus()).isEqualTo(WindowStats.Status.COMPLETE);
            assertThat(window.isCaughtUp()).isTrue();
            assertThat(window.caughtUp()).isDone();

            assertThat(consumer.receive(1, TimeUnit.SECONDS)).isNull();
            assertThatThrownBy(consumer::receive).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void openEndedWindowCatchesUpThenStreamsLiveMatches() throws Exception {
        Seeded s = seed("open");
        try (Session session = newSession();
             Producer<byte[]> producer = newProducer(s.topic);
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(s.topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.since(Instant.ofEpochMilli(s.t1)))
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            assertThat(window.isOpenEnded()).isTrue();

            Drain history = drainUntil(consumer, window::isCaughtUp, 120_000L);

            assertThat(history.emptyBeforeDone)
                    .as("empty receives before the caught-up record must not report the window caught up")
                    .isPositive();
            List<String> expectedHistory = new ArrayList<>(s.phaseB);
            expectedHistory.addAll(s.phaseC);
            assertThat(history.values).containsExactlyElementsOf(expectedHistory);

            WindowStats stats = window.caughtUp().get(5, TimeUnit.SECONDS);
            assertThat(stats.getMatched()).isEqualTo(expectedHistory.size());
            assertThat(stats.getWindowToMs()).isNull();
            assertThat(stats.getEndedBy()).isEqualTo(WindowStats.EndedBy.LOG_END);
            assertThat(stats.getStatus()).isEqualTo(WindowStats.Status.COMPLETE);

            List<String> live = publishPhase(producer, "D");
            Drain liveDrain = drainUntil(consumer, () -> false, live.size(), 60_000L);
            assertThat(liveDrain.values).containsExactlyElementsOf(live);
            assertThat(window.isComplete()).as("an open-ended window never completes").isFalse();
        }
    }

    @Test
    void allReadsEverythingRetainedAndIsNeverTruncated() throws Exception {
        Seeded s = seed("all");
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(s.topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.all())
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            Drain d = drainUntil(consumer, window::isComplete, 120_000L);

            List<String> expected = new ArrayList<>(s.phaseA);
            expected.addAll(s.phaseB);
            expected.addAll(s.phaseC);
            assertThat(d.values).containsExactlyElementsOf(expected);
            assertThat(window.stats().getMatched()).isEqualTo(expected.size());
            assertThat(window.stats().getEndedBy()).isEqualTo(WindowStats.EndedBy.LOG_END);
            assertThat(window.stats().getStatus()).isEqualTo(WindowStats.Status.COMPLETE);
        }
    }

    // Deleted and never written look the same, so a window over a topic that retains nothing can't report
    // a complete, empty result: that would claim nothing happened.
    @Test
    void aWindowOverATopicThatRetainsNothingIsTruncated() throws Exception {
        String topic = "persistent://public/default/window-it-retains-nothing";
        admin.topics().createNonPartitionedTopic(topic);
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.last(1, TimeUnit.HOURS))
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            Drain d = drainUntil(consumer, window::isComplete, 60_000L);

            assertThat(d.values).isEmpty();
            assertThat(window.stats().getScanned()).isZero();
            assertThat(window.stats().getStatus()).isEqualTo(WindowStats.Status.TRUNCATED_BY_RETENTION);
        }
    }

    // Retention may delete messages older than its time limit, so a window that starts before that horizon
    // is truncated even while its messages are still there. Retention is set in whole minutes, so this
    // waits until the window's start is more than a minute old.
    @Test
    void aWindowThatStartsBeforeTheRetentionTimeIsTruncated() throws Exception {
        Seeded s = seed("retention");
        admin.topicPolicies().setRetention(s.topic, new RetentionPolicies(1, -1));
        long deadline = System.currentTimeMillis() + 30_000L;
        while (!new RetentionPolicies(1, -1).equals(admin.topicPolicies().getRetention(s.topic))) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("retention policy not applied to " + s.topic);
            }
            Thread.sleep(500);
        }
        Thread.sleep(Math.max(0, s.t1 + 65_000L - System.currentTimeMillis()));

        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(s.topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.between(Instant.ofEpochMilli(s.t1), Instant.ofEpochMilli(s.t2)))
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            Drain d = drainUntil(consumer, window::isComplete, 120_000L);

            assertThat(s.phaseB).containsAll(d.values);
            assertThat(window.stats().getStatus()).isEqualTo(WindowStats.Status.TRUNCATED_BY_RETENTION);
        }
    }

    // The topic has history, and this is the first live query on it: its new filter must start at the
    // registration, not replay the topic (it did before live filters' subscriptions were created at the end).
    @Test
    void liveOnlySkipsHistoryAndKeepsSetSelector() throws Exception {
        Seeded s = seed("live");
        Set<String> before = spawnedLiveFilterNames();
        try (Session session = newSession();
             Producer<byte[]> producer = newProducer(s.topic);
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(s.topic)
                     .selector(SELECTOR)
                     .window(TimeWindow.liveOnly())
                     .subscribe()) {

            WindowControl window = (WindowControl) consumer;
            assertThat(window.isOpenEnded()).isTrue();
            assertThat(window.isCaughtUp()).as("liveOnly has no history to wait for").isTrue();
            assertThat(window.stats()).isNull();
            waitForNewLiveFilter(before);

            List<String> live = publishPhase(producer, "D");
            Drain d = drainUntil(consumer, () -> false, live.size(), 60_000L);
            assertThat(d.values).as("only live matches, no history").containsExactlyElementsOf(live);

            FilterControl fc = (FilterControl) consumer;
            fc.setSelector("region = 'r01'");
            assertThat(fc.currentSelector()).isEqualTo("region = 'r01'");
        }
    }

    // ---------------------------------------------------------------- Seeding

    /** A seeded source topic: phases A, B, C with {@code t1} between A and B, {@code t2} between B and C. */
    private record Seeded(String topic, long t1, long t2, List<String> phaseA, List<String> phaseB,
                          List<String> phaseC) {
    }

    private Seeded seed(String name) throws Exception {
        String topic = "persistent://public/default/window-it-" + name;
        admin.topics().createNonPartitionedTopic(topic);
        try (Producer<byte[]> producer = newProducer(topic)) {
            List<String> a = publishPhase(producer, "A");
            Thread.sleep(GAP_MS);
            long t1 = System.currentTimeMillis();
            Thread.sleep(GAP_MS);
            List<String> b = publishPhase(producer, "B");
            Thread.sleep(GAP_MS);
            long t2 = System.currentTimeMillis();
            Thread.sleep(GAP_MS);
            List<String> c = publishPhase(producer, "C");
            return new Seeded(topic, t1, t2, a, b, c);
        }
    }

    private Producer<byte[]> newProducer(String topic) throws Exception {
        return client.newProducer(Schema.BYTES).topic(topic).enableBatching(false).create();
    }

    /** Publishes {@link #PER_PHASE} messages; returns the payloads that match {@link #SELECTOR}, in order. */
    private static List<String> publishPhase(Producer<byte[]> producer, String phase) throws Exception {
        List<String> matches = new ArrayList<>();
        for (int i = 0; i < PER_PHASE; i++) {
            String payload = phase + "-" + i;
            String region = i % 3 == 0 ? "r07" : "r01";
            producer.newMessage().key("k" + i).property("region", region)
                    .value(payload.getBytes(StandardCharsets.UTF_8)).send();
            if (region.equals("r07")) {
                matches.add(payload);
            }
        }
        return matches;
    }

    // ---------------------------------------------------------------- Draining

    private static final class Drain {
        final List<String> values = new ArrayList<>();
        int emptyBeforeDone;
        int missingSourceProps;
    }

    private static Drain drainUntil(Consumer<byte[]> consumer, BooleanSupplier done, long timeoutMs)
            throws Exception {
        return drainUntil(consumer, done, Integer.MAX_VALUE, timeoutMs);
    }

    /** Receives until {@code done} holds or {@code count} messages arrived; fails on timeout. */
    private static Drain drainUntil(Consumer<byte[]> consumer, BooleanSupplier done, int count, long timeoutMs)
            throws Exception {
        Drain d = new Drain();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!done.getAsBoolean() && d.values.size() < count) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms with " + d.values.size()
                        + " messages: " + d.values);
            }
            Message<byte[]> m = consumer.receive(1, TimeUnit.SECONDS);
            if (m == null) {
                if (!done.getAsBoolean()) {
                    d.emptyBeforeDone++;
                }
                continue;
            }
            d.values.add(new String(m.getData(), StandardCharsets.UTF_8));
            if (m.getProperty(WindowProtocol.SRC_MSGID_PROPERTY) == null
                    || m.getProperty(WindowProtocol.SRC_PUBLISH_TIME_PROPERTY) == null) {
                d.missingSourceProps++;
            }
            consumer.acknowledge(m);
        }
        LOG.info("Drained {} messages ({} empty receives before done)", d.values.size(), d.emptyBeforeDone);
        return d;
    }

    // ---------------------------------------------------------------- Helpers

    private Session newSession() throws Exception {
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT)
                .privateTopicNamespace(FN_NAMESPACE)
                .build();
    }

    /** Live (non-windowed) spawned filters; windowed ones are named {@code selector-filter-win-*}. */
    private Set<String> spawnedLiveFilterNames() throws Exception {
        Set<String> names = new HashSet<>();
        for (String name : admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE)) {
            if (name.startsWith("selector-filter-") && !name.startsWith("selector-filter-win-")
                    && !name.equals(ORCH_NAME)) {
                names.add(name);
            }
        }
        return names;
    }

    /** Waits for a live filter not in {@code before} to reach RUNNING, so live messages aren't missed. */
    private void waitForNewLiveFilter(Set<String> before) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                for (String name : spawnedLiveFilterNames()) {
                    if (before.contains(name)) {
                        continue;
                    }
                    FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, name);
                    if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                        LOG.info("Live filter function {} is RUNNING", name);
                        Thread.sleep(3_000); // grace for the filter's TableView to load the QueryMessage
                        return;
                    }
                }
            } catch (PulsarAdminException ignored) {
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("No newly-spawned live filter reached RUNNING within 90s");
    }

    /** The container can report ready before standalone has created {@code public/default}; see SessionIT. */
    private static void waitForDefaultNamespace(PulsarAdmin admin) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (true) {
            try {
                if (admin.namespaces().getNamespaces("public").contains("public/default")) {
                    return;
                }
            } catch (PulsarAdminException.NotFoundException tenantNotCreatedYet) {
                // keep waiting
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Namespace public/default did not appear within 60s");
            }
            Thread.sleep(500);
        }
    }

    private void deployWithLeaderRetry(FunctionConfig config, String pkg) throws Exception {
        String packageUrl = "function://" + FN_TENANT + "/" + FN_NAMESPACE + "/" + config.getName() + "@0";
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
                    } catch (Exception ignored) {
                    }
                    Thread.sleep(1_000);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness: "
                + config.getName(), last);
    }

    private void waitForFunctionRunning(String name, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, name);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {} is RUNNING", name);
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Function did not reach RUNNING within " + timeoutMs + " ms: " + name);
    }

    private static Path findNar(String module, String prefix) {
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", module, "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) -> n.startsWith(prefix) && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("NAR " + prefix + "*.nar not found in " + target
                    + ". Build all NARs first: 'mvn -DskipTests package', then 'mvn verify'.");
        }
        return files[0].toPath();
    }
}
