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

package io.streamnative.filter.client.kafka;

import io.streamnative.filter.api.FilterControl;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end IT for {@code .window(TimeWindow)} on the Kafka SDK over KoP, on the same
 * {@code sn-platform} setup as {@link KafkaConsumerKoPIT}. Checks what only a real KoP can show: the
 * filter's marker records (null value, {@code __window_*} headers) reach the Kafka consumer, and
 * {@code poll()} drops them and records the stats.
 *
 * <p>One test method runs a bounded and an open-ended window against one container, since the
 * container is slow to start. The source topic has three phases (A, B, C) with a timestamp taken in
 * each gap ({@code t1}, {@code t2}).
 *
 * <p>Requires Docker AND pre-built NARs — run {@code mvn -DskipTests package} first.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WindowedKafkaConsumerKoPIT {

    private static final Logger LOG = LoggerFactory.getLogger(WindowedKafkaConsumerKoPIT.class);

    private static final String IMAGE = "streamnative/sn-platform:4.2.0.6";

    // Distinct from KoPFilterFunctionIT (9092), the spike IT (9094) and KafkaConsumerKoPIT (9095).
    private static final int KAFKA_HOST_PORT = 9096;

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-kop-window-it";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/kop-window-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/kop-window-orchestrator-state";
    private static final String INPUT_TOPIC        = "persistent://public/default/kafka-window-orders";

    /** Every third message of a phase has {@code region = 'r07'}. */
    private static final String SELECTOR  = "region = 'r07'";
    private static final int    PER_PHASE = 30;
    private static final long   GAP_MS    = 1_500L;

    private static final Path FILTER_NAR       = findNar("selector-filter", "selector-filter-function-");
    private static final Path ORCHESTRATOR_NAR = findNar("orchestrator", "selector-filter-orchestrator-");

    /** Subclass that exposes {@code addFixedExposedPort} (protected on {@link GenericContainer}). */
    private static final class FixedPortPulsarContainer extends GenericContainer<FixedPortPulsarContainer> {
        FixedPortPulsarContainer(DockerImageName image) { super(image); }
        FixedPortPulsarContainer bindHostPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
            return this;
        }
    }

    @Test
    void boundedAndOpenEndedWindowsOverKoP() throws Exception {
        // Container settings as in KafkaConsumerKoPIT; see the comments there.
        try (FixedPortPulsarContainer pulsar = new FixedPortPulsarContainer(DockerImageName.parse(IMAGE))
                .withExposedPorts(6650, 8080)
                .bindHostPort(KAFKA_HOST_PORT, KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                .withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://0.0.0.0:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://localhost:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "false")
                .withEnv("PULSAR_PREFIX_kafkaNamespace", "public/default")
                .withEnv("PULSAR_PREFIX_kafkaEntryFormat", "pulsar_non_batched")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                .withCommand("sh", "-c",
                        "bin/apply-config-from-env.py conf/standalone.conf "
                                + "&& sed -i "
                                + "'s|^functionRuntimeFactoryClassName:.*|"
                                + "functionRuntimeFactoryClassName: "
                                + "org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory|' "
                                + "conf/functions_worker.yml "
                                + "&& bin/pulsar standalone -nss")
                .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)))) {

            pulsar.withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("pulsar-broker"));
            pulsar.start();

            String httpUrl        = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl      = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin pulsarAdmin   = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                pulsarAdmin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
                pulsarAdmin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
                pulsarAdmin.topics().createNonPartitionedTopic(INPUT_TOPIC);
                deployOrchestrator(pulsarAdmin);

                try (Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                                .topic(INPUT_TOPIC)
                                .enableBatching(false)
                                .create();
                     Session session = Session.builder()
                                .pulsarClient(pulsarClient)
                                .pulsarAdmin(pulsarAdmin)
                                .controlTopic(SDK_INPUT_TOPIC)
                                .privateTopicTenant(FN_TENANT)
                                .privateTopicNamespace(FN_NAMESPACE)
                                .build()) {

                    List<String> phaseA = publishPhase(producer, "A");
                    Thread.sleep(GAP_MS);
                    long t1 = System.currentTimeMillis();
                    Thread.sleep(GAP_MS);
                    List<String> phaseB = publishPhase(producer, "B");
                    Thread.sleep(GAP_MS);
                    long t2 = System.currentTimeMillis();
                    Thread.sleep(GAP_MS);
                    List<String> phaseC = publishPhase(producer, "C");
                    LOG.info("Seeded {}: r07 matches A={} B={} C={}", INPUT_TOPIC,
                            phaseA.size(), phaseB.size(), phaseC.size());

                    // -------- Bounded: [t1, t2] is exactly phase B --------
                    try (Consumer<String, byte[]> consumer = FilterableKafkaConsumers
                            .<String, byte[]>newConsumer(session, kafkaProps(kafkaBootstrap))
                            .topic(INPUT_TOPIC)
                            .selector(SELECTOR)
                            .window(TimeWindow.between(Instant.ofEpochMilli(t1), Instant.ofEpochMilli(t2)))
                            .subscribe()) {

                        WindowControl window = (WindowControl) consumer;
                        assertThatThrownBy(() -> ((FilterControl) consumer).setSelector("region = 'r01'"))
                                .isInstanceOf(UnsupportedOperationException.class);

                        Drain d = drainUntil(consumer, window::isComplete, Integer.MAX_VALUE, 180_000L);

                        assertThat(d.emptyBeforeDone)
                                .as("empty polls before the completion record must not report the window done")
                                .isPositive();
                        assertThat(d.values).containsExactlyElementsOf(phaseB);
                        assertThat(d.wrongRegion).isZero();
                        assertThat(d.missingSourceHeaders).isZero();

                        WindowStats stats = window.stats();
                        assertThat(stats).as("stats from the __window_stats__ header").isNotNull();
                        assertThat(stats.getMatched()).isEqualTo(phaseB.size());
                        assertThat(stats.getEndedBy()).isEqualTo(WindowStats.EndedBy.WINDOW_END);
                        assertThat(stats.getStatus()).isEqualTo(WindowStats.Status.COMPLETE);
                        assertThat(consumer.poll(Duration.ofSeconds(2)).isEmpty())
                                .as("nothing follows the completion record").isTrue();
                    }

                    // -------- Open-ended: since t1 is phases B and C, then live --------
                    try (Consumer<String, byte[]> consumer = FilterableKafkaConsumers
                            .<String, byte[]>newConsumer(session, kafkaProps(kafkaBootstrap))
                            .topic(INPUT_TOPIC)
                            .selector(SELECTOR)
                            .window(TimeWindow.since(Instant.ofEpochMilli(t1)))
                            .subscribe()) {

                        WindowControl window = (WindowControl) consumer;
                        Drain history = drainUntil(consumer, window::isCaughtUp, Integer.MAX_VALUE, 180_000L);

                        List<String> expectedHistory = new ArrayList<>(phaseB);
                        expectedHistory.addAll(phaseC);
                        assertThat(history.emptyBeforeDone).isPositive();
                        assertThat(history.values).containsExactlyElementsOf(expectedHistory);

                        WindowStats stats = window.caughtUp().get(5, TimeUnit.SECONDS);
                        assertThat(stats.getMatched()).isEqualTo(expectedHistory.size());
                        assertThat(stats.getWindowToMs()).isNull();
                        assertThat(stats.getEndedBy()).isEqualTo(WindowStats.EndedBy.LOG_END);

                        List<String> live = publishPhase(producer, "D");
                        Drain liveDrain = drainUntil(consumer, () -> false, live.size(), 60_000L);
                        assertThat(liveDrain.values).containsExactlyElementsOf(live);
                        assertThat(window.isComplete()).isFalse();
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- Helpers

    private static Properties kafkaProps(String bootstrap) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-window-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return props;
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

    private static final class Drain {
        final List<String> values = new ArrayList<>();
        int emptyBeforeDone;
        int wrongRegion;
        int missingSourceHeaders;
    }

    /** Polls until {@code done} holds or {@code count} records arrived; fails on timeout. */
    private static Drain drainUntil(Consumer<String, byte[]> consumer, BooleanSupplier done, int count,
                                    long timeoutMs) {
        Drain d = new Drain();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!done.getAsBoolean() && d.values.size() < count) {
            if (System.currentTimeMillis() > deadline) {
                throw new AssertionError("timed out after " + timeoutMs + " ms with " + d.values.size()
                        + " records: " + d.values);
            }
            var records = consumer.poll(Duration.ofMillis(500));
            if (records.isEmpty() && !done.getAsBoolean()) {
                d.emptyBeforeDone++;
            }
            for (ConsumerRecord<String, byte[]> r : records) {
                d.values.add(new String(r.value(), StandardCharsets.UTF_8));
                if (!"r07".equals(header(r, "region"))) {
                    d.wrongRegion++;
                }
                if (header(r, WindowProtocol.SRC_MSGID_PROPERTY) == null
                        || header(r, WindowProtocol.SRC_PUBLISH_TIME_PROPERTY) == null) {
                    d.missingSourceHeaders++;
                }
            }
        }
        LOG.info("Drained {} records ({} empty polls before done)", d.values.size(), d.emptyBeforeDone);
        return d;
    }

    private static String header(ConsumerRecord<String, byte[]> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static void deployOrchestrator(PulsarAdmin admin) throws Exception {
        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for WindowedKafkaConsumerKoPIT")
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
                .userConfig(Map.of(
                        "state-topic",            ORCH_STATE_TOPIC,
                        "function-package-url",   FILTER_PACKAGE_URL,
                        "function-tenant",        FN_TENANT,
                        "function-namespace",     FN_NAMESPACE,
                        "function-parallelism",   1,
                        "apply-compaction",       true,
                        "pulsar-admin-url",       "http://localhost:8080"))
                .build();

        String packageUrl = "function://" + FN_TENANT + "/" + FN_NAMESPACE + "/" + ORCH_NAME + "@0";
        long deadline = System.currentTimeMillis() + 90_000L;
        while (true) {
            try {
                admin.functions().createFunction(orchConfig, ORCHESTRATOR_NAR.toString());
                break;
            } catch (PulsarAdminException e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                boolean retryable = msg.contains("Leader not yet ready") || msg.contains("Service Unavailable")
                        || msg.contains("already exists");
                if (!retryable || System.currentTimeMillis() > deadline) {
                    throw e;
                }
                try {
                    admin.packages().delete(packageUrl);
                } catch (Exception ignored) {
                }
                Thread.sleep(1_000);
            }
        }

        deadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, ORCH_NAME);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Orchestrator {} is RUNNING", ORCH_NAME);
                    Thread.sleep(5_000); // orchestrator initialize() needs to open its TableView
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Orchestrator did not reach RUNNING within 90s");
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
