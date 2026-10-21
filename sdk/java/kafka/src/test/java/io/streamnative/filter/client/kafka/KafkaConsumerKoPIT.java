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
import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live end-to-end IT for the Kafka SDK over KoP. Uses {@code streamnative/sn-platform:4.2.0.6}
 * with {@code kafkaEntryFormat=pulsar_non_batched} (per the spike that validated this path).
 * Deploys the orchestrator, registers a filter via the Kafka SDK, drives input via a native
 * Pulsar producer (avoiding the KoP input-direction bug documented in {@code KoPFilterFunctionIT}),
 * and asserts the Kafka consumer reads filtered output with payload + region-header round-trip
 * and dynamic-selector hot-reload.
 *
 * <p>Requires Docker AND pre-built NARs in {@code functions/selector-filter/target/} and
 * {@code functions/orchestrator/target/} — run {@code mvn verify} at the reactor root.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaConsumerKoPIT {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerKoPIT.class);

    private static final String IMAGE                 = "streamnative/sn-platform:4.2.0.6";
    private static final String FILTER_NAR_DEST       = "/pulsar/selector-filter-function.nar";
    private static final String ORCHESTRATOR_NAR_DEST = "/pulsar/selector-filter-orchestrator.nar";

    // Distinct host port from KoPFilterFunctionIT (9092) and the spike IT (9094) so the ITs can
    // run in parallel without binding clashes.
    private static final int KAFKA_HOST_PORT = 9095;

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-kop-it";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/kop-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/kop-orchestrator-state";
    private static final String INPUT_TOPIC        = "persistent://public/default/kafka-sdk-trades";

    private static final Path FILTER_NAR       = findFilterNar();
    private static final Path ORCHESTRATOR_NAR = findOrchestratorNar();

    /** Subclass that exposes {@code addFixedExposedPort} (protected on {@link GenericContainer}). */
    private static final class FixedPortPulsarContainer extends GenericContainer<FixedPortPulsarContainer> {
        FixedPortPulsarContainer(DockerImageName image) { super(image); }
        FixedPortPulsarContainer bindHostPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
            return this;
        }
    }

    @Test
    void kafkaConsumerReceivesFilteredOutputAndSupportsSetSelectorHotReload() throws Exception {
        try (FixedPortPulsarContainer pulsar = new FixedPortPulsarContainer(DockerImageName.parse(IMAGE))
                .withExposedPorts(6650, 8080)
                .bindHostPort(KAFKA_HOST_PORT, KAFKA_HOST_PORT)
                // No NAR-into-container copies needed — admin.packages().upload() and
                // admin.functions().createFunction(...) accept host paths and stream them up.
                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                .withEnv("PULSAR_PREFIX_kafkaListeners",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners",
                        "PLAINTEXT://localhost:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "false")
                .withEnv("PULSAR_PREFIX_kafkaNamespace", "public/default")
                .withEnv("PULSAR_PREFIX_kafkaEntryFormat", "pulsar_non_batched")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                // Force ThreadRuntime on the function WORKER (not the broker). sn-platform's
                // standalone reads WorkerConfig from conf/functions_worker.yml (NOT from
                // standalone.conf), so PULSAR_PREFIX_* env vars are useless for worker settings.
                // apply-config-from-env.py is for .conf files (key=value) — running it against
                // .yml files appends key=value lines that break YAML parsing. Use sed to
                // surgically replace the one key we care about. Default ProcessRuntime dies with
                // gRPC connection-refused on this image (function subprocess JVM fails to start
                // cleanly with our NAR), so we force ThreadRuntime which runs the function
                // inside the broker JVM.
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

            String httpUrl       = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl     = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin pulsarAdmin   = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                pulsarAdmin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
                pulsarAdmin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
                pulsarAdmin.topics().createNonPartitionedTopic(INPUT_TOPIC);

                // Upload selector-filter as a Pulsar package.
                PackageMetadata pkg = PackageMetadata.builder()
                        .description("selector-filter NAR for KafkaConsumerKoPIT")
                        .contact("ci@localhost")
                        .build();
                pulsarAdmin.packages().upload(pkg, FILTER_PACKAGE_URL, FILTER_NAR.toString());
                LOG.info("Uploaded filter NAR as package {}", FILTER_PACKAGE_URL);

                // Deploy orchestrator.
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
                deployWithLeaderRetry(pulsarAdmin, orchConfig, ORCHESTRATOR_NAR.toString(),
                        FN_TENANT, FN_NAMESPACE, ORCH_NAME);
                waitForFunctionRunning(pulsarAdmin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 90_000L);
                Thread.sleep(5_000);

                try (Producer<byte[]> inputProducer = pulsarClient.newProducer(Schema.BYTES)
                                .topic(INPUT_TOPIC)
                                .enableBatching(false)
                                .create();
                     Session session = Session.builder()
                                .pulsarClient(pulsarClient)
                                .pulsarAdmin(pulsarAdmin)
                                .controlTopic(SDK_INPUT_TOPIC)
                                .privateTopicTenant(FN_TENANT)
                                .privateTopicNamespace(FN_NAMESPACE)
                                .privateTopicPolicy(PrivateTopicPolicy.builder()
                                        .messageTtlSeconds(60)
                                        .retentionTimeMinutes(1)
                                        .retentionSizeMB(8)
                                        .inactiveDeleteSeconds(60)
                                        .build())
                                .build()) {

                    Properties kafkaProps = new Properties();
                    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
                    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-it-" + UUID.randomUUID());
                    kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                            StringDeserializer.class.getName());
                    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                            ByteArrayDeserializer.class.getName());

                    try (Consumer<String, byte[]> consumer = FilterableKafkaConsumers
                            .<String, byte[]>newConsumer(session, kafkaProps)
                            .topic(INPUT_TOPIC)
                            .selector("region = 'US'")
                            .subscribe()) {

                        FilterControl fc = (FilterControl) consumer;
                        String stableQueryId = fc.queryId();

                        // Wait for the spawned filter function to reach RUNNING.
                        waitForSpawnedFilter(pulsarAdmin);

                        // -------- Phase 1: selector A = "region = 'US'" --------
                        inputProducer.newMessage().key("AAPL").property("region", "US")
                                .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("BMW").property("region", "DE")
                                .value("de-1".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("BARC").property("region", "UK")
                                .value("uk-1".getBytes(StandardCharsets.UTF_8)).send();

                        DrainResult phaseA = drainUntilCountOrTimeout(consumer, 1, 45_000L);
                        assertThat(phaseA.payloadByKey).containsEntry("AAPL", "us-1");
                        assertThat(phaseA.payloadByKey).as("phase A: only US should match").hasSize(1);
                        assertThat(phaseA.regionByKey)
                                .as("user property `region` must round-trip as a Kafka header")
                                .containsEntry("AAPL", "US");

                        consumer.commitSync();

                        // -------- Hot-reload selector mid-stream --------
                        fc.setSelector("region = 'UK'");
                        assertThat(fc.queryId())
                                .as("queryId must be stable across setSelector")
                                .isEqualTo(stableQueryId);
                        assertThat(fc.currentSelector()).isEqualTo("region = 'UK'");
                        Thread.sleep(5_000); // orchestrator MODIFY → filter TableView hot-reload

                        // -------- Phase 2: selector B = "region = 'UK'" --------
                        inputProducer.newMessage().key("MSFT").property("region", "US")
                                .value("us-2".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("HSBC").property("region", "UK")
                                .value("uk-2".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("AUDI").property("region", "DE")
                                .value("de-2".getBytes(StandardCharsets.UTF_8)).send();

                        DrainResult phaseB = drainUntilCountOrTimeout(consumer, 1, 30_000L);
                        assertThat(phaseB.payloadByKey).containsEntry("HSBC", "uk-2");
                        assertThat(phaseB.payloadByKey).as("phase B: only UK should match").hasSize(1);
                        assertThat(phaseB.regionByKey).containsEntry("HSBC", "UK");
                        consumer.commitSync();
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- Helpers

    private static final class DrainResult {
        final Map<String, String> payloadByKey = new HashMap<>();
        final Map<String, String> regionByKey  = new HashMap<>();
    }

    private DrainResult drainUntilCountOrTimeout(Consumer<String, byte[]> consumer,
                                                 int targetCount, long timeoutMs) {
        DrainResult out = new DrainResult();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (out.payloadByKey.size() < targetCount && System.currentTimeMillis() < deadline) {
            ingest(consumer.poll(Duration.ofMillis(500)), out);
        }
        ingest(consumer.poll(Duration.ofSeconds(2)), out);
        return out;
    }

    private void ingest(ConsumerRecords<String, byte[]> recs, DrainResult out) {
        for (ConsumerRecord<String, byte[]> r : recs) {
            String key = r.key();
            String value = new String(r.value(), StandardCharsets.UTF_8);
            out.payloadByKey.put(key, value);
            org.apache.kafka.common.header.Header regionHeader = r.headers().lastHeader("region");
            if (regionHeader != null) {
                out.regionByKey.put(key, new String(regionHeader.value(), StandardCharsets.UTF_8));
            }
            LOG.info("Kafka SDK consumer received: key={} payload={} region={} partition={}",
                    key, value, out.regionByKey.get(key), r.partition());
        }
    }

    private void waitForSpawnedFilter(PulsarAdmin admin) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
                for (String name : functions) {
                    if (name.startsWith("selector-filter-") && !name.equals(ORCH_NAME)) {
                        FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, name);
                        if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                            LOG.info("Spawned filter function {} is RUNNING", name);
                            Thread.sleep(3_000); // grace for filter's TableView to load QueryMessage
                            return;
                        }
                    }
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        throw new IllegalStateException("No spawned filter function reached RUNNING within 90s");
    }

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
        throw new IllegalStateException("Function deploy timed out: " + name, last);
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

    private static Path findFilterNar() {
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", "selector-filter", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-function-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "Filter NAR not found in " + target + ". Run 'mvn -pl functions/selector-filter package' first.");
        }
        return files[0].toPath();
    }

    private static Path findOrchestratorNar() {
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", "orchestrator", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-orchestrator-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "Orchestrator NAR not found in " + target + ". Run 'mvn -pl functions/orchestrator package' first.");
        }
        return files[0].toPath();
    }
}
