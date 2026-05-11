package io.streamnative.filter.client.kafka;

import io.streamnative.filter.client.FilterControl;
import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

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
 * Live end-to-end IT for the Kafka SDK. Spins up {@code streamnative/sn-platform:4.2.0.6} with
 * {@code kafkaEntryFormat=pulsar_non_batched} (per the spike that validated this path), deploys
 * the filter function, drives input via a native Pulsar producer (to avoid the KoP
 * input-direction bug already documented in {@code KoPFilterFunctionIT}), and asserts that a real
 * Kafka consumer attached via this SDK reads the filtered output with payload + header round-trip
 * + dynamic-selector hot-reload working as advertised.
 *
 * <p>Requires Docker AND a pre-built {@code function/target/selector-filter-function-*.nar} — run
 * {@code mvn -pl functions/selector-filter package} first, or {@code mvn verify} at the
 * reactor root.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@org.junit.jupiter.api.Disabled("Phase 2: SDK now publishes FilterRequest to an orchestrator. "
        + "Phase 6 will rewire this IT to upload the filter NAR as a Pulsar package and deploy the "
        + "orchestrator before running.")
class KafkaConsumerKoPIT {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerKoPIT.class);

    private static final String IMAGE     = "streamnative/sn-platform:4.2.0.6";
    private static final String NAR_DEST  = "/pulsar/selector-filter-function.nar";

    // Distinct host port from KoPFilterFunctionIT (9092) and the spike IT (9094) so the three
    // ITs can run in parallel without binding clashes.
    private static final int KAFKA_HOST_PORT = 9095;

    private static final String CONTROL_TOPIC = "persistent://public/default/queries";
    private static final String INPUT_TOPIC   = "persistent://public/default/kafka-sdk-trades";
    private static final String FN_TENANT     = "public";
    private static final String FN_NS         = "default";
    private static final String FN_NAME       = "selector-filter-kafka-it";

    private static final Path NAR = findFunctionNar();

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
                .withCopyFileToContainer(MountableFile.forHostPath(NAR), NAR_DEST)
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
                .withCommand("sh", "-c",
                        "bin/apply-config-from-env.py conf/standalone.conf "
                                + "&& bin/pulsar standalone -nss")
                .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)))) {

            pulsar.withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("pulsar-broker"));
            pulsar.start();

            String httpUrl  = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin pulsarAdmin   = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                pulsarAdmin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
                pulsarAdmin.topics().createNonPartitionedTopic(INPUT_TOPIC);

                // Deploy function with INPUT_TOPIC as input. The Kafka SDK creates its own
                // per-consumer private output topics dynamically via Session.
                deployFunction(pulsarAdmin);
                waitForFunctionRunning(pulsarAdmin);

                try (Producer<byte[]> inputProducer = pulsarClient.newProducer(Schema.BYTES)
                                .topic(INPUT_TOPIC)
                                .enableBatching(false)
                                .create();
                     Session session = Session.builder()
                                .pulsarClient(pulsarClient)
                                .pulsarAdmin(pulsarAdmin)
                                .controlTopic(CONTROL_TOPIC)
                                .privateTopicTenant("public")
                                .privateTopicNamespace("default")    // matches kafkaNamespace=public/default
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
                        Thread.sleep(3_000); // function TableView listener picks up registration

                        // -------- Phase 1: selector A = "region = 'US'" --------
                        inputProducer.newMessage().key("AAPL").property("region", "US")
                                .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("BMW").property("region", "DE")
                                .value("de-1".getBytes(StandardCharsets.UTF_8)).send();
                        inputProducer.newMessage().key("BARC").property("region", "UK")
                                .value("uk-1".getBytes(StandardCharsets.UTF_8)).send();

                        DrainResult phaseA = drainUntilCountOrTimeout(consumer, 1, 30_000L);
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
                        Thread.sleep(3_000); // function picks up the swap

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

    private static final class DrainResult {
        final Map<String, String> payloadByKey = new HashMap<>();
        final Map<String, String> regionByKey  = new HashMap<>();
    }

    /**
     * Polls the consumer until at least {@code targetCount} messages have been gathered, or the
     * deadline is reached. Captures the {@code region} header per key so the IT can assert the
     * property→header round-trip.
     */
    private DrainResult drainUntilCountOrTimeout(Consumer<String, byte[]> consumer,
                                                 int targetCount, long timeoutMs) {
        DrainResult out = new DrainResult();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (out.payloadByKey.size() < targetCount && System.currentTimeMillis() < deadline) {
            ingest(consumer.poll(Duration.ofMillis(500)), out);
        }
        // One more poll to catch any straggler — if we already hit the count, this confirms we
        // don't pick up extras (assertion on exact size will fail loudly otherwise).
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

    private void deployFunction(PulsarAdmin admin) throws Exception {
        FunctionConfig config = FunctionConfig.builder()
                .tenant(FN_TENANT)
                .namespace(FN_NS)
                .name(FN_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.SelectorFilterFunction")
                .inputs(List.of(INPUT_TOPIC))
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        deployWithLeaderRetry(admin, config, NAR.toString());
    }

    /**
     * Deploys with retry on "Leader not yet ready"; deletes the stale package between attempts
     * (a failed attempt may have uploaded the package on the way to its 503 — the next try then
     * fails with "Package metadata already exists" otherwise).
     */
    private static void deployWithLeaderRetry(PulsarAdmin admin, FunctionConfig config, String pkg) throws Exception {
        String packageUrl = "function://" + FN_TENANT + "/" + FN_NS + "/" + FN_NAME + "@0";
        long deadline = System.currentTimeMillis() + 90_000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.functions().createFunction(config, pkg);
                return;
            } catch (org.apache.pulsar.client.admin.PulsarAdminException e) {
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
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness", last);
    }

    private static void waitForFunctionRunning(PulsarAdmin admin) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                var status = admin.functions().getFunctionStatus(FN_TENANT, FN_NS, FN_NAME);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {} is RUNNING", FN_NAME);
                    Thread.sleep(5_000); // grace for initialize() to prime the TableView
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        throw new IllegalStateException("Function did not reach RUNNING within 90s");
    }

    private static Path findFunctionNar() {
        // Module dir is sdk/java/kafka; NAR lives at functions/selector-filter/target/.
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", "selector-filter", "target").normalize();
        File[] files = target.toFile().listFiles((dir, name) ->
                name.startsWith("selector-filter-function-") && name.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "NAR not found in " + target
                            + ". Run 'mvn -pl functions/selector-filter package' first.");
        }
        return files[0].toPath();
    }
}
