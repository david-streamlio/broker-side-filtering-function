package io.streamnative.filter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live KoP (Kafka-on-Pulsar) interoperability ITs covering the three {@code kafkaEntryFormat}
 * values supported by KoP. Uses the {@code streamnative/sn-platform} image which bundles the KSN
 * (Kafka StreamNative) protocol handler, so we don't have to download and install a separate
 * {@code .nar}.
 *
 * <p>Each test starts a fresh container with the broker config tuned for one entry format,
 * deploys the function NAR, produces records via a Kafka client, and reads filtered output via
 * the native Pulsar client.
 *
 * <h2>Why three containers</h2>
 *
 * {@code kafkaEntryFormat} is a global broker setting in KoP, not a per-namespace policy. Each
 * format needs its own broker instance, so each test spins up its own container and stops it on
 * completion via try-with-resources.
 *
 * <h2>Cross-version note</h2>
 *
 * The function NAR is built against {@code pulsar.version=4.0.10}; this IT runs against
 * {@code streamnative/sn-platform:4.2.0.6} (Pulsar 4.2). The Function SPI surfaces used here —
 * {@code Function}, {@code Context}, {@code Record.getMessage()}, {@code Message.getKeyBytes()},
 * {@code TableView.forEachAndListen} — are stable across 4.0 → 4.2.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KoPFilterFunctionIT {

    private static final Logger LOG = LoggerFactory.getLogger(KoPFilterFunctionIT.class);

    private static final String IMAGE        = "streamnative/sn-platform:4.2.0.6";
    private static final String FN_NAR_DEST  = "/pulsar/selector-filter-function.nar";

    /**
     * Bind Kafka's listener to a fixed host port so the test JVM (outside Docker) can talk to
     * the broker using the same address the broker advertises to its clients. Pulsar's broker
     * port stays dynamic.
     */
    private static final int KAFKA_HOST_PORT = 9092;

    private static final String CONTROL_TOPIC = "persistent://public/default/queries";
    private static final String OUTPUT_TOPIC  = "persistent://public/default/filtered-output";
    private static final String KAFKA_TOPIC   = "kop-test-input";
    private static final String INPUT_TOPIC   = "persistent://public/default/" + KAFKA_TOPIC;

    private static final Path FN_NAR = findFunctionNar();

    @FunctionalInterface
    private interface KoPTestBody {
        void run(PulsarClient client, PulsarAdmin admin, String kafkaBootstrap) throws Exception;
    }

    /**
     * Starts a fresh sn-platform container with KoP enabled and {@code kafkaEntryFormat} set as
     * requested, deploys the function with explicit inputs on the canonical input topic, then
     * invokes {@code body} with handles to Pulsar client/admin and the Kafka bootstrap server
     * address. Container is stopped via try-with-resources.
     */
    /** Subclass that exposes {@code addFixedExposedPort} (protected on {@link GenericContainer}). */
    private static final class FixedPortPulsarContainer extends GenericContainer<FixedPortPulsarContainer> {
        FixedPortPulsarContainer(DockerImageName image) { super(image); }
        FixedPortPulsarContainer bindHostPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
            return this;
        }
    }

    private void runWithKoP(String kafkaEntryFormat, KoPTestBody body) throws Exception {
        try (FixedPortPulsarContainer pulsar = new FixedPortPulsarContainer(DockerImageName.parse(IMAGE))
                .withExposedPorts(6650, 8080)
                .bindHostPort(KAFKA_HOST_PORT, KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                .withEnv("PULSAR_PREFIX_kafkaListeners",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners",
                        "PLAINTEXT://localhost:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "false")
                .withEnv("PULSAR_PREFIX_kafkaNamespace", "public/default")
                .withEnv("PULSAR_PREFIX_kafkaEntryFormat", kafkaEntryFormat)
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                // Functions worker MUST stay enabled — we deploy the filter NAR via the admin
                // API. -nss disables stream storage (not needed).
                .withCommand("sh", "-c",
                        // Apply PULSAR_PREFIX_* env vars to standalone.conf. The default
                        // entrypoint only configures broker.conf, but `pulsar standalone`
                        // reads standalone.conf — without this, KoP env vars never reach
                        // the running broker.
                        "bin/apply-config-from-env.py conf/standalone.conf "
                                + "&& bin/pulsar standalone -nss")
                .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)))) {

            // Stream broker logs to test stdout so KoP startup / failure messages are visible.
            pulsar.withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("pulsar-broker"));
            pulsar.start();

            String httpUrl = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                admin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
                admin.topics().createNonPartitionedTopic(OUTPUT_TOPIC);
                // Pre-create the Kafka-facing input topic in Pulsar. KoP maps Kafka topic
                // "kop-test-input" to persistent://public/default/kop-test-input (per
                // kafkaNamespace). Creating it explicitly avoids the Kafka producer's
                // metadata-not-found timeout for unknown topics.
                admin.topics().createNonPartitionedTopic(INPUT_TOPIC);

                // Function deploys for both pulsar and pulsar_non_batched formats. For the
                // "kafka" format the function still deploys but won't receive readable messages
                // — see the negative test.
                deployFunction(admin);
                waitForFunctionRunning(admin, "public", "default", "selector-filter-kop-it");

                try {
                    body.run(client, admin, kafkaBootstrap);
                } catch (Throwable t) {
                    // Dump the function-instance log so we can see what the function did
                    // (ProcessRuntime instances log to a file inside the container).
                    try {
                        org.testcontainers.containers.Container.ExecResult r = pulsar.execInContainer(
                                "sh", "-c",
                                "cat /pulsar/logs/functions/public/default/selector-filter-kop-it/"
                                        + "selector-filter-kop-it-0.log 2>/dev/null | tail -200");
                        LOG.error("---- function instance log ----\n{}", r.getStdout());
                    } catch (Exception ignored) {}
                    throw t;
                }
            }
        }
    }

    private void deployFunction(PulsarAdmin admin) throws Exception {
        FunctionConfig config = FunctionConfig.builder()
                .tenant("public")
                .namespace("default")
                .name("selector-filter-kop-it")
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
        deployWithLeaderRetry(admin, config, FN_NAR.toString(),
                "public", "default", "selector-filter-kop-it");
    }

    /**
     * Deploys with retry on "Leader not yet ready". The wrinkle: each failed attempt may have
     * already uploaded the function package on the way to its 503; the next attempt then fails
     * with "Package metadata already exists". We delete the stale package between retries so
     * each attempt starts fresh.
     */
    static void deployWithLeaderRetry(PulsarAdmin admin, FunctionConfig config, String pkg,
                                     String tenant, String ns, String name) throws Exception {
        String packageUrl = "function://" + tenant + "/" + ns + "/" + name + "@0";
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
                    } catch (Exception ignored) {
                        // Package may not exist if the previous attempt failed before upload.
                    }
                    Thread.sleep(1_000);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness", last);
    }

    private static KafkaProducer<String, byte[]> kafkaProducer(String bootstrap) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        return new KafkaProducer<>(props);
    }

    private static void publishQuery(PulsarClient client, String selector) throws Exception {
        try (Producer<byte[]> q = client.newProducer(Schema.BYTES).topic(CONTROL_TOPIC).create()) {
            String json = String.format(
                    "{\"name\":\"kop-q\",\"selector\":\"%s\",\"outputTopic\":\"%s\"}",
                    selector.replace("\"", "\\\""), OUTPUT_TOPIC);
            q.newMessage().key("kop-q").value(json.getBytes(StandardCharsets.UTF_8)).send();
        }
        // Allow the TableView listener to load the query.
        Thread.sleep(2_000);
    }

    private static Consumer<byte[]> outputConsumer(PulsarClient client, String subName) throws Exception {
        return client.newConsumer(Schema.BYTES)
                .topic(OUTPUT_TOPIC)
                .subscriptionName(subName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
    }

    // ===== pulsar (default — batched Pulsar messages) =====

    @Test
    void pulsarFormatTopicIsNotConsumableFromNativePulsarFunction() throws Exception {
        // kafkaEntryFormat=pulsar (the KoP default) stores Kafka records as batched Pulsar
        // messages designed for Kafka consumers. Native Pulsar consumers — including this
        // function — log BatchDeSerializeError and silently drop these records. The function
        // therefore produces zero output regardless of the selector. This test documents the
        // limitation: if you want both Kafka producers AND the function on the same topic, use
        // pulsar_non_batched.
        runWithKoP("pulsar", (client, admin, kafkaBootstrap) -> {
            publishQuery(client, "__key__ IS NOT NULL");

            try (Consumer<byte[]> out = outputConsumer(client, "test-pulsar-batched");
                 KafkaProducer<String, byte[]> kafka = kafkaProducer(kafkaBootstrap)) {

                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC",
                        "p1".getBytes(StandardCharsets.UTF_8))).get();
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "ETH",
                        "p2".getBytes(StandardCharsets.UTF_8))).get();
                kafka.flush();

                Message<byte[]> none = out.receive(20, TimeUnit.SECONDS);
                assertThat(none)
                        .as("kafkaEntryFormat=pulsar uses batched Pulsar wire format that the "
                                + "function's native Pulsar consumer cannot deserialize; expect "
                                + "zero output. Use pulsar_non_batched for Kafka-producer interop.")
                        .isNull();
            }
        });
    }

    // ===== pulsar_non_batched =====

    @Test
    void pulsarNonBatchedFormatProducesMessagesIncompatibleWithFunctionConsumer() throws Exception {
        // **KoP 4.2.0.6 limitation, documented as a negative test.**
        //
        // The intent of kafkaEntryFormat=pulsar_non_batched is to let native Pulsar consumers
        // read Kafka-produced records. The function's MessageBindings code is designed for this
        // case and decodes base64 keys via Message.getKeyBytes() (unit-tested in
        // MessageBindingsTests.fromRecordDecodesKoPBase64Key). In practice on KoP 4.2.0.6 the
        // messages KoP writes still carry batch-envelope framing whose SingleMessageMetadata
        // omits required fields; the Pulsar Function's ConsumerImpl raises
        //   IllegalStateException: Some required fields are missing
        //     at SingleMessageMetadata.checkRequiredFields(SingleMessageMetadata.java:602)
        //     at Commands.deSerializeSingleMessageInBatch(...)
        // before the message ever reaches our function code. No output reaches the filtered
        // topic. Switching to a native Pulsar producer (or to a future KoP version that emits
        // strictly non-batched envelopes) recovers the happy path.
        runWithKoP("pulsar_non_batched", (client, admin, kafkaBootstrap) -> {
            publishQuery(client, "__key__ = 'BTC'");

            try (Consumer<byte[]> out = outputConsumer(client, "test-pnb");
                 KafkaProducer<String, byte[]> kafka = kafkaProducer(kafkaBootstrap)) {

                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC",
                        "p-btc".getBytes(StandardCharsets.UTF_8))).get();
                kafka.flush();

                Message<byte[]> none = out.receive(20, TimeUnit.SECONDS);
                assertThat(none)
                        .as("KoP 4.2.0.6 pulsar_non_batched messages carry batch framing that "
                                + "Pulsar Function consumers cannot deserialize — no output "
                                + "should reach the filtered topic. The function's key-decode "
                                + "logic remains correct (see MessageBindingsTests).")
                        .isNull();
            }
        });
    }

    // ===== kafka (native wire format) =====

    @Test
    void kafkaFormatTopicIsNotReadableByFunctionAndProducesNoOutput() throws Exception {
        runWithKoP("kafka", (client, admin, kafkaBootstrap) -> {
            // Any selector — the function won't see any messages because kafka-format records
            // are not consumable by a native Pulsar consumer.
            publishQuery(client, "__key__ IS NOT NULL");

            try (Consumer<byte[]> out = outputConsumer(client, "test-kafka");
                 KafkaProducer<String, byte[]> kafka = kafkaProducer(kafkaBootstrap)) {

                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC",
                        "should-not-be-filtered".getBytes(StandardCharsets.UTF_8))).get();
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "ETH",
                        "should-not-be-filtered".getBytes(StandardCharsets.UTF_8))).get();
                kafka.flush();

                // Wait generously; expect zero matches.
                Message<byte[]> none = out.receive(20, TimeUnit.SECONDS);
                assertThat(none)
                        .as("kafkaEntryFormat=kafka stores records in Kafka native wire format; "
                                + "native Pulsar consumers (including this function) cannot read "
                                + "them — selector-filtering this topic is impossible.")
                        .isNull();
            }
        });
    }

    /**
     * Polls {@code getFunctionStatus} until {@code numRunning == numInstances}. Catches the
     * race where messages produced immediately after {@code createFunction} are missed because
     * the function instance hasn't subscribed yet.
     */
    private static void waitForFunctionRunning(PulsarAdmin admin, String tenant, String ns, String name)
            throws Exception {
        // Pulsar 4.2's expected_healthcheck_interval=30 means the first heartbeat may arrive
        // ~30s after instance start. Allow up to 90s.
        long deadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                var status = admin.functions().getFunctionStatus(tenant, ns, name);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {}/{}/{} is RUNNING ({} instances)", tenant, ns, name,
                            status.getNumInstances());
                    // RUNNING fires once the instance starts; initialize() may still be priming
                    // the TableView. Give it a beat so the query registry is populated before
                    // we produce input messages.
                    Thread.sleep(5_000);
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        throw new IllegalStateException("Timed out waiting for function " + name + " to be RUNNING");
    }

    private static Path findFunctionNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "target");
        File[] files = target.toFile().listFiles((dir, name) ->
                name.startsWith("selector-filter-function-") && name.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "NAR not found in " + target + ". Run 'mvn package' first.");
        }
        return files[0].toPath();
    }
}
