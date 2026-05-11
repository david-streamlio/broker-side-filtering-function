package io.streamnative.filter;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
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
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

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
    private void runWithKoP(String kafkaEntryFormat, KoPTestBody body) throws Exception {
        try (GenericContainer<?> pulsar = new GenericContainer<>(DockerImageName.parse(IMAGE))
                .withExposedPorts(6650, 8080)
                .withCreateContainerCmdModifier(cmd ->
                        cmd.withHostConfig(cmd.getHostConfig().withPortBindings(
                                new PortBinding(Ports.Binding.bindPort(KAFKA_HOST_PORT),
                                        new ExposedPort(KAFKA_HOST_PORT)))))
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
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withCopyFileToContainer(MountableFile.forHostPath(FN_NAR), FN_NAR_DEST)
                .withCommand("sh", "-c", "bin/pulsar standalone -nss -nfw")
                .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1))
                .withStartupTimeout(Duration.ofMinutes(5))) {

            pulsar.start();

            String httpUrl = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                admin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
                admin.topics().createNonPartitionedTopic(OUTPUT_TOPIC);

                // Function deploys for both pulsar and pulsar_non_batched formats. For the
                // "kafka" format the function still deploys but won't receive readable messages
                // — see the negative test.
                deployFunction(admin);

                // Give the function's TableView listener time to subscribe.
                Thread.sleep(8_000);

                body.run(client, admin, kafkaBootstrap);
            }
        }
    }

    private void deployFunction(PulsarAdmin admin) throws Exception {
        FunctionConfig config = FunctionConfig.builder()
                .tenant("public")
                .namespace("default")
                .name("selector-filter-kop-it")
                .className("io.streamnative.filter.SelectorFilterFunction")
                .inputs(List.of(INPUT_TOPIC))
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        admin.functions().createFunctionWithUrl(config, "file://" + FN_NAR_DEST);
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

    // ===== pulsar (default) =====

    @Test
    void pulsarFormatTopicFiltersByKeyAndProperty() throws Exception {
        runWithKoP("pulsar", (client, admin, kafkaBootstrap) -> {
            publishQuery(client, "__key__ = 'BTC' AND region = 'US'");

            try (Consumer<byte[]> out = outputConsumer(client, "test-pulsar");
                 KafkaProducer<String, byte[]> kafka = kafkaProducer(kafkaBootstrap)) {

                // Match: key=BTC + region=US
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC", "p1".getBytes(StandardCharsets.UTF_8),
                        List.of(new RecordHeader("region", "US".getBytes(StandardCharsets.UTF_8))))).get();
                // No match: wrong key
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "ETH", "p2".getBytes(StandardCharsets.UTF_8),
                        List.of(new RecordHeader("region", "US".getBytes(StandardCharsets.UTF_8))))).get();
                // No match: wrong region
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC", "p3".getBytes(StandardCharsets.UTF_8),
                        List.of(new RecordHeader("region", "DE".getBytes(StandardCharsets.UTF_8))))).get();
                kafka.flush();

                Message<byte[]> m = out.receive(30, TimeUnit.SECONDS);
                assertThat(m).as("exactly one matching record reaches the output").isNotNull();
                assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("p1");
                assertThat(m.getKey()).isEqualTo("BTC");
                assertThat(m.getProperty("region")).isEqualTo("US");

                Message<byte[]> none = out.receive(3, TimeUnit.SECONDS);
                assertThat(none).as("no further matches").isNull();
            }
        });
    }

    // ===== pulsar_non_batched =====

    @Test
    void pulsarNonBatchedFormatTopicFiltersByDecodedKey() throws Exception {
        runWithKoP("pulsar_non_batched", (client, admin, kafkaBootstrap) -> {
            // The selector references the LOGICAL key. KoP will store the Pulsar key as the
            // base64 of "BTC" (= "QlRD") with hasBase64EncodedKey=true; our function decodes
            // via Message.getKeyBytes() so __key__ = 'BTC' matches.
            publishQuery(client, "__key__ = 'BTC'");

            try (Consumer<byte[]> out = outputConsumer(client, "test-pnb");
                 KafkaProducer<String, byte[]> kafka = kafkaProducer(kafkaBootstrap)) {

                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "BTC",
                        "p-btc".getBytes(StandardCharsets.UTF_8))).get();
                kafka.send(new ProducerRecord<>(KAFKA_TOPIC, 0, "ETH",
                        "p-eth".getBytes(StandardCharsets.UTF_8))).get();
                kafka.flush();

                Message<byte[]> m = out.receive(30, TimeUnit.SECONDS);
                assertThat(m).as("BTC reaches the output via key decode").isNotNull();
                assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("p-btc");

                Message<byte[]> none = out.receive(3, TimeUnit.SECONDS);
                assertThat(none).as("no further matches").isNull();
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
