package io.streamnative.filter;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * KoP (Kafka-on-Pulsar) interoperability ITs covering the three {@code kafkaEntryFormat} values:
 * <ul>
 *   <li>{@code pulsar} (default): records are stored in Pulsar message format. Pulsar consumers
 *       read normally; the function reads {@code __key__} from {@code record.getKey()}.</li>
 *   <li>{@code pulsar_non_batched}: each Kafka record is one Pulsar message and the Kafka key
 *       is base64-encoded into the Pulsar string key. The function transparently decodes via
 *       {@code Message.getKeyBytes()}.</li>
 *   <li>{@code kafka}: records are stored in Kafka native wire format. Native Pulsar consumers
 *       (including this function) <strong>cannot</strong> read them; this case is documented
 *       as a known limitation.</li>
 * </ul>
 *
 * <h2>Why this IT is disabled by default</h2>
 *
 * KoP runs as a Pulsar protocol handler installed via a {@code .nar}. As of the time this test
 * was written, the public KoP releases at
 * <a href="https://github.com/streamnative/kop/releases">github.com/streamnative/kop/releases</a>
 * peak at v3.1.1.1 (Pulsar 3.1.x compatible). This module's function NAR is built against Pulsar
 * 4.0.10. Cross-version use (4.0-built NAR running on a 3.1 broker) MAY work because the Function
 * SPI surfaces used here ({@code Function}, {@code Context}, {@code Record}, {@code TableView
 * .forEachAndListen}, {@code Message.getKeyBytes}) are stable across these versions — but we have
 * not validated it.
 *
 * <h2>How to enable</h2>
 *
 * <ol>
 *   <li>Pick a Pulsar+KoP version pair that's known to work together (e.g. Pulsar 3.1.x + KoP
 *       3.1.1.1). Set {@link #PULSAR_IMAGE_TAG} below.</li>
 *   <li>Update the KoP NAR download URL in {@link #KOP_NAR_URL} to the matching release.</li>
 *   <li>Drop the {@link Disabled} annotation on the class.</li>
 *   <li>Rebuild the function NAR against the broker's Pulsar version if you hit binary-incompat
 *       issues at function-deploy time.</li>
 * </ol>
 *
 * <h2>Test matrix</h2>
 *
 * <ul>
 *   <li>{@code pulsarFormatTopicFiltersByKey()} — namespace with default {@code
 *       kafkaEntryFormat=pulsar}. Kafka producer sends key=b"BTC". Selector {@code __key__ =
 *       'BTC'} matches.</li>
 *   <li>{@code pulsarNonBatchedFormatTopicFiltersByDecodedKey()} — namespace with {@code
 *       kafkaEntryFormat=pulsar_non_batched}. Same Kafka producer; selector {@code __key__ =
 *       'BTC'} must still match (function auto-decodes the base64 key).</li>
 *   <li>{@code kafkaFormatTopicCannotBeFilteredAndDocumentsLimitation()} — namespace with
 *       {@code kafkaEntryFormat=kafka}. Function deploy on this topic should produce zero
 *       output (records are unreadable from native Pulsar). The assertion is on the absence of
 *       fan-out within a reasonable timeout.</li>
 * </ul>
 *
 * <h2>Required test-time dependency (when enabling)</h2>
 *
 * <pre>{@code
 * <dependency>
 *     <groupId>org.apache.kafka</groupId>
 *     <artifactId>kafka-clients</artifactId>
 *     <version>3.6.1</version>
 *     <scope>test</scope>
 * </dependency>
 * }</pre>
 */
@Disabled("Requires a Pulsar broker with KoP protocol handler installed and a matching KoP " +
        "version. See class javadoc for setup steps.")
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KoPFilterFunctionIT {

    private static final Logger LOG = LoggerFactory.getLogger(KoPFilterFunctionIT.class);

    /** Update to a Pulsar version that has a matching public KoP release. */
    private static final String PULSAR_IMAGE_TAG = "apachepulsar/pulsar:3.1.1";

    /** Update to match the KoP version compatible with {@link #PULSAR_IMAGE_TAG}. */
    private static final String KOP_NAR_URL =
            "https://github.com/streamnative/kop/releases/download/v3.1.1.1/"
                    + "pulsar-protocol-handler-kafka-3.1.1.1.nar";

    private static final String FN_NAR_DEST  = "/pulsar/selector-filter-function.nar";
    private static final int KAFKA_PORT      = 9092;

    private static final Path FN_NAR = findFunctionNar();

    @Container
    private final GenericContainer<?> pulsar = new GenericContainer<>(DockerImageName.parse(PULSAR_IMAGE_TAG))
            .withExposedPorts(6650, 8080, KAFKA_PORT)
            .withCopyFileToContainer(MountableFile.forHostPath(FN_NAR), FN_NAR_DEST)
            .withEnv("PULSAR_PREFIX_protocolHandlerDirectory", "./protocols")
            .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
            .withEnv("PULSAR_PREFIX_kafkaListeners", "PLAINTEXT://0.0.0.0:9092")
            .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners", "PLAINTEXT://localhost:9092")
            .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                    "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
            .withCommand("sh", "-c",
                    "mkdir -p /pulsar/protocols && "
                    + "curl -fsSL " + KOP_NAR_URL + " -o /pulsar/protocols/kop.nar && "
                    + "bin/pulsar standalone")
            .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1))
            .withStartupTimeout(Duration.ofMinutes(5));

    private PulsarClient client;
    private PulsarAdmin admin;

    void setupCommon(String namespace, String kafkaEntryFormat) throws Exception {
        String httpUrl    = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
        String pulsarUrl  = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
        client = PulsarClient.builder().serviceUrl(pulsarUrl).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build();

        admin.tenants().createTenant("public",
                new org.apache.pulsar.common.policies.data.TenantInfoImpl(
                        java.util.Set.of(), java.util.Set.of("standalone")));
        admin.namespaces().createNamespace("public/" + namespace);
        // Set kafkaEntryFormat on the namespace.
        admin.namespaces().setProperty("public/" + namespace, "kafkaEntryFormat", kafkaEntryFormat);
    }

    @Test
    void pulsarFormatTopicFiltersByKey() throws Exception {
        // TODO when enabling:
        //   1) setupCommon("ns-pulsar", "pulsar");
        //   2) Use a Kafka producer (org.apache.kafka:kafka-clients) to send a record with
        //      key="BTC" and headers {"region":"US"} to topic "public/ns-pulsar/quotes".
        //   3) Deploy the function on persistent://public/ns-pulsar/quotes with a control topic
        //      and a query selector "__key__ = 'BTC' AND region = 'US'".
        //   4) Subscribe a native Pulsar consumer on the query's outputTopic; assert it
        //      receives the message with __key__ = "BTC" and the region property.
    }

    @Test
    void pulsarNonBatchedFormatTopicFiltersByDecodedKey() throws Exception {
        // TODO when enabling:
        //   1) setupCommon("ns-pnb", "pulsar_non_batched");
        //   2) Verify that admin.topics().examineMessage(...) on the input shows the key as
        //      base64-encoded ("QlRD" for "BTC") and msg.hasBase64EncodedKey()=true.
        //   3) Deploy the function and a query selector "__key__ = 'BTC'".
        //   4) Native Pulsar consumer on the outputTopic must receive the matching message —
        //      proving that MessageBindings.from(record) decoded the key transparently.
    }

    @Test
    void kafkaFormatTopicCannotBeFilteredAndDocumentsLimitation() throws Exception {
        // TODO when enabling:
        //   1) setupCommon("ns-kafka", "kafka");
        //   2) Use a Kafka producer to send records.
        //   3) Deploy the function on persistent://public/ns-kafka/quotes.
        //   4) Assert NO message is observed on the query's outputTopic within a reasonable
        //      timeout (e.g. 30s). The Kafka-native wire format is not readable by the
        //      function's native Pulsar consumer. This test documents the limitation.
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
