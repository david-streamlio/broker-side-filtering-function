package io.streamnative.filter.client;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
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
import org.testcontainers.utility.MountableFile;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full end-to-end with the function NAR deployed and the client driving everything.
 *
 * <p>Requires Docker and a pre-built {@code function/target/selector-filter-function-*.nar}.
 * Run {@code mvn -pl function package} before invoking this IT (or use {@code mvn verify} at the
 * reactor root, which builds in dependency order automatically).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SessionIT {

    private static final Logger LOG = LoggerFactory.getLogger(SessionIT.class);

    private static final String NAR_DEST = "/pulsar/selector-filter-function.nar";
    private static final String CONTROL_TOPIC = "persistent://public/default/queries";
    private static final String INPUT_TOPIC   = "persistent://public/default/trades-it";
    private static final String FN_TENANT = "public";
    private static final String FN_NS = "default";
    private static final String FN_NAME = "selector-filter-it-session";

    private static final Path NAR = findNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.0.10"))
            .withFunctionsWorker()
            .withCopyFileToContainer(MountableFile.forHostPath(NAR), NAR_DEST)
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;
    private Producer<byte[]> inputProducer;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();

        admin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_TOPIC);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        // Deploy the function before driving messages.
        FunctionConfig config = FunctionConfig.builder()
                .tenant(FN_TENANT)
                .namespace(FN_NS)
                .name(FN_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.SelectorFilterFunction")
                .inputs(java.util.List.of(INPUT_TOPIC))
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        admin.functions().createFunction(config, NAR.toString());

        // Poll until the function instance is RUNNING.
        long fnReadyDeadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < fnReadyDeadline) {
            try {
                var status = admin.functions().getFunctionStatus(FN_TENANT, FN_NS, FN_NAME);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }

        inputProducer = client.newProducer(Schema.BYTES).topic(INPUT_TOPIC).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (inputProducer != null) inputProducer.close();
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    @Test
    void sessionCreateConsumerEndToEnd() throws Exception {
        try (Session session = Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic(CONTROL_TOPIC)
                .privateTopicTenant("public")
                .privateTopicNamespace("default")
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(60)
                        .retentionTimeMinutes(1)
                        .retentionSizeMB(8)
                        .inactiveDeleteSeconds(60)
                        .build())
                .build();
             FilteredConsumer consumer = session.createConsumer(INPUT_TOPIC, "region = 'US'")) {

            // Give the function's TableView listener a moment to receive the new query.
            Thread.sleep(2000);

            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("p1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BMW").property("region", "DE")
                    .value("p2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("MSFT").property("region", "US")
                    .value("p3".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> m1 = consumer.receive(30, TimeUnit.SECONDS);
            Message<byte[]> m2 = consumer.receive(15, TimeUnit.SECONDS);
            Message<byte[]> m3 = consumer.receive(5, TimeUnit.SECONDS);

            assertThat(m1).isNotNull();
            assertThat(m2).isNotNull();
            assertThat(new String(m1.getData(), StandardCharsets.UTF_8)).isEqualTo("p1");
            assertThat(new String(m2.getData(), StandardCharsets.UTF_8)).isEqualTo("p3");
            assertThat(m1.getProperty("region")).isEqualTo("US");
            assertThat(m3).as("Only US messages should pass the selector").isNull();

            consumer.acknowledge(m1);
            consumer.acknowledge(m2);

            String privateTopic = consumer.getPrivateTopic();
            // Closing the consumer (via try-with-resources at the end) will retract + delete.
            consumer.close();

            // After close: the private topic should be deleted.
            Thread.sleep(2000);
            assertThat(admin.topics().getList("public/default"))
                    .as("private topic should be cleaned up on close")
                    .doesNotContain(privateTopic);
        }
    }

    private static Path findNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "..", "function", "target").normalize();
        File[] files = target.toFile().listFiles((dir, name) ->
                name.startsWith("selector-filter-function-") && name.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "NAR not found in " + target + ". Run 'mvn -pl function package' first.");
        }
        return files[0].toPath();
    }
}
