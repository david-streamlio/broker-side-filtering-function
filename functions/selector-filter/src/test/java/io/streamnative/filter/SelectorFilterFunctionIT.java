package io.streamnative.filter;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
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
 * End-to-end test: real Pulsar broker, real NAR, real control topic.
 *
 * <p>The NAR is expected to exist at {@code target/selector-filter-function-*-SNAPSHOT.nar} by the
 * time this test runs. The Maven Failsafe plugin runs after {@code package}, so this is true under
 * {@code mvn verify}. When you run this IT in an IDE, package the project first.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SelectorFilterFunctionIT {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorFilterFunctionIT.class);

    private static final String NAR_DEST = "/pulsar/selector-filter-function.nar";

    private static final String CONTROL_TOPIC = "persistent://public/default/queries";
    private static final String INPUT_TOPIC   = "persistent://public/default/trades";
    private static final String OUTPUT_TOPIC  = "persistent://public/default/filtered-us";
    private static final String FUNCTION_TENANT = "public";
    private static final String FUNCTION_NS     = "default";
    private static final String FUNCTION_NAME   = "selector-filter-it";

    private static final Path NAR = findNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.1"))
            .withFunctionsWorker()
            .withCopyFileToContainer(MountableFile.forHostPath(NAR), NAR_DEST)
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();
        // The Pulsar 4.2 image opens its admin port before the broker finishes provisioning
        // public/default, so creating topics in @Test can fail with "Namespace not found".
        awaitDefaultNamespace();
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

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    @Test
    void filtersMessagesByJmsSelectorAndForwardsProperties() throws Exception {
        // 1. Create topics + enable compaction on control topic.
        admin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(OUTPUT_TOPIC);

        // 2. Publish one query: match region=US, route to OUTPUT_TOPIC.
        try (Producer<byte[]> queryProducer = client.newProducer(Schema.BYTES).topic(CONTROL_TOPIC).create()) {
            String json = String.format(
                    "{\"name\":\"us-only\",\"selector\":\"region = 'US'\",\"outputTopic\":\"%s\"}",
                    OUTPUT_TOPIC);
            queryProducer.newMessage().key("q-us").value(json.getBytes(StandardCharsets.UTF_8)).send();
        }

        // 3. Subscribe consumer on the output topic.
        Consumer<byte[]> outputConsumer = client.newConsumer(Schema.BYTES)
                .topic(OUTPUT_TOPIC)
                .subscriptionName("test-reader")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // 4. Deploy the function pointing at the input topic.
        FunctionConfig config = FunctionConfig.builder()
                .tenant(FUNCTION_TENANT)
                .namespace(FUNCTION_NS)
                .name(FUNCTION_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className(SelectorFilterFunction.class.getName())
                .inputs(java.util.List.of(INPUT_TOPIC))
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        KoPFilterFunctionIT.deployWithLeaderRetry(admin, config, NAR.toString(),
                FUNCTION_TENANT, FUNCTION_NS, FUNCTION_NAME);

        // 5. Wait for the function to be RUNNING, then give initialize() time to prime
        //    the TableView before producing messages.
        for (int i = 0; i < 90; i++) {
            try {
                var status = admin.functions().getFunctionStatus(FUNCTION_TENANT, FUNCTION_NS, FUNCTION_NAME);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function RUNNING after {}s", i);
                    break;
                }
            } catch (Exception ignored) {}
            Thread.sleep(1000);
        }
        Thread.sleep(5_000);

        // 6. Drive input messages.
        try (Producer<byte[]> in = client.newProducer(Schema.BYTES).topic(INPUT_TOPIC).create()) {
            in.newMessage().key("AAPL").property("region", "US").value("p1".getBytes(StandardCharsets.UTF_8)).send();
            in.newMessage().key("BMW").property("region", "DE").value("p2".getBytes(StandardCharsets.UTF_8)).send();
            in.newMessage().key("MSFT").property("region", "US").value("p3".getBytes(StandardCharsets.UTF_8)).send();
        }

        // 7. Read from output; expect exactly the US messages, in order.
        Message<byte[]> m1 = outputConsumer.receive(30, TimeUnit.SECONDS);
        Message<byte[]> m2 = outputConsumer.receive(15, TimeUnit.SECONDS);
        Message<byte[]> m3 = outputConsumer.receive(5, TimeUnit.SECONDS); // should be null

        assertThat(m1).isNotNull();
        assertThat(m2).isNotNull();
        assertThat(new String(m1.getData(), StandardCharsets.UTF_8)).isEqualTo("p1");
        assertThat(new String(m2.getData(), StandardCharsets.UTF_8)).isEqualTo("p3");
        assertThat(m1.getProperty("region")).isEqualTo("US");
        assertThat(m2.getProperty("region")).isEqualTo("US");
        assertThat(m3).as("Only US messages should pass the selector").isNull();

        outputConsumer.close();
    }

    private static Path findNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "target");
        File[] files = target.toFile().listFiles((dir, name) ->
                name.startsWith("selector-filter-function-") && name.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "NAR not found in " + target + ". Run 'mvn package' before 'mvn verify'/the IT.");
        }
        return files[0].toPath();
    }
}
