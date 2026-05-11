package io.streamnative.filter;

import org.apache.pulsar.client.admin.PulsarAdmin;
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
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end IT for {@code topicsPattern}: the function subscribes via a regex; we verify that
 * a topic created <em>before</em> the function starts is consumed, and that a topic created
 * <em>after</em> the function starts is also picked up via Pulsar's pattern auto-discovery.
 *
 * <p>Pulsar's default {@code patternAutoDiscoveryPeriod} is 60 seconds. The test waits ~75s
 * between creating the late-bound topic and publishing to it to allow the function's consumer
 * to discover it.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RegexPatternIT {

    private static final Logger LOG = LoggerFactory.getLogger(RegexPatternIT.class);

    private static final String NAR_DEST = "/pulsar/selector-filter-function.nar";

    private static final String NAMESPACE      = "public/default";
    private static final String CONTROL_TOPIC  = "persistent://public/default/regex-queries";
    private static final String TOPIC_PATTERN  = "persistent://public/default/regex-in-.*";
    private static final String INITIAL_TOPIC  = "persistent://public/default/regex-in-initial";
    private static final String LATE_TOPIC     = "persistent://public/default/regex-in-late";
    private static final String OUTPUT_TOPIC   = "persistent://public/default/regex-out";

    private static final Path NAR = findNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.0.10"))
            .withFunctionsWorker()
            .withCopyFileToContainer(MountableFile.forHostPath(NAR), NAR_DEST)
            .withStartupTimeout(Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();

        // The HttpWaitStrategy for the admin port returns success before standalone has
        // finished provisioning the public/default namespace. Poll until it exists, then
        // create topics.
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.namespaces().getNamespaces("public");
                if (admin.namespaces().getNamespaces("public").contains(NAMESPACE)) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }

        admin.topics().createNonPartitionedTopic(CONTROL_TOPIC);
        admin.topics().createNonPartitionedTopic(INITIAL_TOPIC);
        admin.topics().createNonPartitionedTopic(OUTPUT_TOPIC);

        // Allow newly-created topics in the namespace to be discoverable by the pattern consumer.
        admin.namespaces().setAutoTopicCreation(NAMESPACE,
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    @Test
    void newTopicMatchingPatternIsDiscoveredAndFiltered() throws Exception {
        // 1. Publish a single query that matches messages from ANY topic matching the input pattern.
        try (Producer<byte[]> q = client.newProducer(Schema.BYTES).topic(CONTROL_TOPIC).create()) {
            String json = String.format(
                    "{\"name\":\"pattern-q\",\"selector\":\"region = 'US'\",\"outputTopic\":\"%s\"}",
                    OUTPUT_TOPIC);
            q.newMessage().key("pattern-q").value(json.getBytes(StandardCharsets.UTF_8)).send();
        }

        // 2. Subscribe a consumer on the output topic.
        Consumer<byte[]> outputConsumer = client.newConsumer(Schema.BYTES)
                .topic(OUTPUT_TOPIC)
                .subscriptionName("regex-it-reader")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // 3. Deploy the function with topicsPattern (NOT explicit inputs).
        FunctionConfig config = FunctionConfig.builder()
                .tenant("public")
                .namespace("default")
                .name("regex-pattern-it")
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.SelectorFilterFunction")
                .topicsPattern(TOPIC_PATTERN)
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        KoPFilterFunctionIT.deployWithLeaderRetry(admin, config, NAR.toString(),
                "public", "default", "regex-pattern-it");

        // Poll until the function instance is RUNNING, then a 5s grace period for initialize()
        // to prime the TableView before producing input messages.
        long fnReadyDeadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < fnReadyDeadline) {
            try {
                var status = admin.functions().getFunctionStatus("public", "default", "regex-pattern-it");
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        Thread.sleep(5_000);

        // 4. Drive a message to the INITIAL topic — must arrive on the output.
        try (Producer<byte[]> p = client.newProducer(Schema.BYTES).topic(INITIAL_TOPIC).create()) {
            p.newMessage().key("AAPL").property("region", "US")
                    .value("from-initial".getBytes(StandardCharsets.UTF_8)).send();
        }

        Message<byte[]> m1 = outputConsumer.receive(30, TimeUnit.SECONDS);
        assertThat(m1).isNotNull();
        assertThat(new String(m1.getData(), StandardCharsets.UTF_8)).isEqualTo("from-initial");

        // 5. Create the LATE-BOUND topic and wait long enough for pattern auto-discovery
        //    (Pulsar's default patternAutoDiscoveryPeriod is 60s).
        admin.topics().createNonPartitionedTopic(LATE_TOPIC);
        LOG.info("Created late-bound topic {}; waiting 75s for pattern discovery...", LATE_TOPIC);
        Thread.sleep(75_000);

        try (Producer<byte[]> p = client.newProducer(Schema.BYTES).topic(LATE_TOPIC).create()) {
            p.newMessage().key("MSFT").property("region", "US")
                    .value("from-late".getBytes(StandardCharsets.UTF_8)).send();
        }

        Message<byte[]> m2 = outputConsumer.receive(30, TimeUnit.SECONDS);
        assertThat(m2).as("Late-bound topic should be discovered and filtered").isNotNull();
        assertThat(new String(m2.getData(), StandardCharsets.UTF_8)).isEqualTo("from-late");

        outputConsumer.close();
    }

    private static Path findNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "target");
        File[] files = target.toFile().listFiles((dir, name) ->
                name.startsWith("selector-filter-function-") && name.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "NAR not found in " + target + ". Run 'mvn package' before this IT.");
        }
        return files[0].toPath();
    }
}
