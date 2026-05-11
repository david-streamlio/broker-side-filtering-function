package io.streamnative.filter.client;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
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
 * Full end-to-end against a live Pulsar container with the function NAR deployed and the client
 * driving everything.
 *
 * <p>Requires Docker and a pre-built {@code function/target/selector-filter-function-*.nar}. Run
 * {@code mvn -pl function package} before invoking this IT (or use {@code mvn verify} at the
 * reactor root, which builds in dependency order automatically).
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SessionIT {

    private static final Logger LOG = LoggerFactory.getLogger(SessionIT.class);

    private static final String NAR_DEST = "/pulsar/selector-filter-function.nar";
    private static final String CONTROL_TOPIC = "persistent://public/default/queries";
    private static final String INPUT_TOPIC   = "persistent://public/default/trades-it";
    private static final String EXTRA_INPUT_TOPIC = "persistent://public/default/extra-it";
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
        admin.topics().createNonPartitionedTopic(EXTRA_INPUT_TOPIC);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        // Deploy the function before driving messages. Inputs include both topics from the
        // start — Pulsar refuses to alter the input set after function creation.
        FunctionConfig config = FunctionConfig.builder()
                .tenant(FN_TENANT)
                .namespace(FN_NS)
                .name(FN_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.SelectorFilterFunction")
                .inputs(java.util.List.of(INPUT_TOPIC, EXTRA_INPUT_TOPIC))
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(1)
                .userConfig(Map.of("control-topic", (Object) CONTROL_TOPIC))
                .build();
        deployWithLeaderRetry(admin, config, NAR.toString(), FN_TENANT, FN_NS, FN_NAME);

        // Poll until the function instance is RUNNING, then a 5s grace period for
        // initialize() to prime the TableView before producing input messages.
        long fnReadyDeadline = System.currentTimeMillis() + 90_000L;
        while (System.currentTimeMillis() < fnReadyDeadline) {
            try {
                var status = admin.functions().getFunctionStatus(FN_TENANT, FN_NS, FN_NAME);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        Thread.sleep(5_000);

        inputProducer = client.newProducer(Schema.BYTES).topic(INPUT_TOPIC).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (inputProducer != null) inputProducer.close();
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    @Test
    void newConsumerEndToEnd() throws Exception {
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_TOPIC)
                     .selector("region = 'US'")
                     .subscribe()) {

            FilterControl fc = (FilterControl) consumer;

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

            String privateTopic = fc.privateTopic();
            consumer.close();

            // After close: poll for up to 15 seconds for the private topic to disappear.
            assertTopicEventuallyDeleted(privateTopic, 15_000L);
        }
    }

    /**
     * Verify the {@link FilterControl#setSelector(String)} round-trip: register a consumer with
     * selector A, produce a mix, receive only A-matches; switch to selector B via setSelector,
     * produce a new mix, receive only B-matches. The queryId must be stable across the swap.
     */
    @Test
    void setSelectorHotReloadsAgainstLiveFunction() throws Exception {
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_TOPIC)
                     .selector("region = 'US'")
                     .subscribe()) {

            FilterControl fc = (FilterControl) consumer;
            String stableQueryId = fc.queryId();
            Thread.sleep(2000); // function picks up the initial registration

            // Phase 1: selector A = "region = 'US'"
            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BMW").property("region", "DE")
                    .value("de-1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BARC").property("region", "UK")
                    .value("uk-1".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> a1 = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(a1).isNotNull();
            assertThat(new String(a1.getData(), StandardCharsets.UTF_8)).isEqualTo("us-1");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only US should match under selector A").isNull();
            consumer.acknowledge(a1);

            // Switch selector mid-stream: "region = 'UK'"
            fc.setSelector("region = 'UK'");
            assertThat(fc.queryId())
                    .as("queryId must be stable across setSelector").isEqualTo(stableQueryId);
            assertThat(fc.currentSelector()).isEqualTo("region = 'UK'");
            Thread.sleep(2000); // give the function's TableView listener time to swap

            // Phase 2: selector B = "region = 'UK'"
            inputProducer.newMessage().key("MSFT").property("region", "US")
                    .value("us-2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("HSBC").property("region", "UK")
                    .value("uk-2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("AUDI").property("region", "DE")
                    .value("de-2".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> b1 = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(b1).isNotNull();
            assertThat(new String(b1.getData(), StandardCharsets.UTF_8)).isEqualTo("uk-2");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only UK should match under selector B").isNull();
            consumer.acknowledge(b1);
        }
    }

    /**
     * Multi-topic {@code __topic__ IN (...)} composition end-to-end. Two input topics, both
     * registered as function inputs; the consumer's source list narrows the routing.
     */
    @Test
    void multiTopicSubscribeRoutesBothSources() throws Exception {
        try (Producer<byte[]> extraProducer = client.newProducer(Schema.BYTES).topic(EXTRA_INPUT_TOPIC).create();
             Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topics(java.util.List.of(INPUT_TOPIC, EXTRA_INPUT_TOPIC))
                     .selector("region = 'US'")
                     .subscribe()) {

            Thread.sleep(2000);

            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("from-a".getBytes(StandardCharsets.UTF_8)).send();
            extraProducer.newMessage().key("MSFT").property("region", "US")
                    .value("from-b".getBytes(StandardCharsets.UTF_8)).send();
            extraProducer.newMessage().key("BMW").property("region", "DE")
                    .value("from-b-de".getBytes(StandardCharsets.UTF_8)).send();

            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < 2; i++) {
                Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
                assertThat(m).isNotNull();
                seen.add(new String(m.getData(), StandardCharsets.UTF_8));
                consumer.acknowledge(m);
            }
            assertThat(seen).containsExactlyInAnyOrder("from-a", "from-b");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only US messages should match").isNull();
        }
    }

    @Test
    void subscribeWithMalformedSelectorFailsLocallyWithoutTouchingControlTopic() throws Exception {
        try (Session session = newSession()) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> session.newConsumer()
                            .topic(INPUT_TOPIC)
                            .selector("THIS IS NOT @@## VALID JMS")
                            .subscribe())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private Session newSession() throws Exception {
        return Session.builder()
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
    }

    private void assertTopicEventuallyDeleted(String privateTopic, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!admin.topics().getList("public/default").contains(privateTopic)) {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(admin.topics().getList("public/default"))
                .as("private topic %s should be cleaned up on close", privateTopic)
                .doesNotContain(privateTopic);
    }

    /**
     * Deploys with retry on "Leader not yet ready", deleting the stale package between
     * attempts (it gets uploaded on the way to the 503 and would otherwise block retries
     * with "Package metadata already exists").
     */
    private static void deployWithLeaderRetry(PulsarAdmin admin, FunctionConfig config, String pkg,
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
                    } catch (Exception ignored) {}
                    Thread.sleep(1_000);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness", last);
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
