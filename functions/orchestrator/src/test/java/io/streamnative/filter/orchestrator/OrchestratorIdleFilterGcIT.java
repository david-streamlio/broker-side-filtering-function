package io.streamnative.filter.orchestrator;

import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live IT for idle-filter GC. Uses a dedicated testcontainer + orchestrator deployment with GC
 * enabled and short TTL/sweep intervals so the tests finish in well under a minute. Runs in its
 * own class so the GC-disabled tests in {@link OrchestratorFunctionIT} (which depend on filter
 * functions persisting across tests) are not affected.
 *
 * <p>Asserts the two scenarios that matter operationally:
 * <ol>
 *   <li>A filter function whose only queryId UNREGISTERs is deleted after the idle TTL elapses
 *       (function removed, control topic removed, registry entry retracted).</li>
 *   <li>A REGISTER arriving inside the TTL window resets the idle clock — the filter survives.</li>
 * </ol>
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrchestratorIdleFilterGcIT {

    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorIdleFilterGcIT.class);

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-gc-it";

    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/gc-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/gc-orchestrator-state";

    // Each test uses a distinct input topic so we have an unambiguous spawned-filter name to track.
    private static final String INPUT_GC      = "persistent://public/default/gc-input";
    private static final String INPUT_REVIVED = "persistent://public/default/gc-revived-input";

    // Tight enough to finish a test pass in <30s, loose enough that incidental jitter doesn't
    // flake the test 2 "revived" check.
    private static final long IDLE_TTL_SECONDS         = 5L;
    private static final long SWEEP_INTERVAL_SECONDS   = 2L;

    private static final Path FILTER_NAR       = findFilterNar();
    private static final Path ORCHESTRATOR_NAR = findOrchestratorNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.0.10"))
            .withFunctionsWorker()
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;
    private Producer<byte[]> producerGc;
    private Producer<byte[]> producerRevived;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_GC);
        admin.topics().createNonPartitionedTopic(INPUT_REVIVED);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for GC IT")
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
                .userConfig(Map.ofEntries(
                        Map.entry(OrchestratorConfig.KEY_STATE_TOPIC,             ORCH_STATE_TOPIC),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PACKAGE_URL,    FILTER_PACKAGE_URL),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_TENANT,         FN_TENANT),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_NAMESPACE,      FN_NAMESPACE),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PARALLELISM,    1),
                        Map.entry(OrchestratorConfig.KEY_APPLY_COMPACTION,        true),
                        Map.entry(OrchestratorConfig.KEY_PULSAR_ADMIN_URL,        "http://localhost:8080"),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_ENABLED,         true),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_TTL_SECONDS,     IDLE_TTL_SECONDS),
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_SWEEP_INTERVAL,  SWEEP_INTERVAL_SECONDS)))
                .build();
        deployWithLeaderRetry(admin, orchConfig, ORCHESTRATOR_NAR.toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 90_000L);
        Thread.sleep(5_000); // grace for TableView open + sweeper start

        producerGc      = client.newProducer(Schema.BYTES).topic(INPUT_GC).create();
        producerRevived = client.newProducer(Schema.BYTES).topic(INPUT_REVIVED).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (producerGc != null) producerGc.close();
        if (producerRevived != null) producerRevived.close();
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    private Session newSession() throws Exception {
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT)
                .privateTopicNamespace(FN_NAMESPACE)
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(60)
                        .retentionTimeMinutes(1)
                        .retentionSizeMB(8)
                        .inactiveDeleteSeconds(60)
                        .build())
                .build();
    }

    // ---------------------------------------------------------------- Tests

    @Test
    void idleFilterIsDeletedAfterTtl() throws Exception {
        String expectedFilterFn  = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_GC);
        String expectedCtrlTopic = "persistent://" + FN_TENANT + "/" + FN_NAMESPACE
                + "/ctrl-" + TopicKeySanitizer.forInputTopic(INPUT_GC);

        // Phase 1: REGISTER + drive a message so we know the filter is genuinely alive.
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_GC)
                     .selector("region = 'US'")
                     .subscribe()) {

            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000);

            producerGc.newMessage().key("AAPL").property("region", "US")
                    .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
            Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(m).as("filter must be live before UNREGISTER").isNotNull();
            assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("us-1");
            consumer.acknowledge(m);
        }
        // consumer.close() → orchestrator UNREGISTER → tombstone → activeQueryIds becomes empty
        // → idleSince stamped → after TTL the sweeper deletes function + control topic + t: entry.

        long deadline = System.currentTimeMillis()
                + TimeUnit.SECONDS.toMillis(IDLE_TTL_SECONDS + SWEEP_INTERVAL_SECONDS * 3 + 10);
        boolean functionGone = false;
        boolean topicGone    = false;
        while (System.currentTimeMillis() < deadline) {
            List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
            functionGone = !functions.contains(expectedFilterFn);
            try {
                admin.topics().getStats(expectedCtrlTopic);
                topicGone = false;
            } catch (PulsarAdminException.NotFoundException e) {
                topicGone = true;
            }
            if (functionGone && topicGone) break;
            Thread.sleep(500);
        }
        assertThat(functionGone).as("idle filter function should be GCed after TTL").isTrue();
        assertThat(topicGone).as("control topic should be GCed after TTL").isTrue();
    }

    @Test
    void recentReRegisterPreventsGc() throws Exception {
        String expectedFilterFn = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_REVIVED);

        // Phase 1: REGISTER, then close (UNREGISTER). Filter goes idle.
        try (Session firstSession = newSession();
             Consumer<byte[]> firstConsumer = firstSession.newConsumer()
                     .topic(INPUT_REVIVED)
                     .selector("region = 'US'")
                     .subscribe()) {
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000);
        }

        // Phase 2: re-REGISTER within the TTL window. The orchestrator processes a new REGISTER,
        // adds the new queryId to activeQueryIds, clears idleSince. The sweeper must skip this
        // entry on every subsequent tick.
        try (Session secondSession = newSession();
             Consumer<byte[]> secondConsumer = secondSession.newConsumer()
                     .topic(INPUT_REVIVED)
                     .selector("region = 'UK'")
                     .subscribe()) {

            Thread.sleep(3_000); // orchestrator processes the new REGISTER, reuses the filter

            // Wait well past TTL + several sweep ticks. If GC was wrongly triggered, the function
            // is gone by now.
            Thread.sleep(TimeUnit.SECONDS.toMillis(IDLE_TTL_SECONDS + SWEEP_INTERVAL_SECONDS * 3 + 5));

            List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
            assertThat(functions).as("re-REGISTER inside the TTL window must keep the filter alive")
                    .contains(expectedFilterFn);

            // Sanity: the live filter still processes input. UK matches should flow.
            producerRevived.newMessage().key("BARC").property("region", "UK")
                    .value("uk-revived".getBytes(StandardCharsets.UTF_8)).send();
            Message<byte[]> m = secondConsumer.receive(30, TimeUnit.SECONDS);
            assertThat(m).isNotNull();
            assertThat(new String(m.getData(), StandardCharsets.UTF_8)).isEqualTo("uk-revived");
            secondConsumer.acknowledge(m);
        }
    }

    // ---------------------------------------------------------------- helpers (mirrored from OrchestratorFunctionIT)

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
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness: " + name, last);
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
        Path target = Paths.get(System.getProperty("user.dir"), "..", "selector-filter", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-function-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Filter NAR not found in " + target);
        }
        return files[0].toPath();
    }

    private static Path findOrchestratorNar() {
        Path target = Paths.get(System.getProperty("user.dir"), "target");
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-orchestrator-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException("Orchestrator NAR not found in " + target);
        }
        return files[0].toPath();
    }
}
