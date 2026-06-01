package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.FilterRequest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionInstanceStats;
import org.apache.pulsar.common.policies.data.FunctionStats;
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
 * Live IT for the continuous existence-reconcile loop. Deploys the orchestrator with
 * {@code existence-reconcile-enabled=true} (short interval) and idle-GC disabled so the only
 * background loop is the reconciler. Proves the gap this feature closes: a filter function deleted
 * out-of-band while a live query still references it is automatically re-created, and end-to-end
 * filtering resumes — the control topic survives the delete, so the re-spawned function reloads its
 * selector state and (idempotently) the reconciler republishes it.
 *
 * <p>Runs in its own class (own container) so the GC-disabled assumption can't be perturbed by the
 * other ITs.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OrchestratorExistenceReconcileIT {

    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorExistenceReconcileIT.class);

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-reconcile-it";

    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/reconcile-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/reconcile-orchestrator-state";
    private static final String INPUT_RECONCILE    = "persistent://public/default/reconcile-input";

    // Aggressive reconcile cadence so the test finishes well under a minute.
    private static final long RECONCILE_INTERVAL_SECONDS = 3L;
    private static final long RECONCILE_MAX_ATTEMPTS      = 5L;

    private static final Path FILTER_NAR       = findFilterNar();
    private static final Path ORCHESTRATOR_NAR = findOrchestratorNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.1"))
            .withFunctionsWorker()
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PulsarClient client;
    private PulsarAdmin admin;
    private Producer<byte[]> producer;
    /** Drives no-op HEARTBEATs so process() runs and drains the deferred reconcile-metrics map. */
    private Producer<byte[]> orchestratorDrainProducer;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();

        awaitDefaultNamespace();

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_RECONCILE);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for reconcile IT")
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
                        Map.entry(OrchestratorConfig.KEY_STATE_TOPIC,            ORCH_STATE_TOPIC),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PACKAGE_URL,   FILTER_PACKAGE_URL),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_TENANT,        FN_TENANT),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_NAMESPACE,     FN_NAMESPACE),
                        Map.entry(OrchestratorConfig.KEY_FUNCTION_PARALLELISM,   1),
                        Map.entry(OrchestratorConfig.KEY_APPLY_COMPACTION,       true),
                        Map.entry(OrchestratorConfig.KEY_PULSAR_ADMIN_URL,       "http://localhost:8080"),
                        // Idle-GC OFF: the reconciler is the only background loop in this IT.
                        Map.entry(OrchestratorConfig.KEY_IDLE_GC_ENABLED,        false),
                        Map.entry(OrchestratorConfig.KEY_EXISTENCE_RECONCILE_ENABLED,      true),
                        Map.entry(OrchestratorConfig.KEY_EXISTENCE_RECONCILE_INTERVAL,     RECONCILE_INTERVAL_SECONDS),
                        Map.entry(OrchestratorConfig.KEY_EXISTENCE_RECONCILE_MAX_ATTEMPTS, RECONCILE_MAX_ATTEMPTS)))
                .build();
        deployWithLeaderRetry(admin, orchConfig, ORCHESTRATOR_NAR.toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        // First function deploy on the functions-worker can be slow to reach RUNNING on a cold,
        // loaded host — give it generous headroom (the spawned filter waits below stay at 90s).
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 150_000L);
        Thread.sleep(5_000); // grace for TableView open + reconciler start

        producer = client.newProducer(Schema.BYTES).topic(INPUT_RECONCILE).create();
        orchestratorDrainProducer = client.newProducer(Schema.BYTES).topic(SDK_INPUT_TOPIC).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (producer != null) producer.close();
        if (orchestratorDrainProducer != null) orchestratorDrainProducer.close();
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
                        .messageTtlSeconds(120)
                        .retentionTimeMinutes(2)
                        .retentionSizeMB(8)
                        .inactiveDeleteSeconds(120)
                        .build())
                .heartbeatIntervalSeconds(3L)
                .build();
    }

    // ---------------------------------------------------------------- Test

    @Test
    void missingFunctionIsRespawnedAndFilteringResumes() throws Exception {
        String expectedFilterFn = "selector-filter-" + TopicKeySanitizer.forInputTopic(INPUT_RECONCILE);

        double baselineRespawned = getOrchestratorUserMetric(FilterReconciler.M_MISSING_RESPAWNED);

        // Keep the Session + consumer OPEN for the whole test so the query stays active
        // (activeQueryIds non-empty) — the reconcile candidate condition.
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_RECONCILE)
                     .selector("region = 'US'")
                     .subscribe()) {

            // Phase 1: prove the filter is genuinely alive before we delete it.
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000); // grace for the filter's control TableView to load the QueryMessage
            producer.newMessage().key("AAPL").property("region", "US")
                    .value("before".getBytes(StandardCharsets.UTF_8)).send();
            assertThat(receiveUntil(consumer, "before", 30))
                    .as("filter must be live before we delete it").isTrue();

            // Phase 2: delete the function OUT-OF-BAND. The control topic survives, so the
            // re-spawned function reloads its selector from the existing TableView.
            admin.functions().deleteFunction(FN_TENANT, FN_NAMESPACE, expectedFilterFn);
            assertThat(functionAbsent(expectedFilterFn))
                    .as("function should be gone immediately after delete").isTrue();

            // Phase 3: the reconciler must re-create it within a few intervals.
            long deadline = System.currentTimeMillis()
                    + TimeUnit.SECONDS.toMillis(RECONCILE_INTERVAL_SECONDS * 4 + 90);
            boolean reappeared = false;
            while (System.currentTimeMillis() < deadline) {
                if (!functionAbsent(expectedFilterFn)) { reappeared = true; break; }
                Thread.sleep(1_000);
            }
            assertThat(reappeared).as("reconciler should re-spawn the missing filter function").isTrue();
            waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, expectedFilterFn, 90_000L);
            Thread.sleep(3_000); // grace for the re-spawned filter to reload its TableView state

            // Phase 4: end-to-end filtering resumes — a NEW matching message reaches the consumer.
            producer.newMessage().key("MSFT").property("region", "US")
                    .value("after".getBytes(StandardCharsets.UTF_8)).send();
            assertThat(receiveUntil(consumer, "after", 60))
                    .as("filtering must resume after the function is reconciled back").isTrue();
        }

        // The reconcile re-spawn surfaces via the deferred-flush metric path (driven by no-op
        // HEARTBEATs into process()).
        awaitMetricDelta(FilterReconciler.M_MISSING_RESPAWNED, baselineRespawned, 1.0, 20_000);
    }

    // ---------------------------------------------------------------- helpers

    /** Receive (and ack) until a message with the expected UTF-8 body arrives, or timeout. */
    private boolean receiveUntil(Consumer<byte[]> consumer, String expected, long timeoutSeconds)
            throws Exception {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSeconds);
        while (System.currentTimeMillis() < deadline) {
            Message<byte[]> m = consumer.receive(5, TimeUnit.SECONDS);
            if (m == null) continue;
            consumer.acknowledge(m);
            if (expected.equals(new String(m.getData(), StandardCharsets.UTF_8))) {
                return true;
            }
            // Otherwise it's a redelivered older message (the re-spawned filter resubscribes at
            // Earliest); keep draining until the one we sent for this phase shows up.
        }
        return false;
    }

    private boolean functionAbsent(String name) throws Exception {
        return !admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE).contains(name);
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
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness: " + name, last);
    }

    private double getOrchestratorUserMetric(String name) {
        String pulsarKey = "user_metric_" + name + "_sum";
        try {
            FunctionStats stats = admin.functions().getFunctionStats(FN_TENANT, FN_NAMESPACE, ORCH_NAME);
            double total = 0.0;
            for (FunctionInstanceStats inst : stats.getInstances()) {
                Map<String, Double> userMetrics = inst.getMetrics().getUserMetrics();
                if (userMetrics != null) {
                    Double v = userMetrics.get(pulsarKey);
                    if (v != null) total += v;
                }
            }
            return total;
        } catch (Exception e) {
            return 0.0;
        }
    }

    private void awaitMetricDelta(String name, double baseline, double minDelta, long timeoutMs)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        long lastDrain = 0L;
        double last = 0.0;
        while (System.currentTimeMillis() < deadline) {
            last = getOrchestratorUserMetric(name);
            if (last - baseline >= minDelta) return;
            if (System.currentTimeMillis() - lastDrain > 2_000) {
                publishNoOpHeartbeat();
                lastDrain = System.currentTimeMillis();
            }
            Thread.sleep(500);
        }
        throw new AssertionError(String.format(
                "metric %s did not grow by at least %s within %d ms; baseline=%s last=%s",
                name, minDelta, timeoutMs, baseline, last));
    }

    private void publishNoOpHeartbeat() {
        try {
            byte[] payload = MAPPER.writeValueAsBytes(
                    FilterRequest.heartbeat(List.of("metric-drain-" + System.nanoTime())));
            orchestratorDrainProducer.newMessage().key("drain").value(payload).send();
        } catch (Exception e) {
            LOG.debug("no-op HEARTBEAT publish failed: {}", e.getMessage());
        }
    }

    private static void waitForFunctionRunning(PulsarAdmin admin, String tenant, String ns,
                                               String name, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String lastDiag = "(no status fetched)";
        while (System.currentTimeMillis() < deadline) {
            try {
                FunctionStatus status = admin.functions().getFunctionStatus(tenant, ns, name);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {}/{}/{} is RUNNING", tenant, ns, name);
                    return;
                }
                StringBuilder sb = new StringBuilder("numInstances=").append(status.getNumInstances())
                        .append(" numRunning=").append(status.getNumRunning());
                for (FunctionStatus.FunctionInstanceStatus inst : status.getInstances()) {
                    FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData d = inst.getStatus();
                    sb.append(" [inst#").append(inst.getInstanceId())
                      .append(" running=").append(d.isRunning())
                      .append(" restarts=").append(d.getNumRestarts())
                      .append(" error=").append(d.getError())
                      .append(" sysExc=").append(d.getLatestSystemExceptions())
                      .append(" userExc=").append(d.getLatestUserExceptions()).append(']');
                }
                lastDiag = sb.toString();
            } catch (Exception e) {
                lastDiag = "getFunctionStatus threw: " + e.getMessage();
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Function did not reach RUNNING within " + timeoutMs
                + " ms: " + name + " — last status: " + lastDiag);
    }

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
