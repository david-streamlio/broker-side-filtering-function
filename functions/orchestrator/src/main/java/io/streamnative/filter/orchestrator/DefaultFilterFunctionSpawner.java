package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.FilterRequest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Production {@link FilterFunctionSpawner} backed by {@link PulsarAdmin}. For each REGISTER on an
 * unknown input topic, it:
 *
 * <ol>
 *   <li>Derives a stable filter-function name + control topic from the registry key (via
 *       {@link TopicKeySanitizer}).</li>
 *   <li>Checks whether the function is already running (idempotent on orchestrator restart with
 *       stale registry replay).</li>
 *   <li>Creates and configures the per-function control topic (compaction on by default).</li>
 *   <li>Spawns the filter function via {@code admin.functions().createFunction(config, packageUrl)}
 *       with leader-not-yet-ready retry.</li>
 * </ol>
 *
 * <p>Returns a fresh {@link RegistryEntry} the orchestrator writes to its state TableView.
 */
final class DefaultFilterFunctionSpawner implements FilterFunctionSpawner {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultFilterFunctionSpawner.class);

    /** FQN of the SelectorFilterFunction main class — kept in sync with functions/selector-filter. */
    private static final String FILTER_FUNCTION_CLASS = "io.streamnative.filter.SelectorFilterFunction";

    /** Wall-clock budget for leader-not-yet-ready retry. */
    private static final long DEPLOY_RETRY_BUDGET_MS = 90_000L;
    private static final long DEPLOY_RETRY_SLEEP_MS  = 1_000L;

    private final PulsarAdmin admin;

    DefaultFilterFunctionSpawner(PulsarAdmin admin) {
        this.admin = admin;
    }

    @Override
    public RegistryEntry spawn(String registryKey, FilterRequest req, OrchestratorConfig cfg) throws Exception {
        boolean isPattern = req.getTopicsPattern() != null && !req.getTopicsPattern().isBlank();
        String sanitizedKey = isPattern
                ? TopicKeySanitizer.forPattern(registryKey)
                : TopicKeySanitizer.forInputTopic(registryKey);

        String functionName = "selector-filter-" + sanitizedKey;
        String controlTopic = String.format("persistent://%s/%s/ctrl-%s",
                cfg.controlTopicTenant(), cfg.controlTopicNamespace(), sanitizedKey);

        if (functionExists(cfg.functionTenant(), cfg.functionNamespace(), functionName)) {
            LOG.info("Filter function {} already exists for registry key=[{}]; reusing controlTopic={}",
                    functionName, registryKey, controlTopic);
            return RegistryEntry.newEntry(controlTopic, functionName, req);
        }

        prepareControlTopic(controlTopic, cfg);
        deployFilterFunction(buildFunctionConfig(registryKey, req, cfg, functionName, controlTopic, isPattern),
                cfg.functionPackageUrl(), cfg.functionTenant(), cfg.functionNamespace(), functionName);

        LOG.info("Spawned filter function {} for registry key=[{}] controlTopic={}",
                functionName, registryKey, controlTopic);
        return RegistryEntry.newEntry(controlTopic, functionName, req);
    }

    // ---------- helpers ----------

    private boolean functionExists(String tenant, String ns, String name) throws PulsarAdminException {
        try {
            admin.functions().getFunctionStatus(tenant, ns, name);
            return true;
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
    }

    private void prepareControlTopic(String controlTopic, OrchestratorConfig cfg) {
        try {
            admin.topics().createNonPartitionedTopic(controlTopic);
            LOG.debug("Created control topic {}", controlTopic);
        } catch (PulsarAdminException.ConflictException ignored) {
            // Already exists — fine.
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to create control topic {}: {}. Function start may rely on auto-create.",
                    controlTopic, e.getMessage());
        }

        if (cfg.applyCompaction()) {
            try {
                admin.topicPolicies().setCompactionThreshold(controlTopic, cfg.compactionThresholdBytes());
                LOG.debug("Set compactionThreshold={} on {}", cfg.compactionThresholdBytes(), controlTopic);
            } catch (PulsarAdminException e) {
                LOG.warn("Failed to set compactionThreshold on {}: {}", controlTopic, e.getMessage());
            }
        }
    }

    private FunctionConfig buildFunctionConfig(String registryKey, FilterRequest req,
                                               OrchestratorConfig cfg, String functionName,
                                               String controlTopic, boolean isPattern) {
        FunctionConfig.FunctionConfigBuilder b = FunctionConfig.builder()
                .tenant(cfg.functionTenant())
                .namespace(cfg.functionNamespace())
                .name(functionName)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className(FILTER_FUNCTION_CLASS)
                .subscriptionPosition(SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(cfg.functionParallelism())
                .userConfig(buildUserConfig(controlTopic));
        if (isPattern) {
            b.topicsPattern(req.getTopicsPattern());
        } else {
            // Single registry key on this spawn path — the orchestrator iterates per-input-topic and
            // spawns separately for multi-input requests. The function's input list is just the one
            // registry key.
            b.inputs(java.util.List.of(registryKey));
        }
        return b.build();
    }

    private static Map<String, Object> buildUserConfig(String controlTopic) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("control-topic", controlTopic);
        return cfg;
    }

    /**
     * Deploy with retry on "Leader not yet ready" / "Service Unavailable" — the broker's function
     * worker may not be fully up-and-leader yet. Lifted from the IT helper deployWithLeaderRetry.
     * "Already exists" is treated as success (idempotent: orchestrator restart can race with stale
     * registry replay).
     */
    private void deployFilterFunction(FunctionConfig fc, String packageUrl,
                                      String tenant, String ns, String name) throws Exception {
        long deadline = System.currentTimeMillis() + DEPLOY_RETRY_BUDGET_MS;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.functions().createFunction(fc, packageUrl);
                return;
            } catch (PulsarAdminException e) {
                last = e;
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (msg.contains("already exists")) {
                    LOG.info("Function {} already exists during create; treating as success", name);
                    return;
                }
                if (msg.contains("Leader not yet ready") || msg.contains("Service Unavailable")) {
                    LOG.debug("Function deploy {}/{}/{}: leader not ready, retrying in {} ms",
                            tenant, ns, name, DEPLOY_RETRY_SLEEP_MS);
                    Thread.sleep(DEPLOY_RETRY_SLEEP_MS);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function spawn timed out waiting for leader readiness: " + name, last);
    }
}
