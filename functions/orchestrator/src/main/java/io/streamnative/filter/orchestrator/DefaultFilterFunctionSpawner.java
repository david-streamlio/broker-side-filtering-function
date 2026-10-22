/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.FilterRequest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.functions.BatchingConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
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

    /**
     * Creates a new live filter's subscription at the end of its topics, so it starts at registration;
     * {@code null} leaves it to the function, which then starts at the earliest retained message.
     */
    private final SubscriptionPositioner positioner;

    DefaultFilterFunctionSpawner(PulsarAdmin admin) {
        this(admin, null);
    }

    DefaultFilterFunctionSpawner(PulsarAdmin admin, SubscriptionPositioner positioner) {
        this.positioner = positioner;
        this.admin = admin;
    }

    @Override
    public RegistryEntry spawn(String registryKey, FilterRequest req, OrchestratorConfig cfg,
                               ControlTopicAction beforeDeploy) throws Exception {
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
        // A live query starts at its registration: the filter's subscription is created at the end of
        // its topics before the function exists. (At Earliest, the first query on a topic would get the
        // topic's whole retained history.) Topics that match a pattern later get the subscription at
        // Earliest from the function, which for a new topic is all of it.
        if (positioner != null) {
            positioner.createAtEnd(isPattern ? null : java.util.List.of(registryKey),
                    isPattern ? req.getTopicsPattern() : null, functionName);
        }
        beforeDeploy.accept(controlTopic);
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
        // Named after the function, so the subscription created at registration is the one it attaches to.
        FunctionConfig.FunctionConfigBuilder b = baseFunctionConfig(cfg, functionName, controlTopic)
                .subName(functionName);
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

    /** Settings shared by the per-topic filters and the per-query windowed filters. */
    private FunctionConfig.FunctionConfigBuilder baseFunctionConfig(OrchestratorConfig cfg, String functionName,
                                                                    String controlTopic) {
        return FunctionConfig.builder()
                .tenant(cfg.functionTenant())
                .namespace(cfg.functionNamespace())
                .name(functionName)
                // Populate `jar` with the package URL up front. Self-managed Pulsar resolves the
                // URL passed to createFunctionWithUrl(...) server-side, but SN Cloud's
                // function-mesh admission webhook validates `spec.java.jar` BEFORE that
                // conversion runs — an empty value fails the webhook with "jar cannot be empty
                // in java runtime". Setting both is harmless on the URL-based path.
                .jar(cfg.functionPackageUrl())
                .runtime(FunctionConfig.Runtime.JAVA)
                .className(FILTER_FUNCTION_CLASS)
                .subscriptionPosition(SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.MANUAL)
                .retainKeyOrdering(true)
                .forwardSourceMessageProperty(true)
                .cleanupSubscription(true)
                .parallelism(cfg.functionParallelism())
                .resources(Resources.builder()
                        .cpu(cfg.functionCpu())
                        .ram(cfg.functionMemoryBytes())
                        .build())
                .producerConfig(buildProducerConfig(cfg))
                .userConfig(buildUserConfig(controlTopic, cfg));
    }

    @Override
    public RegistryEntry spawnWindowed(String queryId, String sourceTopic, String subscription, FilterRequest req,
                                       OrchestratorConfig cfg, ControlTopicAction beforeDeploy) throws Exception {
        String key = TopicKeySanitizer.forWindowedQuery(queryId);
        String functionName = "selector-filter-win-" + key;
        String controlTopic = String.format("persistent://%s/%s/ctrl-win-%s",
                cfg.controlTopicTenant(), cfg.controlTopicNamespace(), key);

        prepareControlTopic(controlTopic, cfg);
        beforeDeploy.accept(controlTopic);
        // The subscription already exists, positioned at the window start, so subscriptionPosition
        // doesn't apply. cleanupSubscription (from the base config) deletes it with the function.
        FunctionConfig fc = baseFunctionConfig(cfg, functionName, controlTopic)
                .inputs(java.util.List.of(sourceTopic))
                .subName(subscription)
                .parallelism(1)
                .build();
        deployFilterFunction(fc, cfg.functionPackageUrl(), cfg.functionTenant(), cfg.functionNamespace(),
                functionName);
        LOG.info("Spawned windowed filter {} for queryId={} source={} subscription={} controlTopic={}",
                functionName, queryId, sourceTopic, subscription, controlTopic);
        return RegistryEntry.newEntry(controlTopic, functionName, req);
    }

    private static Map<String, Object> buildUserConfig(String controlTopic, OrchestratorConfig cfg) {
        Map<String, Object> userConfig = new HashMap<>();
        userConfig.put("control-topic", controlTopic);
        // Diagnostic passthrough — only injected when enabled, so a spawned filter's userConfig stays
        // minimal in the steady state (the filter defaults log-timing-every to 0 / off).
        if (cfg.filterLogTimingEvery() > 0) {
            userConfig.put("log-timing-every", cfg.filterLogTimingEvery());
        }
        return userConfig;
    }

    /**
     * Output-producer config for the spawned filter. Tunes batching for latency: the
     * Pulsar-Functions default batching delay dominates the filter's value-add latency, so we set
     * {@code batchingMaxPublishDelayMs} from {@link OrchestratorConfig#filterOutputBatchMaxPublishDelayMs()}
     * (default 1ms). A value of {@code 0} disables batching entirely.
     */
    private static ProducerConfig buildProducerConfig(OrchestratorConfig cfg) {
        int delayMs = cfg.filterOutputBatchMaxPublishDelayMs();
        BatchingConfig batching = new BatchingConfig(
                delayMs > 0,                    // enabled
                delayMs > 0 ? delayMs : null,   // batchingMaxPublishDelayMs
                null, null, null, null);
        ProducerConfig producerConfig = new ProducerConfig();
        producerConfig.setBatchingConfig(batching);
        return producerConfig;
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
                // createFunctionWithUrl handles `function://...` package URLs (and `http(s)://`,
                // `file://`); createFunction treats its String arg as a local file path. Both
                // overloads exist for a reason.
                admin.functions().createFunctionWithUrl(fc, packageUrl);
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
