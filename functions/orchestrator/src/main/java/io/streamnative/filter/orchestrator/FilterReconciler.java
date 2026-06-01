package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.FilterRequest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Continuous existence-reconcile loop — the "ensure desired filters exist" half of a
 * Kubernetes-style controller. Twin of {@link IdleFilterSweeper} (which is the "delete the ones
 * nobody needs" half): both run on a single-thread {@link ScheduledExecutorService} on a fixed
 * delay and walk the {@code t:} registry, but they operate on disjoint sets — the sweeper repairs
 * by deletion of <em>idle</em> filters (empty {@code activeQueryIds}), this reconciler repairs by
 * re-creation of <em>in-use</em> filters (non-empty {@code activeQueryIds}) whose function has gone
 * missing. The two never fight over the same entry.
 *
 * <p>Scope (locked): <b>missing-only</b>. A crashed <em>instance</em> is restarted by the Pulsar
 * Functions runtime; this loop only re-creates a function whose record is entirely absent
 * ({@link PulsarAdminException.NotFoundException} from {@code getFunctionStatus}) while a live query
 * still references it. Repair is idempotent — it reuses {@link FilterFunctionSpawner#spawn} (which
 * tolerates already-exists and (re)creates the control topic) and then repopulates the control
 * topic with each active query's {@link io.streamnative.filter.common.QueryMessage}, reconstructed
 * from the selector persisted on the {@link QueryEntry}.
 *
 * <p>Each tick:
 * <ol>
 *   <li>Snapshots in-use {@code t:} entries (non-empty {@code activeQueryIds}).</li>
 *   <li>For each: backoff gate → recency re-read → {@code getFunctionStatus}.</li>
 *   <li>On {@code NotFound}: re-spawn (idempotent) + repopulate; on repeated failure, exponential
 *       backoff up to {@code existence-reconcile-max-attempts}, then give up (until the next
 *       orchestrator restart) so a permanently-unspawnable filter doesn't hammer admin forever.</li>
 * </ol>
 *
 * <p>The existing {@code t:} entry is deliberately <b>not</b> overwritten on repair — its
 * {@code activeQueryIds}/{@code idleSince}/{@code createdAt} are preserved; only the broker-side
 * function + control topic are recreated.
 */
final class FilterReconciler implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FilterReconciler.class);

    // Canonical metric names; follow the orchestrator.<area>.<event> convention used elsewhere so
    // operators can read them via `pulsar-admin functions stats`. Dashboards depend on the exact
    // strings — changing them is a breaking change.
    static final String M_EVALUATED          = "orchestrator.reconcile.evaluated";
    static final String M_HEALTHY            = "orchestrator.reconcile.healthy";
    static final String M_MISSING_RESPAWNED  = "orchestrator.reconcile.missing_respawned";
    static final String M_QUERIES_REPUBLISHED = "orchestrator.reconcile.queries_republished";
    static final String M_SKIPPED_RECENT     = "orchestrator.reconcile.skipped_recent";
    static final String M_ERRORS             = "orchestrator.reconcile.errors";
    static final String M_GAVE_UP            = "orchestrator.reconcile.gave_up";

    /** Republishes a {@link io.streamnative.filter.common.QueryMessage} to a control topic. */
    @FunctionalInterface
    interface PublishQuery {
        void publish(String controlTopic, String queryId, String selector, String outputTopic)
                throws Exception;
    }

    private final RegistryStore registry;
    private final PulsarAdmin admin;
    private final OrchestratorConfig config;
    private final Clock clock;
    private final FilterFunctionSpawner spawner;
    private final PublishQuery publishQuery;
    /**
     * Invoked with {@code (metricName, value)} on every relevant reconcile action. Runs on the
     * reconciler scheduler thread, not a {@code process()} thread — see
     * {@link OrchestratorFunction#recordBackgroundMetric} for the deferred-flush rationale.
     */
    private final BiConsumer<String, Double> metrics;

    /** Per-registry-key backoff state. In-memory; cleared on success and on orchestrator restart. */
    private final Map<String, AttemptState> attempts = new ConcurrentHashMap<>();

    private ScheduledExecutorService scheduler;

    FilterReconciler(RegistryStore registry, PulsarAdmin admin, OrchestratorConfig config,
                     Clock clock, FilterFunctionSpawner spawner) {
        this(registry, admin, config, clock, spawner, (c, q, s, o) -> {}, (n, v) -> {});
    }

    FilterReconciler(RegistryStore registry, PulsarAdmin admin, OrchestratorConfig config,
                     Clock clock, FilterFunctionSpawner spawner,
                     PublishQuery publishQuery, BiConsumer<String, Double> metrics) {
        this.registry     = registry;
        this.admin        = admin;
        this.config       = config;
        this.clock        = clock;
        this.spawner      = spawner;
        this.publishQuery = publishQuery;
        this.metrics      = metrics;
    }

    /**
     * Begin running the reconciler on a fixed-delay schedule using
     * {@code existence-reconcile-interval-seconds}. The first tick runs after one interval, not
     * immediately. Calling start more than once is a programmer error and throws.
     */
    void start() {
        if (scheduler != null) {
            throw new IllegalStateException("FilterReconciler already started");
        }
        long intervalSec = config.existenceReconcileIntervalSeconds();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orchestrator-existence-reconciler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::reconcileQuietly, intervalSec, intervalSec, TimeUnit.SECONDS);
        LOG.info("FilterReconciler started: interval={}s, maxAttempts={}",
                intervalSec, config.existenceReconcileMaxAttempts());
    }

    @Override
    public void close() {
        if (scheduler == null) return;
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        scheduler = null;
        LOG.info("FilterReconciler stopped");
    }

    /** Wrapper that catches all throwables so the scheduler doesn't suppress future ticks. */
    private void reconcileQuietly() {
        try {
            reconcile();
        } catch (Throwable t) {
            LOG.warn("FilterReconciler tick failed; will retry on next interval", t);
        }
    }

    /**
     * Run one reconcile tick synchronously and return per-tick stats. Package-private so unit tests
     * can drive ticks deterministically with an injected {@link Clock} rather than waiting on the
     * scheduler.
     */
    ReconcileStats reconcile() {
        ReconcileStats stats = new ReconcileStats();
        Instant now = Instant.now(clock);

        // First pass: snapshot in-use candidate keys. Iterating while issuing writes against the
        // same view is fine here (we don't delete entries), but we still materialize the list so
        // the per-candidate recency re-read is the single source of truth for the act decision.
        List<String> candidateKeys = new ArrayList<>();
        registry.forEachInputTopicEntry((key, entry) -> {
            stats.evaluated++;
            metrics.accept(M_EVALUATED, 1.0);
            if (isInUse(entry)) {
                candidateKeys.add(key);
            }
        });

        // Second pass: backoff gate → recency re-read → health check → repair.
        for (String key : candidateKeys) {
            if (inBackoff(key, now)) {
                stats.skippedRecent++;
                metrics.accept(M_SKIPPED_RECENT, 1.0);
                continue;
            }

            RegistryEntry current = registry.getInputTopicEntry(key);
            if (!isInUse(current)) {
                // Became idle or was retracted since the snapshot — let the sweeper own it.
                LOG.debug("Skipping reconcile for key=[{}]: no longer in use since snapshot", key);
                stats.skippedRecent++;
                metrics.accept(M_SKIPPED_RECENT, 1.0);
                continue;
            }

            boolean missing;
            try {
                admin.functions().getFunctionStatus(config.functionTenant(),
                        config.functionNamespace(), current.getFilterFunctionName());
                missing = false;
            } catch (PulsarAdminException.NotFoundException e) {
                missing = true;
            } catch (PulsarAdminException e) {
                LOG.warn("getFunctionStatus for {} failed: {}. Will retry next tick.",
                        current.getFilterFunctionName(), e.getMessage());
                stats.errors++;
                metrics.accept(M_ERRORS, 1.0);
                recordFailure(key, now);
                continue;
            }

            if (!missing) {
                stats.healthy++;
                metrics.accept(M_HEALTHY, 1.0);
                clearAttempts(key);
                continue;
            }

            // Repair: re-spawn (idempotent) + repopulate the control topic from persisted state.
            try {
                spawner.spawn(key, reconstructRequest(key, current), config);
                stats.missingRespawned++;
                metrics.accept(M_MISSING_RESPAWNED, 1.0);

                int republished = repopulate(current);
                stats.queriesRepublished += republished;
                if (republished > 0) {
                    metrics.accept(M_QUERIES_REPUBLISHED, (double) republished);
                }
                clearAttempts(key);
                LOG.info("Reconcile re-spawned missing filter name={} key=[{}]; republished {} query(ies)",
                        current.getFilterFunctionName(), key, republished);
            } catch (Exception e) {
                int failures = recordFailure(key, now);
                stats.errors++;
                metrics.accept(M_ERRORS, 1.0);
                if (failures >= config.existenceReconcileMaxAttempts()) {
                    markGaveUp(key);
                    stats.gaveUp++;
                    metrics.accept(M_GAVE_UP, 1.0);
                    LOG.warn("Reconcile gave up on key=[{}] after {} attempt(s): {}. Will not retry "
                            + "until the next orchestrator restart or REGISTER.", key, failures, e.getMessage());
                } else {
                    LOG.warn("Reconcile attempt {} for key=[{}] failed: {}. Backing off.",
                            failures, key, e.getMessage());
                }
            }
        }
        return stats;
    }

    /**
     * Rebuild the minimal {@link FilterRequest} {@link FilterFunctionSpawner#spawn} needs from a
     * persisted {@code t:} entry. {@code spawn} reads only the topic binding (pattern vs input
     * topic, derived from {@link RegistryEntry#isPattern()} + the registry key) and the
     * deserializers/clientType — it ignores {@code outputTopic}/{@code selector} (those are
     * per-query and applied during {@link #repopulate}), so we leave them null and never call
     * {@code validate()}.
     */
    private FilterRequest reconstructRequest(String key, RegistryEntry entry) {
        if (entry.isPattern()) {
            return FilterRequest.registerPattern(key, null, null,
                    entry.getKeyDeserializer(), entry.getValueDeserializer(), entry.getClientType());
        }
        return FilterRequest.register(key, null, null,
                entry.getKeyDeserializer(), entry.getValueDeserializer(), entry.getClientType());
    }

    /**
     * Republish a {@link io.streamnative.filter.common.QueryMessage} for every active query on the
     * (now-recreated) control topic, restoring the filter's TableView state. Uses the selector
     * persisted on each {@link QueryEntry}; for legacy entries written before selector persistence
     * (null selector), falls back to the identity selector and logs that the original is
     * unrecoverable. Returns the number of queries republished.
     */
    private int repopulate(RegistryEntry entry) throws Exception {
        Set<String> active = entry.getActiveQueryIds();
        if (active == null || active.isEmpty()) {
            return 0;
        }
        String controlTopic = entry.getControlTopic();
        int count = 0;
        for (String queryId : active) {
            QueryEntry q = registry.getQueryEntry(queryId);
            if (q == null) {
                LOG.debug("Reconcile repopulate: queryId={} has no q: entry; skipping", queryId);
                continue;
            }
            String selector = q.getSelector();
            if (selector == null || selector.isBlank()) {
                selector = OrchestratorFunction.IDENTITY_SELECTOR;
                LOG.warn("Reconcile repopulate: queryId={} has no persisted selector (legacy entry); "
                        + "republishing with identity selector '{}'", queryId, selector);
            }
            publishQuery.publish(controlTopic, queryId, selector, q.getOutputTopic());
            count++;
        }
        return count;
    }

    private static boolean isInUse(RegistryEntry e) {
        return e != null && e.getActiveQueryIds() != null && !e.getActiveQueryIds().isEmpty();
    }

    // ---------- backoff ----------

    private boolean inBackoff(String key, Instant now) {
        AttemptState s = attempts.get(key);
        if (s == null || s.failures == 0) {
            return false;
        }
        if (s.gaveUp) {
            return true;  // permanent skip until restart / attempts cleared
        }
        // Linear-on-failure-count backoff: wait failures × interval before the next attempt.
        Duration wait = Duration.ofSeconds(config.existenceReconcileIntervalSeconds() * s.failures);
        return Duration.between(s.lastAttempt, now).compareTo(wait) < 0;
    }

    private int recordFailure(String key, Instant now) {
        AttemptState s = attempts.computeIfAbsent(key, k -> new AttemptState());
        s.failures++;
        s.lastAttempt = now;
        return s.failures;
    }

    private void markGaveUp(String key) {
        AttemptState s = attempts.get(key);
        if (s != null) {
            s.gaveUp = true;
        }
    }

    private void clearAttempts(String key) {
        attempts.remove(key);
    }

    private static final class AttemptState {
        int failures;
        Instant lastAttempt;
        boolean gaveUp;
    }

    /** Per-tick counters. Useful for tests to assert reconciler decisions. */
    static final class ReconcileStats {
        int evaluated;
        int healthy;
        int missingRespawned;
        int queriesRepublished;
        int skippedRecent;
        int errors;
        int gaveUp;

        @Override
        public String toString() {
            return "ReconcileStats{evaluated=" + evaluated
                    + ", healthy=" + healthy
                    + ", missingRespawned=" + missingRespawned
                    + ", queriesRepublished=" + queriesRepublished
                    + ", skippedRecent=" + skippedRecent
                    + ", errors=" + errors
                    + ", gaveUp=" + gaveUp + "}";
        }
    }
}
