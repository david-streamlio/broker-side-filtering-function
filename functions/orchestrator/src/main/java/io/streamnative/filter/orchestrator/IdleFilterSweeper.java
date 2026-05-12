package io.streamnative.filter.orchestrator;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Periodic GC for idle filter functions. A filter function is "idle" when no queryIds reference it
 * (its {@link RegistryEntry#getActiveQueryIds()} is empty) and an {@code idleGcTtlSeconds} grace
 * window has elapsed since it became idle (per {@link RegistryEntry#getIdleSince()}).
 *
 * <p>The sweeper runs in a single-thread {@link ScheduledExecutorService} on a fixed delay. Each
 * tick:
 * <ol>
 *   <li>Snapshots all {@code t:} entries via {@link RegistryStore#forEachInputTopicEntry}.</li>
 *   <li>For each entry that looks GC-eligible in the snapshot, re-reads the entry to catch any
 *       concurrent REGISTER (the recency check).</li>
 *   <li>If still GC-eligible, deletes (in order): the filter function, the control topic, and the
 *       {@code t:} registry entry. {@code NotFound} on any step is tolerated so a half-finished GC
 *       from a previous tick (e.g. orchestrator restart mid-sequence) self-heals on retry.</li>
 * </ol>
 *
 * <p>Errors on a single candidate are logged and the sweeper continues to the next — one bad
 * filter doesn't poison the whole tick. Errors that leave a partial state (function deleted but
 * topic survived) cause the {@code t:} entry to stay; the next tick re-evaluates and retries.
 */
final class IdleFilterSweeper implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(IdleFilterSweeper.class);

    // Canonical metric names emitted on every relevant sweeper action. These follow the
    // orchestrator.<area>.<event> convention used by OrchestratorFunction.process(); operators
    // can read them via `pulsar-admin functions stats`. Dashboards depend on the exact strings,
    // so changing them is a breaking change.
    static final String M_GC_CANDIDATES_EVALUATED    = "orchestrator.gc.candidates_evaluated";
    static final String M_GC_FUNCTIONS_DELETED       = "orchestrator.gc.functions_deleted";
    static final String M_GC_SKIPPED_RECENT_REGISTER = "orchestrator.gc.skipped_recent_register";
    static final String M_GC_ERRORS                  = "orchestrator.gc.errors";
    static final String M_HEARTBEATS_SYNTHESIZED     = "orchestrator.heartbeats.synthesized";
    static final String M_HEARTBEATS_SKIPPED_RECENT  = "orchestrator.heartbeats.skipped_recent";
    static final String M_HEARTBEATS_ERRORS          = "orchestrator.heartbeats.errors";

    private final RegistryStore registry;
    private final PulsarAdmin admin;
    private final OrchestratorConfig config;
    private final Clock clock;
    /**
     * Invoked with a control topic FQN just before the sweeper deletes the topic. The
     * orchestrator uses this to close + evict its cached {@link org.apache.pulsar.client.api.Producer}
     * on that topic; otherwise the producer's reconnect loop races with the topic delete and
     * resurrects the topic via auto-create (the SDK's private-output namespace requires
     * {@code allowAutoTopicCreation=true}, so we cannot disable it broker-side).
     */
    private final Consumer<String> beforeTopicDelete;
    /**
     * Invoked by the dead-consumer pass to synthesize an UNREGISTER for a queryId whose
     * heartbeat has gone stale. The callback receives the queryId + its current QueryEntry and is
     * expected to do what {@code handleUnregister} would do (tombstone control topics, decrement
     * activeQueryIds, retract the q: entry). Default no-op so callers that do not need
     * dead-consumer detection (e.g. unit tests focused only on t: GC) are unaffected.
     */
    private final ThrowingBiConsumer<String, QueryEntry> synthesizeUnregister;
    /**
     * Invoked with {@code (metricName, 1.0)} on every relevant sweeper action so the orchestrator
     * can surface counters via {@code Context.recordMetric}. Default no-op for tests that don't
     * care about metrics. The callback runs on the sweeper scheduler thread, not on a
     * {@code process()} invocation thread — see Phase 2 wiring for the deferred-flush rationale.
     */
    private final BiConsumer<String, Double> metrics;

    private ScheduledExecutorService scheduler;

    IdleFilterSweeper(RegistryStore registry, PulsarAdmin admin,
                      OrchestratorConfig config, Clock clock) {
        this(registry, admin, config, clock, t -> {}, (qid, q) -> {}, (n, v) -> {});
    }

    IdleFilterSweeper(RegistryStore registry, PulsarAdmin admin,
                      OrchestratorConfig config, Clock clock,
                      Consumer<String> beforeTopicDelete) {
        this(registry, admin, config, clock, beforeTopicDelete, (qid, q) -> {}, (n, v) -> {});
    }

    IdleFilterSweeper(RegistryStore registry, PulsarAdmin admin,
                      OrchestratorConfig config, Clock clock,
                      Consumer<String> beforeTopicDelete,
                      ThrowingBiConsumer<String, QueryEntry> synthesizeUnregister) {
        this(registry, admin, config, clock, beforeTopicDelete, synthesizeUnregister, (n, v) -> {});
    }

    IdleFilterSweeper(RegistryStore registry, PulsarAdmin admin,
                      OrchestratorConfig config, Clock clock,
                      Consumer<String> beforeTopicDelete,
                      ThrowingBiConsumer<String, QueryEntry> synthesizeUnregister,
                      BiConsumer<String, Double> metrics) {
        this.registry             = registry;
        this.admin                = admin;
        this.config               = config;
        this.clock                = clock;
        this.beforeTopicDelete    = beforeTopicDelete;
        this.synthesizeUnregister = synthesizeUnregister;
        this.metrics              = metrics;
    }

    /** {@link BiConsumer} that can throw checked exceptions (performUnregister can). */
    @FunctionalInterface
    interface ThrowingBiConsumer<A, B> {
        void accept(A a, B b) throws Exception;
    }

    /**
     * Begin running the sweeper on a fixed-delay schedule using {@code idleGcSweepIntervalSeconds}.
     * The first tick runs after one interval, not immediately. Calling start more than once is a
     * programmer error and throws.
     */
    void start() {
        if (scheduler != null) {
            throw new IllegalStateException("IdleFilterSweeper already started");
        }
        long intervalSec = config.idleGcSweepIntervalSeconds();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orchestrator-idle-gc-sweeper");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::sweepQuietly, intervalSec, intervalSec, TimeUnit.SECONDS);
        LOG.info("IdleFilterSweeper started: ttl={}s, interval={}s",
                config.idleGcTtlSeconds(), intervalSec);
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
        LOG.info("IdleFilterSweeper stopped");
    }

    /** Wrapper that catches all throwables so the scheduler doesn't suppress future ticks. */
    private void sweepQuietly() {
        try {
            sweep();
        } catch (Throwable t) {
            LOG.warn("IdleFilterSweeper tick failed; will retry on next interval", t);
        }
    }

    /**
     * Run one sweep tick synchronously and return per-tick stats. Package-private so unit tests
     * can drive ticks deterministically with an injected {@link Clock} rather than waiting on the
     * scheduler.
     */
    SweepStats sweep() {
        SweepStats stats = new SweepStats();
        Instant now = Instant.now(clock);
        Duration ttl = Duration.ofSeconds(config.idleGcTtlSeconds());
        Duration maxStale = Duration.ofSeconds(config.heartbeatMaxStaleSeconds());

        // Pass 0: dead-consumer detection. Walk q: entries; for any whose lastSeen is older than
        // maxStale, synthesize an UNREGISTER. This empties the corresponding t: entries'
        // activeQueryIds, which lets the existing t: GC pass below pick them up after one more
        // idle-gc-ttl window (so total dead-to-deleted = maxStale + ttl + sweep slack).
        List<DeadCandidate> deadCandidates = new ArrayList<>();
        registry.forEachQueryEntry((queryId, entry) -> {
            stats.heartbeatsEvaluated++;
            if (isStale(entry, now, maxStale)) {
                deadCandidates.add(new DeadCandidate(queryId, entry));
            }
        });
        for (DeadCandidate c : deadCandidates) {
            // Recency re-read — a fresh HEARTBEAT may have landed since the snapshot.
            QueryEntry current = registry.getQueryEntry(c.queryId);
            if (!isStale(current, now, maxStale)) {
                LOG.debug("Skipping synthesize-unregister for queryId={}: heartbeat arrived "
                        + "since snapshot", c.queryId);
                stats.heartbeatsSkippedRecent++;
                metrics.accept(M_HEARTBEATS_SKIPPED_RECENT, 1.0);
                continue;
            }
            try {
                synthesizeUnregister.accept(c.queryId, current);
                stats.heartbeatsSynthesized++;
                metrics.accept(M_HEARTBEATS_SYNTHESIZED, 1.0);
                LOG.info("Synthesized UNREGISTER for stale queryId={} (lastSeen={} now={})",
                        c.queryId, current.getLastSeen(), now);
            } catch (Exception e) {
                LOG.warn("synthesizeUnregister failed for queryId={}: {}. Next tick will retry.",
                        c.queryId, e.getMessage());
                stats.heartbeatsErrors++;
                metrics.accept(M_HEARTBEATS_ERRORS, 1.0);
            }
        }

        // First pass: snapshot candidates. Iterating the TableView while issuing deletes against
        // the same view can hide entries from the iterator, so we materialize the candidate keys
        // up front and operate on them in a second pass.
        List<String> candidateKeys = new ArrayList<>();
        registry.forEachInputTopicEntry((key, entry) -> {
            stats.evaluated++;
            metrics.accept(M_GC_CANDIDATES_EVALUATED, 1.0);
            if (isGcEligible(entry, now, ttl)) {
                candidateKeys.add(key);
            }
        });

        // Second pass: re-read each candidate (recency check), then GC.
        for (String key : candidateKeys) {
            RegistryEntry current = registry.getInputTopicEntry(key);
            if (!isGcEligible(current, now, ttl)) {
                LOG.debug("Skipping GC for key=[{}]: state changed since snapshot", key);
                stats.skippedRecentRegister++;
                metrics.accept(M_GC_SKIPPED_RECENT_REGISTER, 1.0);
                continue;
            }

            // Function FIRST — this closes the function's consumer on the control topic, so
            // deleting the topic next won't crash a live TableView producer inside the filter.
            if (!deleteFunctionSafely(current)) {
                stats.errors++;
                metrics.accept(M_GC_ERRORS, 1.0);
                continue;
            }
            // BEFORE deleting the topic, evict the orchestrator's own cached producer on it.
            // If we skip this, the producer's reconnect loop will resurrect the topic via
            // auto-create the moment we kill it.
            try {
                beforeTopicDelete.accept(current.getControlTopic());
            } catch (Exception e) {
                LOG.warn("beforeTopicDelete callback threw for {}: {}; proceeding with topic delete",
                        current.getControlTopic(), e.getMessage());
            }
            if (!deleteControlTopicSafely(current)) {
                stats.errors++;
                metrics.accept(M_GC_ERRORS, 1.0);
                continue;
            }
            try {
                registry.removeInputTopicEntry(key);
            } catch (Exception e) {
                LOG.warn("GC near-complete for key=[{}]: function+topic deleted but t: retract "
                        + "failed: {}. Next sweep will retry.", key, e.getMessage());
                stats.errors++;
                metrics.accept(M_GC_ERRORS, 1.0);
                continue;
            }
            stats.deleted++;
            metrics.accept(M_GC_FUNCTIONS_DELETED, 1.0);
            LOG.info("GCed idle filter function name={} key=[{}] controlTopic={} idleSince={}",
                    current.getFilterFunctionName(), key, current.getControlTopic(),
                    current.getIdleSince());
        }
        return stats;
    }

    /** Whether {@code q} has gone past its heartbeat-staleness threshold. */
    private boolean isStale(QueryEntry q, Instant now, Duration maxStale) {
        if (q == null) return false;
        if (q.getLastSeen() == null) return false;  // pre-heartbeat entries are never stale
        Instant ls;
        try {
            ls = Instant.parse(q.getLastSeen());
        } catch (Exception e) {
            LOG.warn("Unparseable lastSeen=[{}] on QueryEntry; skipping", q.getLastSeen());
            return false;
        }
        return Duration.between(ls, now).compareTo(maxStale) >= 0;
    }

    /** Whether {@code entry} would be GC-eligible at {@code now}. */
    private boolean isGcEligible(RegistryEntry entry, Instant now, Duration ttl) {
        if (entry == null) return false;
        if (entry.getActiveQueryIds() == null) return false;        // pre-GC-awareness; skip
        if (!entry.getActiveQueryIds().isEmpty()) return false;     // in use
        if (entry.getIdleSince() == null) return false;             // invariant violation; defensive
        Instant idle;
        try {
            idle = Instant.parse(entry.getIdleSince());
        } catch (Exception e) {
            LOG.warn("Unparseable idleSince=[{}] on entry name={}; skipping",
                    entry.getIdleSince(), entry.getFilterFunctionName());
            return false;
        }
        return Duration.between(idle, now).compareTo(ttl) >= 0;
    }

    private boolean deleteFunctionSafely(RegistryEntry entry) {
        try {
            admin.functions().deleteFunction(
                    config.functionTenant(),
                    config.functionNamespace(),
                    entry.getFilterFunctionName());
            return true;
        } catch (PulsarAdminException.NotFoundException e) {
            LOG.debug("deleteFunction {} returned NotFound; treating as success",
                    entry.getFilterFunctionName());
            return true;
        } catch (PulsarAdminException e) {
            LOG.warn("deleteFunction {} failed: {}. Will retry on next sweep tick.",
                    entry.getFilterFunctionName(), e.getMessage());
            return false;
        }
    }

    private boolean deleteControlTopicSafely(RegistryEntry entry) {
        try {
            admin.topics().delete(entry.getControlTopic(), true);
            return true;
        } catch (PulsarAdminException.NotFoundException e) {
            LOG.debug("delete topic {} returned NotFound; treating as success",
                    entry.getControlTopic());
            return true;
        } catch (PulsarAdminException e) {
            LOG.warn("delete topic {} failed: {}. Function already gone, will retry on next sweep.",
                    entry.getControlTopic(), e.getMessage());
            return false;
        }
    }

    /** Per-tick counters. Useful for tests to assert sweeper decisions. */
    static final class SweepStats {
        int evaluated;
        int deleted;
        int skippedRecentRegister;
        int errors;
        int heartbeatsEvaluated;
        int heartbeatsSynthesized;
        int heartbeatsSkippedRecent;
        int heartbeatsErrors;

        @Override
        public String toString() {
            return "SweepStats{evaluated=" + evaluated
                    + ", deleted=" + deleted
                    + ", skippedRecentRegister=" + skippedRecentRegister
                    + ", errors=" + errors
                    + ", heartbeatsEvaluated=" + heartbeatsEvaluated
                    + ", heartbeatsSynthesized=" + heartbeatsSynthesized
                    + ", heartbeatsSkippedRecent=" + heartbeatsSkippedRecent
                    + ", heartbeatsErrors=" + heartbeatsErrors
                    + '}';
        }
    }

    private record DeadCandidate(String queryId, QueryEntry entry) {}
}
