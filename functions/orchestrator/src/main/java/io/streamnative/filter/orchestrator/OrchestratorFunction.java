package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.Command;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.QueryMessage;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Orchestrator Pulsar Function. Consumes SDK {@link FilterRequest}s, maintains a per-input-topic
 * registry of filter function instances (TableView-backed on a configured state topic), spawns new
 * filter functions on demand via a {@link FilterFunctionSpawner}, and routes
 * {@link QueryMessage}s to the appropriate per-function control topic.
 *
 * <p>Wire contract: see {@code protocol/filter-request.schema.json}. The orchestrator's input
 * topic (the SDK's {@code controlTopic}) is a regular function input, not a TableView; the
 * orchestrator's own state topic IS a TableView, distinct from per-filter-function control topics
 * (also TableViews, read by the spawned filter functions).
 *
 * <p>Parallelism MUST be 1 in v1 — multiple instances would race on spawn decisions for the same
 * registry key. See plan R2 for the leader-election / KeyShared sharding path forward.
 */
public class OrchestratorFunction implements Function<byte[], Void> {

    private static final Logger LOG = LoggerFactory.getLogger(OrchestratorFunction.class);

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** JMS-selector identity used downstream when the SDK omits a user selector. */
    static final String IDENTITY_SELECTOR = "1=1";

    // ---- runtime state (assigned in initialize) ----
    private OrchestratorConfig config;
    private RegistryStore registry;
    private FilterFunctionSpawner spawner;
    private PulsarClient pulsarClient;
    private PulsarAdmin ownedAdmin;
    /** Per-per-function-control-topic producer cache. Lazy. */
    private ConcurrentMap<String, Producer<byte[]>> controlProducers;
    private Producer<byte[]> stateProducer;
    private TableView<byte[]> stateView;
    /** Source of {@link Instant}s for GC bookkeeping (idleSince). Overridable for tests. */
    private Clock clock = Clock.systemUTC();
    /** Background sweeper. {@code null} when {@code idle-gc-enabled = false}. */
    private IdleFilterSweeper idleFilterSweeper;
    /**
     * Sweeper-emitted counters waiting to be flushed into the next process()-time {@link Context}.
     * Calls to {@code Context.recordMetric} from threads other than the function's process loop
     * are silently dropped by Pulsar's function-instance stats endpoint — verified empirically by
     * the Phase 4 live IT before this fallback existed (all sweeper counters stayed at 0.0). So
     * the sweeper scheduler thread now writes into this map, and the next process() invocation
     * drains the accumulated counters into {@code ctx.recordMetric} on the correct thread.
     *
     * <p>Cost: one ConcurrentHashMap merge per sweeper-stat increment + one drain per process()
     * call. Bounded size — at most seven keys (the canonical sweeper metric names from
     * {@link IdleFilterSweeper}). Values are doubles summed across sweep ticks since the last
     * flush.
     */
    private final ConcurrentMap<String, Double> pendingSweeperMetrics = new ConcurrentHashMap<>();

    /** Constructor for Pulsar runtime. */
    public OrchestratorFunction() {}

    /** Test seam — inject mocks for state plumbing and spawner. */
    OrchestratorFunction(OrchestratorConfig config, RegistryStore registry,
                         FilterFunctionSpawner spawner, PulsarClient pulsarClient,
                         ConcurrentMap<String, Producer<byte[]>> controlProducers) {
        this(config, registry, spawner, pulsarClient, controlProducers, Clock.systemUTC());
    }

    /** Test seam with a fixed clock — drives the idleSince timestamp on UNREGISTER. */
    OrchestratorFunction(OrchestratorConfig config, RegistryStore registry,
                         FilterFunctionSpawner spawner, PulsarClient pulsarClient,
                         ConcurrentMap<String, Producer<byte[]>> controlProducers,
                         Clock clock) {
        this.config           = config;
        this.registry         = registry;
        this.spawner          = spawner;
        this.pulsarClient     = pulsarClient;
        this.controlProducers = controlProducers;
        this.clock            = clock;
    }

    @Override
    public void initialize(Context ctx) throws Exception {
        this.config           = OrchestratorConfig.fromContext(ctx);
        this.pulsarClient     = ctx.getPulsarClient();
        this.controlProducers = new ConcurrentHashMap<>();

        this.stateProducer = pulsarClient.newProducer(Schema.BYTES)
                .topic(config.stateTopic())
                .producerName("orchestrator-state-" + UUID.randomUUID())
                .enableBatching(false)
                .create();

        this.stateView = pulsarClient.newTableView(Schema.BYTES)
                .topic(config.stateTopic())
                .create();

        this.registry = new RegistryStore(stateView, stateProducer);

        // Construct our own PulsarAdmin from userConfig — Context.getPulsarAdmin() is only
        // available when the function worker is configured with pulsarFunctionsCluster, which
        // lightweight (and many production) deployments don't enable by default.
        this.ownedAdmin = PulsarAdmin.builder()
                .serviceHttpUrl(config.pulsarAdminUrl())
                .build();
        this.spawner = new DefaultFilterFunctionSpawner(ownedAdmin);

        if (config.idleGcEnabled()) {
            this.idleFilterSweeper = new IdleFilterSweeper(registry, ownedAdmin, config, clock,
                    this::evictControlProducer, this::performUnregister, this::recordSweeperMetric);
            this.idleFilterSweeper.start();
        }

        LOG.info("OrchestratorFunction initialized. stateTopic={}, packageUrl={}, "
                        + "fnTenant={}, fnNamespace={}, ctrlTopicTenant={}, ctrlTopicNs={}, "
                        + "parallelism={}, applyCompaction={}, idleGcEnabled={}, "
                        + "idleGcTtlSeconds={}, idleGcSweepIntervalSeconds={}",
                config.stateTopic(), config.functionPackageUrl(),
                config.functionTenant(), config.functionNamespace(),
                config.controlTopicTenant(), config.controlTopicNamespace(),
                config.functionParallelism(), config.applyCompaction(),
                config.idleGcEnabled(), config.idleGcTtlSeconds(),
                config.idleGcSweepIntervalSeconds());
    }

    @Override
    public Void process(byte[] payload, Context ctx) throws Exception {
        flushPendingSweeperMetrics(ctx);
        ctx.recordMetric("orchestrator.requests.received", 1);

        if (payload == null || payload.length == 0) {
            // The SDK never publishes an empty body on the orchestrator's input topic — every
            // intent is expressed as a typed FilterRequest. Ignore stray nulls gracefully.
            LOG.debug("Ignoring null/empty payload on orchestrator input");
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null;
        }

        String queryId = ctx.getCurrentRecord().getKey().orElse(null);

        FilterRequest req;
        try {
            req = MAPPER.readValue(payload, FilterRequest.class);
            req.validate();
        } catch (IllegalArgumentException | JsonProcessingException e) {
            LOG.warn("Malformed FilterRequest queryId={}: {}", queryId, e.getMessage());
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null; // permanent reject — ack
        }

        // HEARTBEAT carries the queryId list in the body; the message key is arbitrary. Every
        // other command keys the message by the single queryId it operates on.
        if (req.getCommand() != Command.HEARTBEAT && (queryId == null || queryId.isBlank())) {
            LOG.warn("FilterRequest missing message key (queryId); rejecting");
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null;
        }

        switch (req.getCommand()) {
            case REGISTER   -> handleRegister(queryId, req, ctx);
            case MODIFY     -> handleModify(queryId, req, ctx);
            case UNREGISTER -> handleUnregister(queryId, ctx);
            case HEARTBEAT  -> handleHeartbeat(req, ctx);
        }
        return null;
    }

    // ---------------------------------------------------------------- REGISTER

    private void handleRegister(String queryId, FilterRequest req, Context ctx) throws Exception {
        // Redelivery / duplicate check on queryId.
        QueryEntry existingQuery = registry.getQueryEntry(queryId);
        if (existingQuery != null) {
            if (existingQuery.getOutputTopic().equals(req.getOutputTopic())) {
                LOG.info("REGISTER queryId={} is a redelivery (same fields); idempotent success", queryId);
                ctx.recordMetric("orchestrator.requests.accepted", 1);
                return;
            }
            LOG.warn("REGISTER queryId={} already exists with different fields; rejecting "
                    + "(SDK should use MODIFY)", queryId);
            ctx.recordMetric("orchestrator.requests.rejected_duplicate", 1);
            return;
        }

        // Derive registry keys: one per input topic, or the pattern itself.
        List<String> registryKeys = registryKeysFor(req);

        // Dry-run pass: schema compatibility against any existing registry entries. All-or-nothing
        // — we do not partially commit a multi-input REGISTER.
        for (String key : registryKeys) {
            RegistryEntry existing = registry.getInputTopicEntry(key);
            if (existing != null) {
                RegistryEntry.CompatibilityResult compat = existing.check(req);
                if (compat.isRejected()) {
                    LOG.warn("REGISTER queryId={} rejected: registry key=[{}] {}",
                            queryId, key, compat.reason());
                    ctx.recordMetric("orchestrator.requests.rejected_schema", 1);
                    return;
                }
            }
        }

        // Commit pass: ensure each input topic has a registered filter function. Track the
        // per-function control topics this queryId is being published to, in parallel with the
        // registry keys (used by UNREGISTER to do O(1) lookups into the t: entries for GC
        // bookkeeping).
        List<String> publishedControlTopics = new ArrayList<>(registryKeys.size());
        List<String> publishedRegistryKeys  = new ArrayList<>(registryKeys.size());
        for (String key : registryKeys) {
            RegistryEntry existing = registry.getInputTopicEntry(key);
            RegistryEntry entry;
            if (existing == null) {
                entry = spawner.spawn(key, req, config);
                ctx.recordMetric("orchestrator.functions.spawned", 1);
                LOG.info("Spawned filter function for registry key=[{}] name={} controlTopic={}",
                        key, entry.getFilterFunctionName(), entry.getControlTopic());
            } else {
                ctx.recordMetric("orchestrator.functions.reused", 1);
                if (existing.check(req).needsUpgrade()) {
                    entry = existing.mergedWith(req);
                    LOG.info("Upgraded registry entry for key=[{}] with new serializers", key);
                } else {
                    entry = existing;
                }
            }
            // Track this queryId on the entry so idle-GC can decide later when ALL queryIds have
            // UNREGISTERed. Set semantics: re-REGISTER of the same queryId is a no-op.
            entry = entry.withQueryId(queryId);
            registry.putInputTopicEntry(key, entry);

            publishQuery(entry.getControlTopic(), queryId, normalizedSelector(req.getSelector()),
                    req.getOutputTopic());
            publishedControlTopics.add(entry.getControlTopic());
            publishedRegistryKeys.add(key);
        }

        registry.putQueryEntry(queryId, new QueryEntry(
                req.getOutputTopic(), publishedControlTopics, publishedRegistryKeys,
                Instant.now(clock).toString()));
        ctx.recordMetric("orchestrator.requests.accepted", 1);
        LOG.info("REGISTER queryId={} outputTopic={} controlTopics={}",
                queryId, req.getOutputTopic(), publishedControlTopics);
    }

    // ---------------------------------------------------------------- MODIFY

    private void handleModify(String queryId, FilterRequest req, Context ctx) throws Exception {
        QueryEntry q = registry.getQueryEntry(queryId);
        if (q == null) {
            LOG.warn("MODIFY queryId={} not found in registry; ignoring (orchestrator state may "
                    + "be in flux or the SDK is misbehaving)", queryId);
            ctx.recordMetric("orchestrator.requests.rejected_unknown", 1);
            return;
        }
        String selector = normalizedSelector(req.getSelector());
        for (String controlTopic : q.getControlTopics()) {
            publishQuery(controlTopic, queryId, selector, q.getOutputTopic());
        }
        // Touch lastSeen — a MODIFY counts as liveness signal just like a heartbeat.
        registry.putQueryEntry(queryId, q.withLastSeen(Instant.now(clock)));
        ctx.recordMetric("orchestrator.requests.accepted", 1);
        LOG.info("MODIFY queryId={} selector=[{}] fanout={}",
                queryId, selector, q.getControlTopics().size());
    }

    // ---------------------------------------------------------------- UNREGISTER

    // ---------------------------------------------------------------- HEARTBEAT

    private void handleHeartbeat(FilterRequest req, Context ctx) throws Exception {
        ctx.recordMetric("orchestrator.heartbeats.received", 1);
        Instant now = Instant.now(clock);
        int touched = 0;
        int unknown = 0;
        for (String queryId : req.getQueryIds()) {
            QueryEntry q = registry.getQueryEntry(queryId);
            if (q == null) {
                // Already tombstoned (probably) — the SDK will drop this queryId from its live
                // set on the next heartbeat tick. Silent no-op.
                unknown++;
                continue;
            }
            registry.putQueryEntry(queryId, q.withLastSeen(now));
            touched++;
        }
        ctx.recordMetric("orchestrator.heartbeats.touched", touched);
        ctx.recordMetric("orchestrator.heartbeats.unknown", unknown);
        LOG.debug("HEARTBEAT touched={} unknown={} (total={})",
                touched, unknown, req.getQueryIds().size());
    }

    // ---------------------------------------------------------------- UNREGISTER

    private void handleUnregister(String queryId, Context ctx) throws Exception {
        QueryEntry q = registry.getQueryEntry(queryId);
        if (q == null) {
            // Idempotent — UNREGISTER on unknown queryId is silent success per locked design.
            LOG.debug("UNREGISTER queryId={} not found in registry; idempotent success", queryId);
            ctx.recordMetric("orchestrator.unregisters.unknown", 1);
            ctx.recordMetric("orchestrator.requests.accepted", 1);
            return;
        }
        performUnregister(queryId, q);
        ctx.recordMetric("orchestrator.tombstones.fanout", q.getControlTopics().size());
        ctx.recordMetric("orchestrator.requests.accepted", 1);
        LOG.info("UNREGISTER queryId={} tombstoned on {} control topic(s)",
                queryId, q.getControlTopics().size());
    }

    /**
     * Core UNREGISTER work, factored out so the {@link IdleFilterSweeper} can synthesize an
     * UNREGISTER on dead-consumer detection without re-entering the message-processing path.
     * Tombstones the queryId on each control topic, decrements {@code activeQueryIds} on each
     * referenced {@code t:} entry, and retracts the {@code q:} entry. Idempotent — a repeated call
     * with the same queryId is a no-op because the q: entry has been removed.
     */
    void performUnregister(String queryId, QueryEntry q) throws Exception {
        // Tombstone the queryId on every per-function control topic it was published to.
        for (String controlTopic : q.getControlTopics()) {
            Producer<byte[]> p = controlProducerFor(controlTopic);
            p.newMessage().key(queryId).value(null).send();
        }

        // Decrement activeQueryIds on the corresponding t: entries so idle-GC can decide later.
        // Legacy QueryEntries (registryKeys == null) skip this — those filters won't be
        // GC-eligible, but that's strictly better than today's no-GC behavior.
        if (q.getRegistryKeys() != null) {
            Instant now = Instant.now(clock);
            for (String key : q.getRegistryKeys()) {
                RegistryEntry t = registry.getInputTopicEntry(key);
                if (t == null) continue;
                RegistryEntry updated = t.withoutQueryId(queryId, now);
                if (updated != t) {
                    registry.putInputTopicEntry(key, updated);
                }
            }
        } else {
            LOG.debug("performUnregister queryId={} has legacy QueryEntry without registryKeys; "
                    + "activeQueryIds bookkeeping skipped (filter will not be GC-eligible)",
                    queryId);
        }

        registry.removeQueryEntry(queryId);
    }

    // ---------------------------------------------------------------- helpers

    /** Derive the list of registry keys covered by a REGISTER FilterRequest. */
    private static List<String> registryKeysFor(FilterRequest req) {
        if (req.getTopicsPattern() != null && !req.getTopicsPattern().isBlank()) {
            return Collections.singletonList(req.getTopicsPattern());
        }
        return List.copyOf(req.getInputTopics());
    }

    /**
     * Publish a {@link QueryMessage} to a per-function control topic. The filter function reads
     * this topic via TableView and adds/updates the compiled selector for the queryId key.
     */
    private void publishQuery(String controlTopic, String queryId, String selector, String outputTopic)
            throws PulsarClientException {
        QueryMessage qm = new QueryMessage("session-" + queryId, selector, outputTopic);
        byte[] payload;
        try {
            payload = MAPPER.writeValueAsBytes(qm);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize QueryMessage", e);
        }
        Producer<byte[]> p = controlProducerFor(controlTopic);
        p.newMessage().key(queryId).value(payload).send();
    }

    private Producer<byte[]> controlProducerFor(String controlTopic) {
        return controlProducers.computeIfAbsent(controlTopic, t -> {
            try {
                return pulsarClient.newProducer(Schema.BYTES)
                        .topic(t)
                        .producerName("orchestrator-ctrl-" + UUID.randomUUID())
                        .enableBatching(false)
                        .create();
            } catch (PulsarClientException e) {
                throw new IllegalStateException("Failed to create producer for control topic " + t, e);
            }
        });
    }

    /**
     * Called by {@link IdleFilterSweeper} just before it deletes a control topic. We must close
     * and remove our cached producer so its reconnect loop does not resurrect the topic via
     * namespace auto-create the moment the delete lands.
     */
    private void evictControlProducer(String controlTopic) {
        Producer<byte[]> p = controlProducers.remove(controlTopic);
        if (p != null) {
            try { p.close(); } catch (Exception e) {
                LOG.warn("Close control producer for {} during GC: {}", controlTopic, e.getMessage());
            }
        }
    }

    /**
     * Sweeper-side metric callback. Invoked from the sweeper's scheduler thread; we accumulate
     * into {@link #pendingSweeperMetrics} and let the next process() invocation flush on the
     * function's process loop thread. Calling {@code ctx.recordMetric} directly from this thread
     * is silently dropped by Pulsar's stats endpoint (R1, verified by Phase 4 IT).
     *
     * <p>Package-private so unit tests can exercise the callback's contract directly.
     */
    void recordSweeperMetric(String name, Double value) {
        pendingSweeperMetrics.merge(name, value, Double::sum);
    }

    /**
     * Drain {@link #pendingSweeperMetrics} into {@code ctx.recordMetric}. Called at the top of
     * every process() — recording on the function's process loop thread is the only thread
     * Pulsar's stats endpoint accepts updates from. Drained values are removed atomically so a
     * concurrent sweeper increment after the drain accumulates afresh and flushes on the next
     * process() call.
     *
     * <p>Defensive: any exception from a single {@code recordMetric} call is logged and the
     * drain continues — losing one counter must not corrupt the others.
     */
    private void flushPendingSweeperMetrics(Context ctx) {
        if (pendingSweeperMetrics.isEmpty()) return;
        for (String name : pendingSweeperMetrics.keySet()) {
            Double value = pendingSweeperMetrics.remove(name);
            if (value == null || value == 0.0) continue;
            try {
                ctx.recordMetric(name, value);
            } catch (Exception e) {
                LOG.debug("recordMetric({}, {}) during sweeper-metric flush threw: {}",
                        name, value, e.getMessage());
            }
        }
    }

    private static String normalizedSelector(String userSelector) {
        return (userSelector == null || userSelector.isBlank()) ? IDENTITY_SELECTOR : userSelector;
    }

    @Override
    public void close() throws Exception {
        // Close the sweeper FIRST so it does not race with us tearing down the admin/registry
        // beneath it.
        if (idleFilterSweeper != null) {
            try { idleFilterSweeper.close(); } catch (Exception e) { LOG.warn("Close sweeper", e); }
        }
        if (controlProducers != null) {
            for (Producer<byte[]> p : controlProducers.values()) {
                try { p.close(); } catch (Exception e) { LOG.warn("Close control producer", e); }
            }
        }
        if (stateProducer != null) {
            try { stateProducer.close(); } catch (Exception e) { LOG.warn("Close state producer", e); }
        }
        if (stateView != null) {
            try { stateView.close(); } catch (Exception e) { LOG.warn("Close state view", e); }
        }
        if (ownedAdmin != null) {
            try { ownedAdmin.close(); } catch (Exception e) { LOG.warn("Close pulsar admin", e); }
        }
    }
}
