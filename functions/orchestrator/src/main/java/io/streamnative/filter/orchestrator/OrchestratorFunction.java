package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.Command;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.QueryMessage;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    /** Per-per-function-control-topic producer cache. Lazy. */
    private ConcurrentMap<String, Producer<byte[]>> controlProducers;
    private Producer<byte[]> stateProducer;
    private TableView<byte[]> stateView;

    /** Constructor for Pulsar runtime. */
    public OrchestratorFunction() {}

    /** Test seam — inject mocks for state plumbing and spawner. */
    OrchestratorFunction(OrchestratorConfig config, RegistryStore registry,
                         FilterFunctionSpawner spawner, PulsarClient pulsarClient,
                         ConcurrentMap<String, Producer<byte[]>> controlProducers) {
        this.config           = config;
        this.registry         = registry;
        this.spawner          = spawner;
        this.pulsarClient     = pulsarClient;
        this.controlProducers = controlProducers;
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

        // Phase 4 will replace this with a real spawner backed by ctx.getPulsarAdmin().
        // For Phase 3 the orchestrator's routing logic is exercisable via the test seam
        // constructor; the production wire-up is a placeholder until Phase 4.
        this.spawner = (registryKey, req, cfg) -> {
            throw new UnsupportedOperationException(
                    "Real FilterFunctionSpawner not yet wired (Phase 4). Phase-3 production "
                            + "deployment cannot spawn filter functions; tests inject a mock spawner.");
        };

        LOG.info("OrchestratorFunction initialized. stateTopic={}, packageUrl={}, "
                        + "fnTenant={}, fnNamespace={}, ctrlTopicTenant={}, ctrlTopicNs={}, "
                        + "parallelism={}, applyCompaction={}",
                config.stateTopic(), config.functionPackageUrl(),
                config.functionTenant(), config.functionNamespace(),
                config.controlTopicTenant(), config.controlTopicNamespace(),
                config.functionParallelism(), config.applyCompaction());
    }

    @Override
    public Void process(byte[] payload, Context ctx) throws Exception {
        ctx.recordMetric("orchestrator.requests.received", 1);

        if (payload == null || payload.length == 0) {
            // The SDK never publishes an empty body on the orchestrator's input topic — every
            // intent is expressed as a typed FilterRequest. Ignore stray nulls gracefully.
            LOG.debug("Ignoring null/empty payload on orchestrator input");
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null;
        }

        String queryId = ctx.getCurrentRecord().getKey().orElse(null);
        if (queryId == null || queryId.isBlank()) {
            LOG.warn("FilterRequest missing message key (queryId); rejecting");
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null;
        }

        FilterRequest req;
        try {
            req = MAPPER.readValue(payload, FilterRequest.class);
            req.validate();
        } catch (IllegalArgumentException | JsonProcessingException e) {
            LOG.warn("Malformed FilterRequest queryId={}: {}", queryId, e.getMessage());
            ctx.recordMetric("orchestrator.requests.rejected_validation", 1);
            return null; // permanent reject — ack
        }

        switch (req.getCommand()) {
            case REGISTER   -> handleRegister(queryId, req, ctx);
            case MODIFY     -> handleModify(queryId, req, ctx);
            case UNREGISTER -> handleUnregister(queryId, ctx);
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
        // per-function control topics this queryId is being published to.
        List<String> publishedControlTopics = new ArrayList<>(registryKeys.size());
        for (String key : registryKeys) {
            RegistryEntry existing = registry.getInputTopicEntry(key);
            RegistryEntry entry;
            if (existing == null) {
                entry = spawner.spawn(key, req, config);
                registry.putInputTopicEntry(key, entry);
                ctx.recordMetric("orchestrator.functions.spawned", 1);
                LOG.info("Spawned filter function for registry key=[{}] name={} controlTopic={}",
                        key, entry.getFilterFunctionName(), entry.getControlTopic());
            } else {
                ctx.recordMetric("orchestrator.functions.reused", 1);
                if (existing.check(req).needsUpgrade()) {
                    RegistryEntry merged = existing.mergedWith(req);
                    registry.putInputTopicEntry(key, merged);
                    entry = merged;
                    LOG.info("Upgraded registry entry for key=[{}] with new serializers", key);
                } else {
                    entry = existing;
                }
            }
            publishQuery(entry.getControlTopic(), queryId, normalizedSelector(req.getSelector()),
                    req.getOutputTopic());
            publishedControlTopics.add(entry.getControlTopic());
        }

        registry.putQueryEntry(queryId, new QueryEntry(req.getOutputTopic(), publishedControlTopics));
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
        ctx.recordMetric("orchestrator.requests.accepted", 1);
        LOG.info("MODIFY queryId={} selector=[{}] fanout={}",
                queryId, selector, q.getControlTopics().size());
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
        // Tombstone the queryId on every per-function control topic it was published to.
        for (String controlTopic : q.getControlTopics()) {
            Producer<byte[]> p = controlProducerFor(controlTopic);
            p.newMessage().key(queryId).value(null).send();
        }
        registry.removeQueryEntry(queryId);
        ctx.recordMetric("orchestrator.tombstones.fanout", q.getControlTopics().size());
        ctx.recordMetric("orchestrator.requests.accepted", 1);
        LOG.info("UNREGISTER queryId={} tombstoned on {} control topic(s)",
                queryId, q.getControlTopics().size());
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

    private static String normalizedSelector(String userSelector) {
        return (userSelector == null || userSelector.isBlank()) ? IDENTITY_SELECTOR : userSelector;
    }

    @Override
    public void close() throws Exception {
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
    }
}
