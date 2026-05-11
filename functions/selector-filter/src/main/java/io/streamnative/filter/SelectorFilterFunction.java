package io.streamnative.filter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.QueryMessage;
import jakarta.jms.JMSException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class SelectorFilterFunction implements Function<byte[], CompletableFuture<Void>> {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorFilterFunction.class);

    static final String CFG_CONTROL_TOPIC = "control-topic";
    /**
     * userConfig key (boolean string). Default {@code true}. When {@code false} the function
     * never calls {@link System#nanoTime()} or records {@link #METRIC_PROCESSING_TIME_MS} — useful
     * if profiling shows the metric itself is a bottleneck under extreme throughput.
     */
    static final String CFG_PROCESSING_TIME_METRIC = "enable-processing-time-metric";
    static final long LOG_FREQUENCY = 100_000L;

    // Message-count metrics. Per Prometheus convention these are monotonic counters; rates are
    // computed at the monitoring layer (e.g. rate(filter_messages_in[1m])).
    // For every input record: messages_in += 1, and exactly one of (messages_dropped += 1) or
    // (messages_matched += 1) fires. messages_out accumulates total fan-outs (>= matched).
    static final String METRIC_MESSAGES_IN      = "filter.messages.in";
    static final String METRIC_MESSAGES_MATCHED = "filter.messages.matched";
    static final String METRIC_MESSAGES_DROPPED = "filter.messages.dropped";
    static final String METRIC_MESSAGES_OUT     = "filter.messages.out";

    // Byte-count metrics in the same shape as message counts. bytes_out = bytes_in * fanout
    // factor when matched, else 0.
    static final String METRIC_BYTES_IN      = "filter.bytes.in";
    static final String METRIC_BYTES_DROPPED = "filter.bytes.dropped";
    static final String METRIC_BYTES_OUT     = "filter.bytes.out";

    // Query lifecycle counters.
    static final String METRIC_QUERIES_ACTIVE    = "filter.queries.active";
    static final String METRIC_QUERIES_LOADED   = "filter.queries.loaded";
    static final String METRIC_QUERIES_RETRACTED = "filter.queries.retracted";
    static final String METRIC_QUERIES_BAD      = "filter.queries.bad";

    static final String METRIC_SEND_FAILURES = "filter.send.failures";

    /**
     * Per-message wall-clock processing time in milliseconds. Recorded as a sample (double) so
     * the monitoring layer can compute percentiles (p50/p95/p99). Only emitted when
     * {@link #CFG_PROCESSING_TIME_METRIC} is enabled.
     */
    static final String METRIC_PROCESSING_TIME_MS = "filter.processing.time.ms";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ConcurrentMap<String, CompiledQuery> compiled = new ConcurrentHashMap<>();
    private final AtomicLong filteredCount = new AtomicLong();
    private final AtomicLong droppedCount = new AtomicLong();

    private volatile TableView<byte[]> controlTopicView;
    private volatile Context boundContext;
    private volatile boolean processingTimeMetricEnabled = true;

    @Override
    public void initialize(Context ctx) throws Exception {
        String controlTopic = resolveControlTopic(ctx);
        if (controlTopic == null || controlTopic.isBlank()) {
            throw new RuntimeException(
                    "Invalid configuration. '" + CFG_CONTROL_TOPIC + "' must be provided in userConfig.");
        }

        this.boundContext = ctx;
        this.processingTimeMetricEnabled = resolveBoolean(ctx, CFG_PROCESSING_TIME_METRIC, true);
        LOG.info("Processing-time metric ({}) is {}", METRIC_PROCESSING_TIME_MS,
                processingTimeMetricEnabled ? "ENABLED" : "DISABLED");

        PulsarClient pulsarClient = ctx.getPulsarClient();
        this.controlTopicView = pulsarClient.newTableView(Schema.BYTES)
                .topic(controlTopic)
                .create();

        controlTopicView.forEachAndListen(this::onQueryEvent);

        warnOnLoop(ctx);
        recordMetric(METRIC_QUERIES_ACTIVE, compiled.size());
        LOG.info("Initialized SelectorFilterFunction with control topic {} and {} preloaded queries",
                controlTopic, compiled.size());
    }

    @Override
    public CompletableFuture<Void> process(byte[] payload, Context ctx) throws Exception {
        this.boundContext = ctx;

        // Cheap branch: if the metric is disabled, no nanoTime call at all.
        final long startNanos = processingTimeMetricEnabled ? System.nanoTime() : 0L;

        int payloadBytes = payload == null ? 0 : payload.length;
        ctx.recordMetric(METRIC_MESSAGES_IN, 1.0);
        ctx.recordMetric(METRIC_BYTES_IN, payloadBytes);

        Record<?> record = ctx.getCurrentRecord();

        if (compiled.isEmpty()) {
            return ackAsDropped(ctx, record, payloadBytes, startNanos);
        }

        MessageBindings bindings = MessageBindings.from(record);

        List<CompletableFuture<?>> sends = new ArrayList<>();
        Map<String, String> forwardProperties = null;
        boolean forwardPropertiesResolved = false;

        for (CompiledQuery cq : compiled.values()) {
            if (cq.evaluator().matches(bindings)) {
                TypedMessageBuilder<byte[]> builder = ctx.newOutputMessage(cq.outputTopic(), Schema.BYTES)
                        .value(payload);
                if (!forwardPropertiesResolved) {
                    // First match: resolve once and reuse for any subsequent matches.
                    forwardProperties = record.getProperties();
                    forwardPropertiesResolved = true;
                }
                if (forwardProperties != null && !forwardProperties.isEmpty()) {
                    builder = builder.properties(forwardProperties);
                }
                String recordKey = bindings.getKey();
                if (recordKey != null) {
                    builder = builder.key(recordKey);
                }
                sends.add(builder.sendAsync());
            }
        }

        if (sends.isEmpty()) {
            return ackAsDropped(ctx, record, payloadBytes, startNanos);
        }

        final int fanout = sends.size();
        return CompletableFuture.allOf(sends.toArray(new CompletableFuture[0]))
                .whenComplete((v, err) -> {
                    if (err != null) {
                        ctx.recordMetric(METRIC_SEND_FAILURES, 1.0);
                        LOG.warn("Send failed for one or more output topics; failing record", err);
                        record.fail();
                    } else {
                        record.ack();
                        ctx.recordMetric(METRIC_MESSAGES_MATCHED, 1.0);
                        ctx.recordMetric(METRIC_MESSAGES_OUT, fanout);
                        ctx.recordMetric(METRIC_BYTES_OUT, (double) payloadBytes * fanout);
                        long c = filteredCount.addAndGet(fanout);
                        if (c % LOG_FREQUENCY < fanout) {
                            LOG.info("Filtered {} messages (dropped={}, active queries={})",
                                    c, droppedCount.get(), compiled.size());
                        }
                    }
                    recordProcessingTime(ctx, startNanos);
                })
                .thenApply(v -> null);
    }

    private CompletableFuture<Void> ackAsDropped(Context ctx, Record<?> record, int payloadBytes, long startNanos) {
        droppedCount.incrementAndGet();
        ctx.recordMetric(METRIC_MESSAGES_DROPPED, 1.0);
        ctx.recordMetric(METRIC_BYTES_DROPPED, payloadBytes);
        record.ack();
        recordProcessingTime(ctx, startNanos);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Records the elapsed wall-clock time since {@code startNanos} as a sample on
     * {@link #METRIC_PROCESSING_TIME_MS}. No-op when the metric is disabled. Called from both the
     * matched-fanout completion path (async, on the broker IO thread) and the dropped/empty path
     * (sync, on the broker thread that invoked process()).
     */
    private void recordProcessingTime(Context ctx, long startNanos) {
        if (!processingTimeMetricEnabled) {
            return;
        }
        double elapsedMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        ctx.recordMetric(METRIC_PROCESSING_TIME_MS, elapsedMs);
    }

    private static boolean resolveBoolean(Context ctx, String key, boolean defaultValue) {
        Object value = ctx.getUserConfigValue(key).orElse(null);
        if (value == null) {
            return defaultValue;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private String resolveControlTopic(Context ctx) {
        Object value = ctx.getUserConfigValue(CFG_CONTROL_TOPIC).orElse(null);
        return value == null ? null : value.toString();
    }

    void onQueryEvent(String queryId, byte[] value) {
        if (queryId == null) {
            return;
        }
        if (value == null || value.length == 0) {
            CompiledQuery removed = compiled.remove(queryId);
            if (removed != null) {
                LOG.info("Retracted query id={} name='{}'", queryId, removed.name());
                recordMetric(METRIC_QUERIES_RETRACTED, 1.0);
                recordMetric(METRIC_QUERIES_ACTIVE, compiled.size());
            }
            return;
        }
        QueryMessage qm;
        try {
            qm = MAPPER.readValue(value, QueryMessage.class);
        } catch (Exception e) {
            LOG.warn("Failed to parse QueryMessage for id={}: {}", queryId, e.toString());
            recordMetric(METRIC_QUERIES_BAD, 1.0);
            return;
        }
        if (qm.getSelector() == null || qm.getSelector().isBlank()) {
            LOG.warn("Query id={} has empty selector; skipping", queryId);
            recordMetric(METRIC_QUERIES_BAD, 1.0);
            return;
        }
        if (qm.getOutputTopic() == null || qm.getOutputTopic().isBlank()) {
            LOG.warn("Query id={} has empty outputTopic; skipping", queryId);
            recordMetric(METRIC_QUERIES_BAD, 1.0);
            return;
        }
        SelectorEvaluator evaluator;
        try {
            evaluator = SelectorEvaluator.compile(qm.getSelector());
        } catch (JMSException e) {
            LOG.warn("Query id={} has invalid selector [{}]: {}", queryId, qm.getSelector(), e.getMessage());
            recordMetric(METRIC_QUERIES_BAD, 1.0);
            return;
        }
        compiled.put(queryId, new CompiledQuery(queryId, qm.getName(), qm.getOutputTopic(), evaluator));
        LOG.info("Loaded query id={} name='{}' selector=[{}] -> {}",
                queryId, qm.getName(), qm.getSelector(), qm.getOutputTopic());
        recordMetric(METRIC_QUERIES_LOADED, 1.0);
        recordMetric(METRIC_QUERIES_ACTIVE, compiled.size());
    }

    private void recordMetric(String name, double value) {
        Context ctx = boundContext;
        if (ctx == null) {
            return;
        }
        try {
            ctx.recordMetric(name, value);
        } catch (Throwable t) {
            // Some runtime variants may not allow recordMetric outside process(); ignore.
        }
    }

    private void warnOnLoop(Context ctx) {
        Collection<String> inputs = safeInputs(ctx);
        if (inputs == null || inputs.isEmpty()) {
            return;
        }
        Set<String> outputs = new HashSet<>();
        for (CompiledQuery cq : compiled.values()) {
            outputs.add(cq.outputTopic());
        }
        for (String input : inputs) {
            if (outputs.contains(input)) {
                LOG.warn("Configured output topic {} is also an input topic; this WILL create a loop. "
                        + "Function will continue but operators are responsible for breaking the cycle.", input);
            }
        }
    }

    private static Collection<String> safeInputs(Context ctx) {
        try {
            return ctx.getInputTopics();
        } catch (Throwable t) {
            return null;
        }
    }

    /**
     * Test-only hook: load a query without going through the TableView. Mirrors what the
     * listener does on a normal control-topic message.
     */
    void loadQueryForTest(String queryId, byte[] payload) {
        onQueryEvent(queryId, payload);
    }

    /** Test-only hook: number of currently loaded queries. */
    int activeQueryCountForTest() {
        return compiled.size();
    }

    @Override
    public void close() throws Exception {
        if (controlTopicView != null) {
            controlTopicView.close();
        }
        compiled.clear();
    }
}
