package io.streamnative.filter;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.QueryMessage;
import javax.jms.JMSException;
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
import java.util.concurrent.atomic.AtomicInteger;

public class SelectorFilterFunction implements Function<byte[], CompletableFuture<Void>> {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorFilterFunction.class);

    static final String CFG_CONTROL_TOPIC = "control-topic";
    /**
     * userConfig key (boolean string). Default {@code true}. When {@code false} the function
     * never calls {@link System#nanoTime()} or records {@link #METRIC_PROCESSING_TIME_MS} — useful
     * if profiling shows the metric itself is a bottleneck under extreme throughput.
     */
    static final String CFG_PROCESSING_TIME_METRIC = "enable-processing-time-metric";
    /**
     * userConfig key (int). When &gt; 0, log a per-message phase-breakdown {@code [timing]} line every
     * N matched messages: queue (and its producer/ingest split when broker-entry-metadata is enabled),
     * eval time, and sendAsync round-trip. Default {@code 0} (off) — a diagnostic, threaded on per
     * deployment via the orchestrator's {@code filter-log-timing-every} knob; left off in steady state
     * to avoid per-message log volume.
     */
    static final String CFG_LOG_TIMING_EVERY = "log-timing-every";
    static final long LOG_FREQUENCY = 100_000L;
    /** Warn when a single sendAsync() takes longer than this. */
    static final long SLOW_SEND_WARN_MS  = 5_000L;
    /** Log an ERROR (not just a WARN) for extremely stalled sends. */
    static final long SLOW_SEND_ERROR_MS = 30_000L;
    /** Heartbeat log interval — shows function health even when messages aren't flowing. */
    static final long HEARTBEAT_INTERVAL_MS = 30_000L;

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

    /**
     * Synchronous span of {@code process()} for a MATCHED record: entry → all {@code sendAsync()}
     * calls dispatched (deserialize + selector evaluation + message build + producer enqueue), in
     * milliseconds. This is the function's own CPU "value-add" latency, excluding the output-send
     * round-trip. Matched-only (dropped records emit no output). Recorded as a sample so the
     * monitoring layer computes percentiles. Gated by {@link #CFG_PROCESSING_TIME_METRIC}.
     */
    static final String METRIC_DISPATCH_TIME_MS = "filter.dispatch.time.ms";

    /**
     * Full span of {@code process()} for a MATCHED record: entry → all output sends acked — i.e.
     * {@link #METRIC_DISPATCH_TIME_MS} plus the output-send round-trip, in milliseconds.
     * Matched-only, distinct from {@link #METRIC_PROCESSING_TIME_MS} which also samples the dropped
     * path. {@code match_processing − dispatch} isolates the output-send component, so a baseline
     * can tell whether value-add latency is code-bound (dispatch) or send-bound. Gated by
     * {@link #CFG_PROCESSING_TIME_METRIC}.
     */
    static final String METRIC_MATCH_PROCESSING_TIME_MS = "filter.match.processing.time.ms";

    // Latency metrics: running sums + counts so the observability can compute
    // avg latency as delta(sum)/delta(count) over any polling window.
    // "publish" = Pulsar broker received the source message (broker-side filter latency).
    // "event"   = original Kafka/producer timestamp on the message (true E2E including bridge).
    static final String METRIC_LATENCY_PUBLISH_MS_SUM   = "filter.latency.publish_ms_sum";
    static final String METRIC_LATENCY_PUBLISH_MS_COUNT = "filter.latency.publish_ms_count";
    static final String METRIC_LATENCY_EVENT_MS_SUM     = "filter.latency.event_ms_sum";
    static final String METRIC_LATENCY_EVENT_MS_COUNT   = "filter.latency.event_ms_count";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ConcurrentMap<String, CompiledQuery> compiled = new ConcurrentHashMap<>();
    private final AtomicLong filteredCount   = new AtomicLong();
    private final AtomicLong droppedCount    = new AtomicLong();
    private final AtomicLong messagesIn      = new AtomicLong();
    /** Number of sendAsync() calls currently in-flight (not yet completed). */
    private final AtomicInteger pendingSends = new AtomicInteger();
    /** Epoch-ms of the last heartbeat log emission. */
    private final AtomicLong lastHeartbeatMs = new AtomicLong(0);

    private final AtomicLong latencyPublishMsSum   = new AtomicLong();
    private final AtomicLong latencyPublishMsCount = new AtomicLong();
    private final AtomicLong latencyEventMsSum     = new AtomicLong();
    private final AtomicLong latencyEventMsCount   = new AtomicLong();

    private volatile TableView<byte[]> controlTopicView;
    private volatile Context boundContext;
    private volatile boolean processingTimeMetricEnabled = true;
    private volatile int logTimingEvery = 0;
    private final AtomicLong timingLogCounter = new AtomicLong();

    @Override
    public void initialize(Context ctx) throws Exception {
        String controlTopic = resolveControlTopic(ctx);
        if (controlTopic == null || controlTopic.isBlank()) {
            throw new RuntimeException(
                    "Invalid configuration. '" + CFG_CONTROL_TOPIC + "' must be provided in userConfig.");
        }

        this.boundContext = ctx;
        this.processingTimeMetricEnabled = resolveBoolean(ctx, CFG_PROCESSING_TIME_METRIC, true);
        this.logTimingEvery = resolveInt(ctx, CFG_LOG_TIMING_EVERY, 0);
        LOG.info("Processing-time metric ({}) is {}", METRIC_PROCESSING_TIME_MS,
                processingTimeMetricEnabled ? "ENABLED" : "DISABLED");

        LOG.info("[debug-init] starting TableView on control topic {}", controlTopic);
        PulsarClient pulsarClient = ctx.getPulsarClient();
        this.controlTopicView = pulsarClient.newTableView(Schema.BYTES)
                .topic(controlTopic)
                .create();
        LOG.info("[debug-init] TableView created, loading entries...");

        controlTopicView.forEachAndListen(this::onQueryEvent);
        LOG.info("[debug-init] forEachAndListen done, {} queries loaded", compiled.size());

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
        final long startMs    = System.currentTimeMillis();

        long inCount = messagesIn.incrementAndGet();
        int payloadBytes = payload == null ? 0 : payload.length;
        ctx.recordMetric(METRIC_MESSAGES_IN, 1.0);
        ctx.recordMetric(METRIC_BYTES_IN, payloadBytes);

        // Debug: log first 10 messages to confirm format is correct after kafkaEntryFormat fix.
        // "Discarding corrupted message" in ConsumerImpl means KoP batch-format entries (produced
        // WITHOUT kafkaEntryFormat=pulsar_non_batched) reached the consumer before it was able to
        // decode them. Each discard consumes a flow-control permit without returning it, driving
        // availablePermits negative and deadlocking dispatch. Fix: set kafkaEntryFormat=
        // pulsar_non_batched on the topic AND namespace, then restart KoP producers (ShadowTraffic).
        if (inCount <= 10) {
            Record<?> dbgRecord = ctx.getCurrentRecord();
            String dbgKey = dbgRecord.getKey().orElse("(no key)");
            long dbgPt = dbgRecord.getMessage().map(m -> m.getPublishTime()).orElse(-1L);
            long dbgEt = dbgRecord.getMessage().map(m -> m.getEventTime()).orElse(-1L);
            int dbgProps = dbgRecord.getProperties() == null ? 0 : dbgRecord.getProperties().size();
            // A zero-length payload with no key is a telltale sign of a KoP batch-container
            // artifact that slipped through (should have been discarded by ConsumerImpl).
            boolean suspicious = (payloadBytes == 0 && dbgKey.equals("(no key)"));
            LOG.info("[debug-process] msg#{} key={} payloadBytes={} publishTime={} eventTime={} props={} compiled={} suspicious={}",
                    inCount, dbgKey, payloadBytes, dbgPt, dbgEt, dbgProps, compiled.size(), suspicious);
            if (suspicious) {
                LOG.warn("[debug-process] msg#{} looks like a KoP batch-container artifact — acking as dropped. " +
                         "Root cause: kafkaEntryFormat not set on topic before this message was produced.", inCount);
            }
        }

        // Periodic heartbeat — fires regardless of throughput so operators can tell
        // the function is alive even when it is stuck waiting on a send.
        emitHeartbeat(ctx);

        Record<?> record = ctx.getCurrentRecord();

        if (compiled.isEmpty()) {
            LOG.warn("[debug-process] compiled map is EMPTY — dropping msg#{}", inCount);
            return ackAsDropped(ctx, record, payloadBytes, startNanos);
        }

        MessageBindings bindings = MessageBindings.from(record, payload);
        final String msgKey = bindings.getKey();
        // Preserve the source event time so end-to-end latency survives the filter hop. Without this,
        // the output producer stamps a fresh publish time at re-publish, and any downstream consumer
        // measuring latency off the filtered topic's timestamps under-counts by the ingest time.
        // Under KoP, eventTime carries the Kafka producer's CreateTime (see recordMatchLatency note).
        final long srcEventTime = record.getMessage().map(m -> m.getEventTime()).orElse(0L);

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
                if (msgKey != null) {
                    builder = builder.key(msgKey);
                }
                if (srcEventTime > 0) {
                    builder = builder.eventTime(srcEventTime);
                }
                final String outputTopic = cq.outputTopic();
                pendingSends.incrementAndGet();
                CompletableFuture<?> send = builder.sendAsync()
                        .whenComplete((r, e) -> {
                            pendingSends.decrementAndGet();
                            long elapsedMs = System.currentTimeMillis() - startMs;
                            if (e != null) {
                                LOG.error("[send-failure] topic={} key={} elapsedMs={}: {}",
                                        outputTopic, msgKey, elapsedMs, e.getMessage());
                            } else if (elapsedMs >= SLOW_SEND_ERROR_MS) {
                                LOG.error("[send-stall] topic={} key={} elapsedMs={} — broker may be unreachable or topic missing; "
                                        + "this blocks flow-control permits and will zombie the function",
                                        outputTopic, msgKey, elapsedMs);
                            } else if (elapsedMs >= SLOW_SEND_WARN_MS) {
                                LOG.warn("[send-slow] topic={} key={} elapsedMs={} pending={}",
                                        outputTopic, msgKey, elapsedMs, pendingSends.get());
                            }
                        });
                sends.add(send);
            }
        }

        if (sends.isEmpty()) {
            return ackAsDropped(ctx, record, payloadBytes, startNanos);
        }

        // (b1) value-add latency: entry → all sends dispatched (deserialize + eval + build +
        // enqueue). Recorded synchronously on the process-loop thread, matched-only.
        recordDispatchTime(ctx, startNanos);
        // Capture ms-precision dispatch-done time for the human-readable [timing] log.
        final long evalDoneMs = System.currentTimeMillis();

        final int fanout = sends.size();
        return CompletableFuture.allOf(sends.toArray(new CompletableFuture[0]))
                .whenComplete((v, err) -> {
                    if (err != null) {
                        ctx.recordMetric(METRIC_SEND_FAILURES, 1.0);
                        LOG.warn("[send-fail] one or more sends failed for key={} fanout={}; failing record",
                                msgKey, fanout, err);
                        record.fail();
                    } else {
                        record.ack();
                        ctx.recordMetric(METRIC_MESSAGES_MATCHED, 1.0);
                        ctx.recordMetric(METRIC_MESSAGES_OUT, fanout);
                        ctx.recordMetric(METRIC_BYTES_OUT, (double) payloadBytes * fanout);
                        recordMatchLatency(ctx, record);
                        // (b) total value-add latency: entry → all output sends acked (matched-only).
                        recordMatchProcessingTime(ctx, startNanos);

                        // Human-readable phase breakdown every logTimingEvery matched messages.
                        // queue = producer CreateTime → process() called. When broker-entry-metadata
                        //         is enabled it splits into:
                        //   producer = CreateTime → broker receive (producer accumulator + network)
                        //   ingest   = broker receive → process() (BookKeeper add-entry + dispatch)
                        // eval  = process() start → all sendAsync() submitted (CPU value-add)
                        // send  = sendAsync() submitted → broker acks output (network round-trip)
                        // Under KoP, publishTime = the Kafka producer's CreateTime (NOT broker-receive),
                        // so the producer/ingest split needs brokerPublishTime; it shows '?' when the
                        // AppendBrokerTimestampMetadataInterceptor isn't enabled on the cluster.
                        int every = logTimingEvery;
                        if (every > 0 && timingLogCounter.incrementAndGet() % every == 0) {
                            long totalMs = System.currentTimeMillis() - startMs;
                            long evalMs  = evalDoneMs - startMs;
                            long sendMs  = totalMs - evalMs;
                            var msgOpt = record.getMessage();
                            long pt  = msgOpt.map(m -> m.getPublishTime()).orElse(-1L);
                            long bpt = msgOpt.flatMap(m -> m.getBrokerPublishTime()).orElse(-1L);
                            String queue    = pt > 0             ? (startMs - pt) + "ms" : "?";
                            String producer = (bpt > 0 && pt > 0) ? (bpt - pt) + "ms"    : "?";
                            String ingest   = bpt > 0            ? (startMs - bpt) + "ms" : "?";
                            LOG.info("[timing] key={} queue={} producer={} ingest={} eval={}ms send={}ms total={}ms fanout={} pending={}",
                                    msgKey, queue, producer, ingest,
                                    evalMs, sendMs, totalMs, fanout, pendingSends.get());
                        }

                        long c = filteredCount.addAndGet(fanout);
                        if (c % LOG_FREQUENCY < fanout) {
                            LOG.info("Filtered {} messages (in={} dropped={} active-queries={})",
                                    c, inCount, droppedCount.get(), compiled.size());
                        }
                    }
                    recordProcessingTime(ctx, startNanos);
                })
                .thenApply(v -> null);
    }

    /**
     * Emits a heartbeat log at most once per {@link #HEARTBEAT_INTERVAL_MS}.
     * Visible even when the function is stuck waiting on pending sends — the key
     * signal for detecting the zombie pattern where availablePermits → 0.
     */
    private void emitHeartbeat(Context ctx) {
        long now = System.currentTimeMillis();
        long last = lastHeartbeatMs.get();
        if (now - last >= HEARTBEAT_INTERVAL_MS && lastHeartbeatMs.compareAndSet(last, now)) {
            int pending = pendingSends.get();
            LOG.info("[heartbeat] in={} matched={} dropped={} queries={} pending-sends={}{}",
                    messagesIn.get(), filteredCount.get(), droppedCount.get(),
                    compiled.size(), pending,
                    pending > 0 ? " *** SENDS BLOCKED — flow-control will stall ***" : "");
            ctx.recordMetric("filter.pending.sends", pending);
        }
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
     * Records broker-to-forward and event-to-forward latency for a matched message.
     * Both use running sums + counts so the observability can compute averages over any window.
     *
     * <p>Note: under KoP, <em>both</em> {@code publishTime} and {@code eventTime} are set from the
     * Kafka record timestamp (the producer's CreateTime) — not the broker-receive instant — so these
     * two metrics measure the same producer→forward span. The true broker-receive time is
     * {@code brokerPublishTime} (broker entry metadata), used by the {@code [timing]} log to split
     * producer-accumulator from broker-ingest. Either timestamp can be 0 if unavailable; those
     * messages are skipped for that metric.
     */
    private void recordMatchLatency(Context ctx, Record<?> record) {
        record.getMessage().ifPresent(msg -> {
            long now = System.currentTimeMillis();

            long pt = msg.getPublishTime();
            if (pt > 0) {
                long latMs = now - pt;
                if (latMs >= 0) {
                    ctx.recordMetric(METRIC_LATENCY_PUBLISH_MS_SUM,   (double) latencyPublishMsSum.addAndGet(latMs));
                    ctx.recordMetric(METRIC_LATENCY_PUBLISH_MS_COUNT, (double) latencyPublishMsCount.incrementAndGet());
                }
            }

            long et = msg.getEventTime();
            if (et > 0) {
                long latMs = now - et;
                if (latMs >= 0) {
                    ctx.recordMetric(METRIC_LATENCY_EVENT_MS_SUM,   (double) latencyEventMsSum.addAndGet(latMs));
                    ctx.recordMetric(METRIC_LATENCY_EVENT_MS_COUNT, (double) latencyEventMsCount.incrementAndGet());
                }
            }
        });
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

    /**
     * Records {@link #METRIC_DISPATCH_TIME_MS} — the synchronous (b1) value-add span from
     * {@code process()} entry to all sends dispatched. Called on the process-loop thread for a
     * matched record only. No-op when the metric is disabled (and {@code startNanos} is then 0).
     */
    private void recordDispatchTime(Context ctx, long startNanos) {
        if (!processingTimeMetricEnabled) {
            return;
        }
        double elapsedMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        ctx.recordMetric(METRIC_DISPATCH_TIME_MS, elapsedMs);
    }

    /**
     * Records {@link #METRIC_MATCH_PROCESSING_TIME_MS} — the full (b) value-add span from
     * {@code process()} entry to all output sends acked, for a matched record. Called from the
     * matched-fanout completion (async, broker IO thread) on success. No-op when disabled.
     */
    private void recordMatchProcessingTime(Context ctx, long startNanos) {
        if (!processingTimeMetricEnabled) {
            return;
        }
        double elapsedMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        ctx.recordMetric(METRIC_MATCH_PROCESSING_TIME_MS, elapsedMs);
    }

    private static boolean resolveBoolean(Context ctx, String key, boolean defaultValue) {
        Object value = ctx.getUserConfigValue(key).orElse(null);
        if (value == null) return defaultValue;
        return Boolean.parseBoolean(value.toString());
    }

    private static int resolveInt(Context ctx, String key, int defaultValue) {
        Object value = ctx.getUserConfigValue(key).orElse(null);
        if (value == null) return defaultValue;
        try { return Integer.parseInt(value.toString()); }
        catch (NumberFormatException e) { return defaultValue; }
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
        compiled.put(queryId, new CompiledQuery(queryId, qm.getName(), qm.getOutputTopic(), evaluator,
                qm.getValueSchema()));
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

    /** Test-only hook: the loaded {@link CompiledQuery} for {@code queryId}, or {@code null}. */
    CompiledQuery queryForTest(String queryId) {
        return compiled.get(queryId);
    }

    @Override
    public void close() throws Exception {
        if (controlTopicView != null) {
            controlTopicView.close();
        }
        compiled.clear();
    }
}
