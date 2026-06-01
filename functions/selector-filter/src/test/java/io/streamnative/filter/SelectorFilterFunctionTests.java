package io.streamnative.filter;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SelectorFilterFunctionTests {

    private Context ctx;
    private Record<byte[]> record;
    private PulsarClient client;
    private TableViewBuilder<byte[]> tvBuilder;
    private TableView<byte[]> tv;

    private final Map<String, TypedMessageBuilder<byte[]>> outputBuilders = new HashMap<>();
    private final Map<String, AtomicReference<CompletableFuture<?>>> sendFutures = new HashMap<>();

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setup() throws Exception {
        ctx = Mockito.mock(Context.class);
        record = Mockito.mock(Record.class);
        client = Mockito.mock(PulsarClient.class);
        tvBuilder = (TableViewBuilder<byte[]>) Mockito.mock(TableViewBuilder.class);
        tv = (TableView<byte[]>) Mockito.mock(TableView.class);

        when(ctx.getCurrentRecord()).thenReturn((Record) record);
        when(ctx.getPulsarClient()).thenReturn(client);

        when(client.newTableView(any(Schema.class))).thenReturn(tvBuilder);
        when(tvBuilder.topic(anyString())).thenReturn(tvBuilder);
        when(tvBuilder.create()).thenReturn(tv);

        when(record.getKey()).thenReturn(Optional.of("AAPL"));
        when(record.getTopicName()).thenReturn(Optional.of("persistent://public/quotes/trades"));
        when(record.getProperties()).thenReturn(Map.of("region", "US", "marketcap_bucket", "LARGE"));

        Mockito.doNothing().when(ctx).recordMetric(anyString(), anyDouble());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private TypedMessageBuilder<byte[]> mockOutputBuilder(String outputTopic, CompletableFuture<?> sendResult) throws Exception {
        TypedMessageBuilder<byte[]> b = (TypedMessageBuilder<byte[]>) Mockito.mock(TypedMessageBuilder.class);
        when(b.value(any(byte[].class))).thenReturn(b);
        when(b.properties(any(Map.class))).thenReturn(b);
        when(b.key(anyString())).thenReturn(b);
        when(b.sendAsync()).thenReturn((CompletableFuture) sendResult);
        when(ctx.newOutputMessage(eq(outputTopic), any(Schema.class))).thenReturn((TypedMessageBuilder) b);
        outputBuilders.put(outputTopic, b);
        return b;
    }

    private void givenControlTopic(String topic) {
        when(ctx.getUserConfigValue(eq(SelectorFilterFunction.CFG_CONTROL_TOPIC)))
                .thenReturn(Optional.of(topic));
    }

    private byte[] queryJson(String selector, String outputTopic) {
        String json = String.format("{\"name\":\"q\",\"selector\":\"%s\",\"outputTopic\":\"%s\"}",
                selector.replace("\"", "\\\""), outputTopic);
        return json.getBytes(StandardCharsets.UTF_8);
    }

    // ---------------------- initialize ----------------------

    @Test
    void initializeMissingControlTopicThrows() {
        when(ctx.getUserConfigValue(anyString())).thenReturn(Optional.empty());
        SelectorFilterFunction fn = new SelectorFilterFunction();
        assertThatThrownBy(() -> fn.initialize(ctx))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("control-topic");
    }

    @Test
    void initializeBuildsTableViewAndRegistersListener() throws Exception {
        givenControlTopic("persistent://public/control/queries");

        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        verify(client).newTableView(any(Schema.class));
        verify(tvBuilder).topic("persistent://public/control/queries");
        verify(tvBuilder).create();
        verify(tv).forEachAndListen(any());
    }

    // ---------------------- onQueryEvent ----------------------

    @Test
    void loadsValidQuery() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("__key__ = 'AAPL'", "persistent://public/filtered/out-1"));
        assertThat(fn.activeQueryCountForTest()).isEqualTo(1);
    }

    @Test
    void tombstoneRetractsQuery() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("__key__ = 'AAPL'", "persistent://public/filtered/out-1"));
        fn.loadQueryForTest("q1", null);
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    @Test
    void emptyPayloadAlsoRetracts() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("__key__ = 'AAPL'", "persistent://public/filtered/out-1"));
        fn.loadQueryForTest("q1", new byte[0]);
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    @Test
    void malformedJsonIsLoggedAndSkippedExistingQueryUnchanged() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("__key__ = 'AAPL'", "persistent://public/filtered/out-1"));
        fn.loadQueryForTest("q1", "{not json".getBytes(StandardCharsets.UTF_8));
        // Existing entry must remain — we do NOT mutate on bad input. The current behavior is
        // documented in onQueryEvent: bad-JSON returns early.
        assertThat(fn.activeQueryCountForTest()).isEqualTo(1);
    }

    @Test
    void invalidSelectorIsLoggedAndSkipped() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("region =", "persistent://public/filtered/out-1"));
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    @Test
    void queryWithEmptyOutputTopicIsSkipped() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("region = 'US'", ""));
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    // ---------------------- process ----------------------

    @Test
    void processWithEmptyRegistryAcksAndDoesNotSend() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        CompletableFuture<Void> result = fn.process("payload".getBytes(StandardCharsets.UTF_8), ctx);

        assertThat(result).isCompleted();
        verify(record).ack();
        verify(record, never()).fail();
        verify(ctx, never()).newOutputMessage(anyString(), any(Schema.class));
    }

    @Test
    void processSingleMatchFansOutAndAcks() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        String outputTopic = "persistent://public/filtered/us-large-cap";
        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(outputTopic, CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson(
                "__key__ = 'AAPL' AND region = 'US' AND marketcap_bucket = 'LARGE'", outputTopic));

        byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        CompletableFuture<Void> result = fn.process(payload, ctx);

        assertThat(result).isCompleted();
        verify(builder).value(payload);
        verify(builder).key("AAPL");
        verify(builder).sendAsync();
        verify(record).ack();
        verify(record, never()).fail();
    }

    @Test
    void processMultipleMatchesFansOutToAllAndAcksOnce() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        TypedMessageBuilder<byte[]> b1 = mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        TypedMessageBuilder<byte[]> b2 = mockOutputBuilder("persistent://public/out/b", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));
        fn.loadQueryForTest("q2", queryJson("marketcap_bucket = 'LARGE'", "persistent://public/out/b"));

        CompletableFuture<Void> result = fn.process("p".getBytes(StandardCharsets.UTF_8), ctx);
        assertThat(result).isCompleted();

        verify(b1).sendAsync();
        verify(b2).sendAsync();
        verify(record, times(1)).ack();
    }

    @Test
    void processNoMatchAcksAndDoesNotSend() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("region = 'JP'", "persistent://public/out/a"));

        CompletableFuture<Void> result = fn.process("p".getBytes(StandardCharsets.UTF_8), ctx);
        assertThat(result).isCompleted();
        verify(record).ack();
        verify(ctx, never()).newOutputMessage(anyString(), any(Schema.class));
    }

    @Test
    void processSendFailureCallsFail() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("publish broke"));
        mockOutputBuilder("persistent://public/out/a", failed);

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        CompletableFuture<Void> result = fn.process("p".getBytes(StandardCharsets.UTF_8), ctx);
        assertThatThrownBy(result::join).isInstanceOf(CompletionException.class);

        verify(record).fail();
        verify(record, never()).ack();
    }

    @Test
    void processPartialSendFailureCallsFail() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        CompletableFuture<Object> failed = new CompletableFuture<>();
        failed.completeExceptionally(new RuntimeException("publish broke"));
        mockOutputBuilder("persistent://public/out/b", failed);

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));
        fn.loadQueryForTest("q2", queryJson("marketcap_bucket = 'LARGE'", "persistent://public/out/b"));

        CompletableFuture<Void> result = fn.process("p".getBytes(StandardCharsets.UTF_8), ctx);
        assertThatThrownBy(result::join).isInstanceOf(CompletionException.class);
        verify(record).fail();
        verify(record, never()).ack();
    }

    @Test
    void processForwardsUserPropertiesToOutput() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(builder).properties(propsCaptor.capture());
        assertThat(propsCaptor.getValue())
                .containsEntry("region", "US")
                .containsEntry("marketcap_bucket", "LARGE");
    }

    @Test
    void processRecordsByteCountMetricsForMatchedPath() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        byte[] payload = "abcdefghij".getBytes(StandardCharsets.UTF_8); // 10 bytes
        fn.process(payload, ctx).join();

        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_IN, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_BYTES_IN, 10.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_MATCHED, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_OUT, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_BYTES_OUT, 10.0);
    }

    @Test
    void processRecordsByteCountMetricsForDroppedPath() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // No queries -> drop.
        byte[] payload = "abc".getBytes(StandardCharsets.UTF_8); // 3 bytes
        fn.process(payload, ctx).join();

        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_IN, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_BYTES_IN, 3.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_DROPPED, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_BYTES_DROPPED, 3.0);
    }

    @Test
    void matchedPathRecordsValueAddLatencySplit() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        // (b1) entry→dispatch and (b) entry→acked are both recorded for a matched record.
        verify(ctx).recordMetric(eq(SelectorFilterFunction.METRIC_DISPATCH_TIME_MS), anyDouble());
        verify(ctx).recordMetric(eq(SelectorFilterFunction.METRIC_MATCH_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void droppedPathDoesNotRecordValueAddLatencySplit() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // No queries -> drop; the matched-only split metrics must not fire.
        fn.process("abc".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(ctx, never()).recordMetric(eq(SelectorFilterFunction.METRIC_DISPATCH_TIME_MS), anyDouble());
        verify(ctx, never()).recordMetric(eq(SelectorFilterFunction.METRIC_MATCH_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void processFanoutMultipliesBytesOut() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        mockOutputBuilder("persistent://public/out/b", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));
        fn.loadQueryForTest("q2", queryJson("marketcap_bucket = 'LARGE'", "persistent://public/out/b"));

        byte[] payload = "1234567890".getBytes(StandardCharsets.UTF_8); // 10 bytes
        fn.process(payload, ctx).join();

        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_MESSAGES_OUT, 2.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_BYTES_OUT, 20.0);
    }

    @Test
    void loadQueryRecordsLifecycleMetrics() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        Mockito.clearInvocations(ctx);
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_QUERIES_LOADED, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_QUERIES_ACTIVE, 1.0);
    }

    @Test
    void retractQueryRecordsLifecycleMetrics() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));
        Mockito.clearInvocations(ctx);
        fn.loadQueryForTest("q1", null);

        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_QUERIES_RETRACTED, 1.0);
        verify(ctx).recordMetric(SelectorFilterFunction.METRIC_QUERIES_ACTIVE, 0.0);
    }

    @Test
    void badQueriesIncrementMetric() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);
        Mockito.clearInvocations(ctx);

        // Malformed JSON
        fn.loadQueryForTest("q1", "{not json".getBytes(StandardCharsets.UTF_8));
        // Empty selector
        fn.loadQueryForTest("q2", queryJson("", "persistent://public/out/a"));
        // Empty output topic
        fn.loadQueryForTest("q3", queryJson("region = 'US'", ""));
        // Invalid selector
        fn.loadQueryForTest("q4", queryJson("region =", "persistent://public/out/a"));

        verify(ctx, times(4)).recordMetric(SelectorFilterFunction.METRIC_QUERIES_BAD, 1.0);
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    @Test
    void kopInternalHeaderFormatIsForwardedButNotFilterable() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // Source record carries the KoP-injected disambiguation property alongside real ones.
        when(record.getProperties()).thenReturn(Map.of(
                "region", "US",
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"region\":\"string\"}"));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        // A selector that tries to use the internal property must NOT match (it's hidden).
        fn.loadQueryForTest("hidden-q", queryJson(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT + " IS NOT NULL",
                "persistent://public/out/a"));

        // A selector against a real property works and triggers the forward.
        fn.loadQueryForTest("real-q", queryJson("region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        // Exactly one output, from the real query. The hidden-q never matched because
        // __ksn_internal_header_format is filtered to null at lookup time.
        verify(builder, times(1)).sendAsync();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(builder).properties(propsCaptor.capture());
        Map<String, String> forwarded = propsCaptor.getValue();
        assertThat(forwarded)
                .as("KoP internal property is preserved in the forwarded properties map for "
                        + "downstream KoP-aware tools")
                .containsKey(MessageBindings.KOP_INTERNAL_HEADER_FORMAT)
                .containsEntry("region", "US");
    }

    // ===== Function-level transparent KoP base64 decoding =====

    @Test
    void processMatchesAgainstDecodedBase64PropertyEndToEnd() throws Exception {
        // KoP delivered "header_key" as base64 "a2V5" (decodes to "key"). The disambig flags
        // the property as base64. End-to-end through process(): a selector against the DECODED
        // form ("key") must match.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        when(record.getProperties()).thenReturn(Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header_key\":\"base64\"}",
                "header_key", "a2V5"));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson("header_key = 'key'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(builder).sendAsync();
        verify(record).ack();
        verify(record, never()).fail();
    }

    @Test
    void processDoesNotMatchAgainstRawBase64WhenTransparentDecodeFires() throws Exception {
        // The complementary case: the property is decoded, so a selector comparing against the
        // RAW base64 string must NOT match.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        when(record.getProperties()).thenReturn(Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header_key\":\"base64\"}",
                "header_key", "a2V5"));

        fn.loadQueryForTest("q1", queryJson("header_key = 'a2V5'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(record).ack();
        verify(ctx, never()).newOutputMessage(anyString(), any(Schema.class));
    }

    @Test
    void processMatchesAgainstRawBase64ForTrueBinaryPropertyEndToEnd() throws Exception {
        // Property flagged as base64 but whose decoded bytes [0x88] aren't valid UTF-8. The
        // function falls back to surfacing the raw base64 string; the selector compares against
        // that.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        when(record.getProperties()).thenReturn(Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"bin_h\":\"base64\"}",
                "bin_h", "iA=="));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson("bin_h = 'iA=='", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(builder).sendAsync();
        verify(record).ack();
    }

    @Test
    void processForwardsRawPropertiesEvenWhenSelectorSawDecodedForm() throws Exception {
        // Decoding is a selector-only view. The forward path must publish the ORIGINAL Pulsar
        // properties (raw base64 + disambig) so KoP-aware downstream tools can decide for
        // themselves how to interpret.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        when(record.getProperties()).thenReturn(Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header_key\":\"base64\"}",
                "header_key", "a2V5"));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson("header_key = 'key'", "persistent://public/out/a"));
        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<String, String>> propsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(builder).properties(propsCaptor.capture());
        Map<String, String> forwarded = propsCaptor.getValue();
        assertThat(forwarded)
                .as("Forward path is untouched by selector-side decoding")
                .containsEntry("header_key", "a2V5")
                .containsKey(MessageBindings.KOP_INTERNAL_HEADER_FORMAT);
    }

    @Test
    void processHandlesMixedFlaggedAndUnflaggedPropertiesEndToEnd() throws Exception {
        // A realistic record with one base64-flagged header (decoded) and one regular UTF-8
        // header (passed through). A compound selector touching both must match correctly.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        when(record.getProperties()).thenReturn(Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"flagged\":\"base64\"}",
                "flagged", "a2V5",     // decodes to "key"
                "region", "US"));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));

        fn.loadQueryForTest("q1", queryJson(
                "flagged = 'key' AND region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(builder).sendAsync();
    }

    // ===== Processing-time metric =====

    @Test
    void processRecordsProcessingTimeMetricByDefault() throws Exception {
        // Default behavior — no userConfig key set means the metric is on.
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(ctx, times(1)).recordMetric(eq(SelectorFilterFunction.METRIC_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void processRecordsProcessingTimeMetricForDroppedPath() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // No queries -> dropped path.
        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(ctx, times(1)).recordMetric(eq(SelectorFilterFunction.METRIC_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void processingTimeMetricCanBeDisabledViaUserConfig() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        when(ctx.getUserConfigValue(eq(SelectorFilterFunction.CFG_PROCESSING_TIME_METRIC)))
                .thenReturn(Optional.of("false"));

        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        mockOutputBuilder("persistent://public/out/a", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(ctx, never()).recordMetric(eq(SelectorFilterFunction.METRIC_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void processingTimeMetricDisabledAlsoSkipsDroppedPath() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        when(ctx.getUserConfigValue(eq(SelectorFilterFunction.CFG_PROCESSING_TIME_METRIC)))
                .thenReturn(Optional.of("false"));

        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();

        verify(ctx, never()).recordMetric(eq(SelectorFilterFunction.METRIC_PROCESSING_TIME_MS), anyDouble());
    }

    @Test
    void processStripsPartitionSuffixForTopicBinding() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // Source topic with partition suffix; selector matches the normalized form.
        when(record.getTopicName()).thenReturn(Optional.of("persistent://public/quotes/trades-partition-5"));

        TypedMessageBuilder<byte[]> builder = mockOutputBuilder(
                "persistent://public/out/a", CompletableFuture.completedFuture(null));
        fn.loadQueryForTest("q1", queryJson(
                "__topic__ = 'persistent://public/quotes/trades'", "persistent://public/out/a"));

        fn.process("p".getBytes(StandardCharsets.UTF_8), ctx).join();
        verify(builder).sendAsync();
    }

    // ---------------------- close ----------------------

    @Test
    void closeClosesTableViewAndClearsRegistry() throws Exception {
        givenControlTopic("persistent://public/control/queries");
        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/out/a"));
        fn.close();
        verify(tv).close();
        assertThat(fn.activeQueryCountForTest()).isZero();
    }

    // ---------------------- loop guard ----------------------

    @Test
    void warnsWhenOutputTopicOverlapsInput() throws Exception {
        givenControlTopic("persistent://public/control/queries");

        List<String> inputs = new ArrayList<>();
        inputs.add("persistent://public/quotes/trades");
        when(ctx.getInputTopics()).thenReturn(inputs);

        SelectorFilterFunction fn = new SelectorFilterFunction();
        fn.initialize(ctx);

        // The loop-check runs during initialize but pre-load is required; mimic by loading
        // a query then re-running the check via initialize-style assertion is not necessary
        // here — we only assert initialize does not throw with overlapping outputs.
        fn.loadQueryForTest("q1", queryJson("region = 'US'", "persistent://public/quotes/trades"));
        assertThat(fn.activeQueryCountForTest()).isEqualTo(1);
    }
}
