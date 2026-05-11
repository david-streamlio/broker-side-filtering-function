package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.QueryMessage;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for OrchestratorFunction.process() routing logic. Mocks the TableView (registry
 * reads), Producer (state + per-function control topic writes), FilterFunctionSpawner, and
 * Context.
 */
class OrchestratorFunctionTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private TableView<byte[]> stateView;
    private Producer<byte[]> stateProducer;
    private TypedMessageBuilder<byte[]> stateMsgBuilder;
    private PulsarClient pulsarClient;
    private FilterFunctionSpawner spawner;
    private ConcurrentMap<String, Producer<byte[]>> controlProducers;

    /** Per-control-topic capture of all (key, payload) pairs published. */
    private Map<String, List<KV>> controlPublishes;

    private OrchestratorConfig config;
    private OrchestratorFunction orchestrator;

    /** Simulates the registry's TableView storage so tests can pre-seed entries. */
    private final Map<String, byte[]> tableStorage = new HashMap<>();

    private record KV(String key, byte[] value) {}

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
        stateView = (TableView<byte[]>) mock(TableView.class);
        stateProducer = (Producer<byte[]>) mock(Producer.class);
        stateMsgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        pulsarClient = mock(PulsarClient.class);
        spawner = mock(FilterFunctionSpawner.class);
        controlProducers = new ConcurrentHashMap<>();
        controlPublishes = new HashMap<>();

        // TableView reads are served from tableStorage.
        when(stateView.get(anyString())).thenAnswer(inv -> tableStorage.get(inv.getArgument(0, String.class)));

        // State producer writes go back into tableStorage (or remove on null).
        when(stateProducer.newMessage()).thenReturn(stateMsgBuilder);
        org.mockito.Mockito.doAnswer(inv -> {
            stateLastKey = inv.getArgument(0, String.class);
            return stateMsgBuilder;
        }).when(stateMsgBuilder).key(anyString());
        org.mockito.Mockito.doAnswer(inv -> {
            stateStoreNow(stateLastKey, inv.getArgument(0));
            return stateMsgBuilder;
        }).when(stateMsgBuilder).value(any(byte[].class));
        org.mockito.Mockito.doAnswer(inv -> {
            stateStoreNow(stateLastKey, null);
            return stateMsgBuilder;
        }).when(stateMsgBuilder).value(isNull());

        config = OrchestratorConfig.forTesting(
                "persistent://public/orchestrator/registry-state",
                "function://public/filters/selector-filter@1.0.0",
                "public", "filtered");

        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient, controlProducers);
    }

    private String stateLastKey;

    private void stateStoreNow(String key, byte[] bytes) {
        if (bytes == null) tableStorage.remove(key);
        else tableStorage.put(key, bytes);
    }

    // ---------------------------------------------------------------- Helpers

    private Context contextFor(String queryId) {
        Context ctx = mock(Context.class);
        @SuppressWarnings("unchecked")
        Record<byte[]> rec = (Record<byte[]>) mock(Record.class);
        when(rec.getKey()).thenReturn(Optional.of(queryId));
        when(ctx.getCurrentRecord()).thenAnswer(inv -> rec);
        return ctx;
    }

    @SuppressWarnings("unchecked")
    private Producer<byte[]> stubControlProducer(String controlTopic) {
        Producer<byte[]> p = (Producer<byte[]>) mock(Producer.class);
        TypedMessageBuilder<byte[]> b = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        when(p.newMessage()).thenReturn(b);
        final String[] lastKey = {null};
        // doAnswer().when() form avoids the phantom-invocation footgun that the when().thenAnswer()
        // form trips when combined with any() matchers on void-returning mock setups.
        org.mockito.Mockito.doAnswer(inv -> {
            lastKey[0] = inv.getArgument(0, String.class);
            return b;
        }).when(b).key(anyString());
        org.mockito.Mockito.doAnswer(inv -> {
            byte[] v = inv.getArgument(0);
            controlPublishes.computeIfAbsent(controlTopic, k -> new ArrayList<>())
                    .add(new KV(lastKey[0], v));
            return b;
        }).when(b).value(any(byte[].class));
        org.mockito.Mockito.doAnswer(inv -> {
            controlPublishes.computeIfAbsent(controlTopic, k -> new ArrayList<>())
                    .add(new KV(lastKey[0], null));
            return b;
        }).when(b).value(isNull());
        controlProducers.put(controlTopic, p);
        return p;
    }

    private void seedRegistryEntry(String registryKey, RegistryEntry entry) throws Exception {
        tableStorage.put(RegistryStore.TOPIC_KEY_PREFIX + registryKey, MAPPER.writeValueAsBytes(entry));
    }

    private void seedQueryEntry(String queryId, QueryEntry entry) throws Exception {
        tableStorage.put(RegistryStore.QUERY_KEY_PREFIX + queryId, MAPPER.writeValueAsBytes(entry));
    }

    private byte[] serialize(FilterRequest req) throws Exception {
        return MAPPER.writeValueAsBytes(req);
    }

    // ---------------------------------------------------------------- REGISTER

    @Test
    void registerOnNewInputTopicSpawnsAndPublishes() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                "region = 'US'",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                ClientType.KAFKA);

        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-trades-abcd1234",
                "selector-filter-trades-abcd1234",
                req.getKeyDeserializer(), req.getValueDeserializer(), ClientType.KAFKA,
                Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        // Spawner was called once for the input topic.
        verify(spawner).spawn("persistent://public/in/trades", req, config);

        // The orchestrator wrote a t: entry AND a q: entry to state.
        assertThat(tableStorage).containsKey(RegistryStore.TOPIC_KEY_PREFIX + "persistent://public/in/trades");
        assertThat(tableStorage).containsKey(RegistryStore.QUERY_KEY_PREFIX + queryId);

        // QueryMessage was published to the spawned function's control topic.
        List<KV> publishes = controlPublishes.get(spawned.getControlTopic());
        assertThat(publishes).hasSize(1);
        assertThat(publishes.get(0).key()).isEqualTo(queryId);
        QueryMessage qm = MAPPER.readValue(publishes.get(0).value(), QueryMessage.class);
        assertThat(qm.getSelector()).isEqualTo("region = 'US'");
        assertThat(qm.getOutputTopic()).isEqualTo("persistent://public/filtered/filtered-abc");
    }

    @Test
    void registerOnExistingInputTopicReusesAndDoesNotSpawn() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-xyz",
                "region = 'UK'",
                "KDeser", "VDeser", ClientType.KAFKA);

        RegistryEntry existing = new RegistryEntry(
                "persistent://public/filtered/ctrl-trades-abcd1234",
                "selector-filter-trades-abcd1234",
                "KDeser", "VDeser", ClientType.KAFKA, "2026-05-10T00:00:00Z");
        seedRegistryEntry("persistent://public/in/trades", existing);
        stubControlProducer(existing.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        verify(spawner, never()).spawn(any(), any(), any());

        // QueryMessage routed to the existing function's control topic.
        List<KV> publishes = controlPublishes.get(existing.getControlTopic());
        assertThat(publishes).hasSize(1);
        QueryMessage qm = MAPPER.readValue(publishes.get(0).value(), QueryMessage.class);
        assertThat(qm.getSelector()).isEqualTo("region = 'UK'");
        assertThat(qm.getOutputTopic()).isEqualTo("persistent://public/filtered/filtered-xyz");
    }

    @Test
    void registerWithSchemaMismatchRejectsWithoutSpawningOrPublishing() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-xyz",
                "region = 'UK'",
                "IntegerDeser", "ByteDeser", ClientType.KAFKA);

        // Existing entry has StringDeser; incoming says IntegerDeser → reject.
        RegistryEntry existing = new RegistryEntry(
                "persistent://public/filtered/ctrl-trades-abcd1234",
                "selector-filter-trades-abcd1234",
                "StringDeser", "ByteDeser", ClientType.KAFKA, "2026-05-10T00:00:00Z");
        seedRegistryEntry("persistent://public/in/trades", existing);

        orchestrator.process(serialize(req), contextFor(queryId));

        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty();
        // No q: entry was written for the rejected queryId.
        assertThat(tableStorage).doesNotContainKey(RegistryStore.QUERY_KEY_PREFIX + queryId);
    }

    @Test
    void registerRedeliveryWithSameFieldsIsIdempotent() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                "region = 'US'",
                "KDeser", "VDeser", ClientType.KAFKA);

        // Pretend a previous successful REGISTER already wrote both entries.
        RegistryEntry existing = new RegistryEntry(
                "persistent://public/filtered/ctrl-x",
                "selector-filter-x",
                "KDeser", "VDeser", ClientType.KAFKA, "2026-05-10T00:00:00Z");
        seedRegistryEntry("persistent://public/in/trades", existing);
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-abc",
                List.of(existing.getControlTopic())));

        orchestrator.process(serialize(req), contextFor(queryId));

        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty(); // no new publish
    }

    @Test
    void registerRedeliveryWithDifferentFieldsIsRejected() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-DIFFERENT",  // different outputTopic
                "region = 'US'",
                "KDeser", "VDeser", ClientType.KAFKA);

        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-abc",
                List.of("persistent://public/filtered/ctrl-x")));

        orchestrator.process(serialize(req), contextFor(queryId));

        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty();
    }

    @Test
    void registerMultiInputTopicFansOutToOneFilterPerTopic() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                List.of("persistent://public/in/a", "persistent://public/in/b"),
                "persistent://public/filtered/filtered-multi",
                "region = 'US'",
                null, null, ClientType.PULSAR);

        RegistryEntry entryA = new RegistryEntry(
                "persistent://public/filtered/ctrl-a", "selector-filter-a",
                null, null, ClientType.PULSAR, Instant.now().toString());
        RegistryEntry entryB = new RegistryEntry(
                "persistent://public/filtered/ctrl-b", "selector-filter-b",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn("persistent://public/in/a", req, config)).thenReturn(entryA);
        when(spawner.spawn("persistent://public/in/b", req, config)).thenReturn(entryB);
        stubControlProducer(entryA.getControlTopic());
        stubControlProducer(entryB.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        verify(spawner).spawn("persistent://public/in/a", req, config);
        verify(spawner).spawn("persistent://public/in/b", req, config);
        assertThat(controlPublishes.get(entryA.getControlTopic())).hasSize(1);
        assertThat(controlPublishes.get(entryB.getControlTopic())).hasSize(1);

        byte[] qBytes = tableStorage.get(RegistryStore.QUERY_KEY_PREFIX + queryId);
        QueryEntry q = MAPPER.readValue(qBytes, QueryEntry.class);
        assertThat(q.getOutputTopic()).isEqualTo("persistent://public/filtered/filtered-multi");
        assertThat(q.getControlTopics()).containsExactlyInAnyOrder(
                entryA.getControlTopic(), entryB.getControlTopic());
    }

    @Test
    void registerWithNullSelectorTranslatesToIdentityDownstream() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                null,                       // no user selector
                null, null, ClientType.PULSAR);

        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        QueryMessage qm = MAPPER.readValue(
                controlPublishes.get(spawned.getControlTopic()).get(0).value(),
                QueryMessage.class);
        // Identity selector — downstream filter function never sees a null/blank.
        assertThat(qm.getSelector()).isEqualTo(OrchestratorFunction.IDENTITY_SELECTOR);
    }

    @Test
    void registerNullPayloadIsAccepted() throws Exception {
        // Defensive — stray null body. Should ack silently with a metric.
        orchestrator.process(null, contextFor(UUID.randomUUID().toString()));
        orchestrator.process(new byte[0], contextFor(UUID.randomUUID().toString()));
        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty();
    }

    // ---------------------------------------------------------------- MODIFY

    @Test
    void modifyRepublishesQueryMessageWithNewSelectorOnSameQueryId() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String controlTopic = "persistent://public/filtered/ctrl-x";
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-abc",
                List.of(controlTopic)));
        stubControlProducer(controlTopic);

        FilterRequest req = FilterRequest.modify("region IN ('US','UK')");
        orchestrator.process(serialize(req), contextFor(queryId));

        List<KV> publishes = controlPublishes.get(controlTopic);
        assertThat(publishes).hasSize(1);
        assertThat(publishes.get(0).key()).isEqualTo(queryId);
        QueryMessage qm = MAPPER.readValue(publishes.get(0).value(), QueryMessage.class);
        assertThat(qm.getSelector()).isEqualTo("region IN ('US','UK')");
        assertThat(qm.getOutputTopic()).isEqualTo("persistent://public/filtered/filtered-abc");
    }

    @Test
    void modifyFansOutAcrossAllRegisteredControlTopics() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String ctrlA = "persistent://public/filtered/ctrl-a";
        String ctrlB = "persistent://public/filtered/ctrl-b";
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-multi", List.of(ctrlA, ctrlB)));
        stubControlProducer(ctrlA);
        stubControlProducer(ctrlB);

        orchestrator.process(serialize(FilterRequest.modify("x = 1")), contextFor(queryId));

        assertThat(controlPublishes.get(ctrlA)).hasSize(1);
        assertThat(controlPublishes.get(ctrlB)).hasSize(1);
    }

    @Test
    void modifyOnUnknownQueryIdIsIgnored() throws Exception {
        // No q: entry seeded — MODIFY on unknown queryId should be silent (metric only).
        orchestrator.process(serialize(FilterRequest.modify("x = 1")), contextFor("unknown-id"));
        assertThat(controlPublishes).isEmpty();
    }

    // ---------------------------------------------------------------- UNREGISTER

    @Test
    void unregisterPublishesTombstonesAndRemovesQueryEntry() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String ctrlA = "persistent://public/filtered/ctrl-a";
        String ctrlB = "persistent://public/filtered/ctrl-b";
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-multi", List.of(ctrlA, ctrlB)));
        stubControlProducer(ctrlA);
        stubControlProducer(ctrlB);

        orchestrator.process(serialize(FilterRequest.unregister()), contextFor(queryId));

        // Both control topics got a null-bodied tombstone for queryId.
        List<KV> pubsA = controlPublishes.get(ctrlA);
        List<KV> pubsB = controlPublishes.get(ctrlB);
        assertThat(pubsA).hasSize(1);
        assertThat(pubsA.get(0).value()).isNull();
        assertThat(pubsA.get(0).key()).isEqualTo(queryId);
        assertThat(pubsB).hasSize(1);
        assertThat(pubsB.get(0).value()).isNull();

        // q: entry removed from storage.
        assertThat(tableStorage).doesNotContainKey(RegistryStore.QUERY_KEY_PREFIX + queryId);
    }

    @Test
    void unregisterOnUnknownQueryIdIsIdempotent() throws Exception {
        orchestrator.process(serialize(FilterRequest.unregister()), contextFor("unknown-id"));
        assertThat(controlPublishes).isEmpty();
        // No exception, no state change.
    }

    // ---------------------------------------------------------------- Validation

    @Test
    void malformedJsonPayloadIsRejected() throws Exception {
        orchestrator.process("not json {".getBytes(), contextFor(UUID.randomUUID().toString()));
        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty();
    }

    @Test
    void missingQueryIdKeyIsRejected() throws Exception {
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/x", "persistent://public/out/x",
                "x = 1", null, null, ClientType.PULSAR);

        Context ctx = mock(Context.class);
        @SuppressWarnings("unchecked")
        Record<byte[]> rec = (Record<byte[]>) mock(Record.class);
        when(rec.getKey()).thenReturn(Optional.empty());
        when(ctx.getCurrentRecord()).thenAnswer(inv -> rec);

        orchestrator.process(serialize(req), ctx);
        verify(spawner, never()).spawn(any(), any(), any());
        assertThat(controlPublishes).isEmpty();
    }
}
