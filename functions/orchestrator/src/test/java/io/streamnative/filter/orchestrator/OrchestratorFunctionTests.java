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
import static org.mockito.ArgumentMatchers.eq;
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
    void registerForwardsValueSchemaIntoQueryMessageAndPersistsIt() throws Exception {
        String queryId = UUID.randomUUID().toString();
        io.streamnative.filter.common.ValueSchemaInfo schema =
                new io.streamnative.filter.common.ValueSchemaInfo(
                        "com.acme.Trade", "AVRO", "eyJ0eXBlIjoicmVjb3JkIn0=", null);
        FilterRequest req = FilterRequest.register(
                List.of("persistent://public/in/trades"),
                "persistent://public/filtered/filtered-vs",
                "price > 100",
                null, "com.acme.Trade", schema, ClientType.PULSAR);

        // Spawn a registry entry from the request, mirroring DefaultFilterFunctionSpawner so the
        // entry carries the value schema the orchestrator forwards.
        RegistryEntry spawned = RegistryEntry.newEntry(
                "persistent://public/filtered/ctrl-vs", "selector-filter-vs", req);
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        // The published QueryMessage carries the full value schema.
        QueryMessage qm = MAPPER.readValue(
                controlPublishes.get(spawned.getControlTopic()).get(0).value(), QueryMessage.class);
        assertThat(qm.getValueSchema()).isEqualTo(schema);

        // The persisted t: and q: entries both carry it (so reconcile/MODIFY republish preserve it).
        RegistryEntry te = MAPPER.readValue(
                tableStorage.get(RegistryStore.TOPIC_KEY_PREFIX + "persistent://public/in/trades"),
                RegistryEntry.class);
        assertThat(te.getValueSchema()).isEqualTo(schema);
        QueryEntry qe = MAPPER.readValue(
                tableStorage.get(RegistryStore.QUERY_KEY_PREFIX + queryId), QueryEntry.class);
        assertThat(qe.getValueSchema()).isEqualTo(schema);
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
    void modifyRepublishesPreservesValueSchemaFromQueryEntry() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String controlTopic = "persistent://public/filtered/ctrl-vs";
        io.streamnative.filter.common.ValueSchemaInfo schema =
                new io.streamnative.filter.common.ValueSchemaInfo("com.acme.Trade", "AVRO", "AAA=", null);
        // QueryEntry persisted at REGISTER carries the schema; MODIFY must republish it.
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-vs",
                List.of(controlTopic), List.of("persistent://public/in/trades"),
                Instant.now().toString(), "price > 100", schema));
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.modify("price > 200")), contextFor(queryId));

        QueryMessage qm = MAPPER.readValue(
                controlPublishes.get(controlTopic).get(0).value(), QueryMessage.class);
        assertThat(qm.getSelector()).isEqualTo("price > 200");
        assertThat(qm.getValueSchema()).isEqualTo(schema);
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

    // ---------------------------------------------------------------- GC bookkeeping (Phase 2)

    private RegistryEntry readRegistryEntry(String registryKey) throws Exception {
        byte[] raw = tableStorage.get(RegistryStore.TOPIC_KEY_PREFIX + registryKey);
        return raw == null ? null : MAPPER.readValue(raw, RegistryEntry.class);
    }

    private QueryEntry readQueryEntry(String queryId) throws Exception {
        byte[] raw = tableStorage.get(RegistryStore.QUERY_KEY_PREFIX + queryId);
        return raw == null ? null : MAPPER.readValue(raw, QueryEntry.class);
    }

    @Test
    void registerRecordsQueryIdInActiveQueryIdsOnSpawn() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                "region = 'US'", null, null, ClientType.PULSAR);

        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        RegistryEntry stored = readRegistryEntry("persistent://public/in/trades");
        assertThat(stored.getActiveQueryIds()).containsExactly(queryId);
        assertThat(stored.getIdleSince()).isNull();
    }

    @Test
    void registerRecordsParallelRegistryKeysOnQueryEntry() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                List.of("persistent://public/in/a", "persistent://public/in/b"),
                "persistent://public/filtered/filtered-multi",
                "region = 'US'", null, null, ClientType.PULSAR);

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

        QueryEntry q = readQueryEntry(queryId);
        // Parallel arrays: registryKeys[i] is the t: key whose entry owns controlTopics[i].
        assertThat(q.getRegistryKeys()).hasSize(2);
        assertThat(q.getControlTopics()).hasSize(2);
        for (int i = 0; i < q.getRegistryKeys().size(); i++) {
            assertThat(q.getControlTopics().get(i))
                    .startsWith("persistent://public/filtered/ctrl-");
        }
    }

    @Test
    void registerOnReusedEntryAddsQueryIdToActiveSet() throws Exception {
        // Pre-seed an entry from a previous REGISTER (queryId q-prior already in the set).
        RegistryEntry existing = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-prior");
        seedRegistryEntry("persistent://public/in/trades", existing);
        stubControlProducer(existing.getControlTopic());

        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-new",
                "x = 1", null, null, ClientType.PULSAR);
        orchestrator.process(serialize(req), contextFor(queryId));

        RegistryEntry stored = readRegistryEntry("persistent://public/in/trades");
        assertThat(stored.getActiveQueryIds()).containsExactlyInAnyOrder("q-prior", queryId);
    }

    @Test
    void unregisterRemovesQueryIdFromActiveSet() throws Exception {
        // Seed: one t: entry with two queryIds active.
        String controlTopic = "persistent://public/filtered/ctrl-x";
        RegistryEntry seeded = new RegistryEntry(
                controlTopic, "selector-filter-x",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-survivor")
                .withQueryId("q-leaving");
        seedRegistryEntry("persistent://public/in/trades", seeded);

        // Seed q: entry for q-leaving with the new registryKeys field.
        seedQueryEntry("q-leaving", new QueryEntry(
                "persistent://public/filtered/filtered-x",
                List.of(controlTopic),
                List.of("persistent://public/in/trades")));
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.unregister()), contextFor("q-leaving"));

        RegistryEntry afterUnregister = readRegistryEntry("persistent://public/in/trades");
        assertThat(afterUnregister.getActiveQueryIds()).containsExactly("q-survivor");
        assertThat(afterUnregister.getIdleSince()).isNull(); // still active
    }

    @Test
    void unregisterEmptiesActiveSetAndStampsIdleSince() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T20:00:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        String controlTopic = "persistent://public/filtered/ctrl-x";
        RegistryEntry seeded = new RegistryEntry(
                controlTopic, "selector-filter-x",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-1");
        seedRegistryEntry("persistent://public/in/trades", seeded);
        seedQueryEntry("q-1", new QueryEntry(
                "persistent://public/filtered/filtered-x",
                List.of(controlTopic),
                List.of("persistent://public/in/trades")));
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.unregister()), contextFor("q-1"));

        RegistryEntry after = readRegistryEntry("persistent://public/in/trades");
        assertThat(after.getActiveQueryIds()).isEmpty();
        assertThat(after.getIdleSince()).isEqualTo("2026-05-12T20:00:00Z");
    }

    @Test
    void unregisterOnLegacyQueryEntrySkipsBookkeeping() throws Exception {
        // Seed a t: entry that DOES have GC bookkeeping (activeQueryIds = {q-1}).
        String controlTopic = "persistent://public/filtered/ctrl-x";
        RegistryEntry seeded = new RegistryEntry(
                controlTopic, "selector-filter-x",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-1");
        seedRegistryEntry("persistent://public/in/trades", seeded);

        // Seed a LEGACY q: entry (no registryKeys field) — v1-era state.
        seedQueryEntry("q-1", new QueryEntry(
                "persistent://public/filtered/filtered-x",
                List.of(controlTopic)));  // legacy 2-arg constructor
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.unregister()), contextFor("q-1"));

        // Tombstone WAS published (existing behavior unchanged).
        List<KV> pubs = controlPublishes.get(controlTopic);
        assertThat(pubs).hasSize(1);
        assertThat(pubs.get(0).value()).isNull();

        // But t: entry's activeQueryIds is UNCHANGED — bookkeeping was skipped.
        // (This filter just won't be GC-eligible until churned.)
        RegistryEntry after = readRegistryEntry("persistent://public/in/trades");
        assertThat(after.getActiveQueryIds()).containsExactly("q-1");
    }

    // ---------------------------------------------------------------- HEARTBEAT (Phase 2)

    @Test
    void registerStampsLastSeenOnQueryEntry() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T22:00:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                "region = 'US'", null, null, ClientType.PULSAR);

        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        QueryEntry stored = readQueryEntry(queryId);
        assertThat(stored.getLastSeen()).isEqualTo("2026-05-12T22:00:00Z");
    }

    @Test
    void modifyTouchesLastSeen() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T22:05:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        String queryId = UUID.randomUUID().toString();
        String controlTopic = "persistent://public/filtered/ctrl-x";
        // Seed with an OLDER lastSeen — MODIFY must overwrite it.
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-abc",
                List.of(controlTopic),
                List.of("persistent://public/in/trades"),
                "2026-05-12T22:00:00Z"));
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.modify("region IN ('US','UK')")),
                contextFor(queryId));

        QueryEntry stored = readQueryEntry(queryId);
        assertThat(stored.getLastSeen()).isEqualTo("2026-05-12T22:05:00Z");
    }

    // ---------------------------------------------------------------- selector persistence (reconcile)

    @Test
    void registerPersistsNormalizedSelectorOnQueryEntry() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                "region = 'US'", null, null, ClientType.PULSAR);
        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        assertThat(readQueryEntry(queryId).getSelector()).isEqualTo("region = 'US'");
    }

    @Test
    void registerWithNullSelectorPersistsIdentity() throws Exception {
        String queryId = UUID.randomUUID().toString();
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-abc",
                null, null, null, ClientType.PULSAR);
        RegistryEntry spawned = new RegistryEntry(
                "persistent://public/filtered/ctrl-x", "selector-filter-x",
                null, null, ClientType.PULSAR, Instant.now().toString());
        when(spawner.spawn(any(), any(), any())).thenReturn(spawned);
        stubControlProducer(spawned.getControlTopic());

        orchestrator.process(serialize(req), contextFor(queryId));

        // null/blank user selector normalizes to the JMS identity so reconcile can replay it.
        assertThat(readQueryEntry(queryId).getSelector())
                .isEqualTo(OrchestratorFunction.IDENTITY_SELECTOR);
    }

    @Test
    void modifyUpdatesPersistedSelector() throws Exception {
        String queryId = UUID.randomUUID().toString();
        String controlTopic = "persistent://public/filtered/ctrl-x";
        seedQueryEntry(queryId, new QueryEntry(
                "persistent://public/filtered/filtered-abc",
                List.of(controlTopic),
                List.of("persistent://public/in/trades"),
                "2026-05-12T22:00:00Z",
                "region = 'US'"));
        stubControlProducer(controlTopic);

        orchestrator.process(serialize(FilterRequest.modify("region IN ('US','UK')")),
                contextFor(queryId));

        assertThat(readQueryEntry(queryId).getSelector()).isEqualTo("region IN ('US','UK')");
    }

    @Test
    void heartbeatTouchesLastSeenOnAllNamedQueryEntries() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T22:10:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        // Seed three q: entries with older lastSeens.
        seedQueryEntry("q-1", new QueryEntry(
                "persistent://public/filtered/filtered-1",
                List.of("persistent://public/filtered/ctrl-1"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z"));
        seedQueryEntry("q-2", new QueryEntry(
                "persistent://public/filtered/filtered-2",
                List.of("persistent://public/filtered/ctrl-2"),
                List.of("persistent://public/in/b"),
                "2026-05-12T21:59:00Z"));
        seedQueryEntry("q-3", new QueryEntry(
                "persistent://public/filtered/filtered-3",
                List.of("persistent://public/filtered/ctrl-3"),
                List.of("persistent://public/in/c"),
                "2026-05-12T21:58:00Z"));

        // Heartbeat covers q-1 and q-3 but NOT q-2.
        FilterRequest hb = FilterRequest.heartbeat(List.of("q-1", "q-3"));
        // The orchestrator does not require a meaningful message key for HEARTBEAT —
        // any non-blank value is fine, and a blank/null value is also accepted.
        orchestrator.process(serialize(hb), contextFor("heartbeat-key"));

        assertThat(readQueryEntry("q-1").getLastSeen()).isEqualTo("2026-05-12T22:10:00Z");
        assertThat(readQueryEntry("q-3").getLastSeen()).isEqualTo("2026-05-12T22:10:00Z");
        // q-2 was NOT named in the heartbeat → its lastSeen is preserved.
        assertThat(readQueryEntry("q-2").getLastSeen()).isEqualTo("2026-05-12T21:59:00Z");

        // Heartbeat publishes nothing on the control topics.
        assertThat(controlPublishes).isEmpty();
    }

    @Test
    void heartbeatForUnknownQueryIdIsSilentlyIgnored() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T22:15:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        seedQueryEntry("q-known", new QueryEntry(
                "persistent://public/filtered/filtered-1",
                List.of("persistent://public/filtered/ctrl-1"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z"));

        // Mix of known + unknown queryIds in a single heartbeat — must not throw and must touch
        // the known one.
        FilterRequest hb = FilterRequest.heartbeat(List.of("q-known", "q-unknown", "q-also-unknown"));
        orchestrator.process(serialize(hb), contextFor("heartbeat-key"));

        assertThat(readQueryEntry("q-known").getLastSeen()).isEqualTo("2026-05-12T22:15:00Z");
        assertThat(readQueryEntry("q-unknown")).isNull();
        assertThat(readQueryEntry("q-also-unknown")).isNull();
    }

    @Test
    void heartbeatAcceptsBlankMessageKey() throws Exception {
        // HEARTBEAT carries queryIds in the body; the key on the wire is arbitrary. Other
        // commands still require a non-blank key.
        Context ctx = mock(Context.class);
        @SuppressWarnings("unchecked")
        Record<byte[]> rec = (Record<byte[]>) mock(Record.class);
        when(rec.getKey()).thenReturn(Optional.empty());
        when(ctx.getCurrentRecord()).thenAnswer(inv -> rec);

        seedQueryEntry("q-1", new QueryEntry(
                "persistent://public/filtered/filtered-1",
                List.of("persistent://public/filtered/ctrl-1"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z"));

        orchestrator.process(
                serialize(FilterRequest.heartbeat(List.of("q-1"))),
                ctx);

        // No exception. q-1's lastSeen was touched.
        assertThat(readQueryEntry("q-1").getLastSeen()).isNotEqualTo("2026-05-12T22:00:00Z");
    }

    @Test
    void multiInputUnregisterDecrementsEachRegistryEntry() throws Exception {
        java.time.Clock fixed = java.time.Clock.fixed(
                Instant.parse("2026-05-12T21:30:00Z"), java.time.ZoneOffset.UTC);
        RegistryStore registry = new RegistryStore(stateView, stateProducer);
        orchestrator = new OrchestratorFunction(config, registry, spawner, pulsarClient,
                controlProducers, fixed);

        String ctrlA = "persistent://public/filtered/ctrl-a";
        String ctrlB = "persistent://public/filtered/ctrl-b";

        RegistryEntry entryA = new RegistryEntry(ctrlA, "selector-filter-a",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-1");
        RegistryEntry entryB = new RegistryEntry(ctrlB, "selector-filter-b",
                null, null, ClientType.PULSAR, "2026-05-10T00:00:00Z")
                .withQueryId("q-1");
        seedRegistryEntry("persistent://public/in/a", entryA);
        seedRegistryEntry("persistent://public/in/b", entryB);
        seedQueryEntry("q-1", new QueryEntry(
                "persistent://public/filtered/filtered-multi",
                List.of(ctrlA, ctrlB),
                List.of("persistent://public/in/a", "persistent://public/in/b")));
        stubControlProducer(ctrlA);
        stubControlProducer(ctrlB);

        orchestrator.process(serialize(FilterRequest.unregister()), contextFor("q-1"));

        // Both entries lost the queryId and became idle at the same instant.
        RegistryEntry afterA = readRegistryEntry("persistent://public/in/a");
        RegistryEntry afterB = readRegistryEntry("persistent://public/in/b");
        assertThat(afterA.getActiveQueryIds()).isEmpty();
        assertThat(afterA.getIdleSince()).isEqualTo("2026-05-12T21:30:00Z");
        assertThat(afterB.getActiveQueryIds()).isEmpty();
        assertThat(afterB.getIdleSince()).isEqualTo("2026-05-12T21:30:00Z");
    }

    // ---------------------------------------------------------------- Sweeper metric callback wiring (deferred-flush)

    @Test
    void recordSweeperMetricBeforeProcessAccumulatesQuietly() {
        // Cold-start: the sweeper scheduler can fire before any traffic arrives. Counters land in
        // the pending map; the next process() invocation flushes them. Reaching this line is
        // the assertion (no exception).
        orchestrator.recordSweeperMetric("orchestrator.gc.candidates_evaluated", 1.0);
        orchestrator.recordSweeperMetric("orchestrator.gc.functions_deleted", 1.0);
    }

    @Test
    void nextProcessInvocationFlushesPendingSweeperMetrics() throws Exception {
        orchestrator.recordSweeperMetric("orchestrator.gc.functions_deleted", 1.0);
        orchestrator.recordSweeperMetric("orchestrator.gc.candidates_evaluated", 3.0);

        Context ctx = contextFor("q-anything");
        orchestrator.process(serialize(FilterRequest.heartbeat(List.of("q-unknown-1"))), ctx);

        verify(ctx).recordMetric("orchestrator.gc.functions_deleted", 1.0);
        verify(ctx).recordMetric("orchestrator.gc.candidates_evaluated", 3.0);
    }

    @Test
    void pendingSweeperMetricsAreDrainedOncePerProcess() throws Exception {
        orchestrator.recordSweeperMetric("orchestrator.gc.functions_deleted", 1.0);

        Context ctx1 = contextFor("q-anything");
        Context ctx2 = contextFor("q-anything");
        byte[] hb = serialize(FilterRequest.heartbeat(List.of("q-unknown-1")));

        orchestrator.process(hb, ctx1);
        orchestrator.process(hb, ctx2);

        // First process() consumed the pending counter; second sees an empty map.
        verify(ctx1).recordMetric("orchestrator.gc.functions_deleted", 1.0);
        verify(ctx2, never()).recordMetric(eq("orchestrator.gc.functions_deleted"), any(Double.class));
    }

    @Test
    void multipleSweeperMetricCallsBeforeFlushAccumulateAsSum() throws Exception {
        orchestrator.recordSweeperMetric("orchestrator.gc.candidates_evaluated", 1.0);
        orchestrator.recordSweeperMetric("orchestrator.gc.candidates_evaluated", 1.0);
        orchestrator.recordSweeperMetric("orchestrator.gc.candidates_evaluated", 3.0);

        Context ctx = contextFor("q-anything");
        orchestrator.process(serialize(FilterRequest.heartbeat(List.of("q-unknown-1"))), ctx);

        // Three sweep ticks worth of increments arrive on the next process() as a single summed
        // record — Pulsar's per-instance metric counter sums values across recordMetric calls
        // anyway, so 1+1+3=5 in one call is equivalent to three separate calls.
        verify(ctx).recordMetric("orchestrator.gc.candidates_evaluated", 5.0);
    }

    @Test
    void flushSwallowsExceptionFromOneRecordMetricCall() throws Exception {
        Context ctx = contextFor("q-anything");
        org.mockito.Mockito.doThrow(new RuntimeException("simulated"))
                .when(ctx).recordMetric(eq("orchestrator.gc.errors"), any(Double.class));

        orchestrator.recordSweeperMetric("orchestrator.gc.errors", 1.0);
        orchestrator.recordSweeperMetric("orchestrator.gc.functions_deleted", 2.0);

        // process() must complete normally even though one recordMetric call throws — losing a
        // single counter must not poison a sweep cycle.
        orchestrator.process(serialize(FilterRequest.heartbeat(List.of("q-unknown-1"))), ctx);

        // The non-failing counter still landed.
        verify(ctx).recordMetric("orchestrator.gc.functions_deleted", 2.0);
    }

    @Test
    void canonicalMetricNamesRoundTripThroughDeferredFlush() throws Exception {
        // The exact public surface of metric names is locked by the todo and consumed by
        // dashboards. Drive every one of the seven through the callback, flush once, and assert
        // that each string reaches Context.recordMetric — guards against any future rename.
        List<String> canonical = List.of(
                "orchestrator.gc.candidates_evaluated",
                "orchestrator.gc.functions_deleted",
                "orchestrator.gc.skipped_recent_register",
                "orchestrator.gc.errors",
                "orchestrator.heartbeats.synthesized",
                "orchestrator.heartbeats.skipped_recent",
                "orchestrator.heartbeats.errors");
        for (String name : canonical) {
            orchestrator.recordSweeperMetric(name, 1.0);
        }

        Context ctx = contextFor("q-anything");
        orchestrator.process(serialize(FilterRequest.heartbeat(List.of("q-unknown-1"))), ctx);

        for (String name : canonical) {
            verify(ctx).recordMetric(name, 1.0);
        }
    }
}
