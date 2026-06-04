package io.streamnative.filter.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.Command;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.ValueSchemaInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SessionTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private PulsarClient client;
    private PulsarAdmin admin;
    private Topics topics;
    private TopicPolicies topicPolicies;
    private ProducerBuilder<byte[]> controlProducerBuilder;
    private Producer<byte[]> controlProducer;
    private TypedMessageBuilder<byte[]> controlMsgBuilder;
    private ConsumerBuilder<byte[]> consumerBuilder;
    private Consumer<byte[]> consumer;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
        client = mock(PulsarClient.class);
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        topicPolicies = mock(TopicPolicies.class);
        controlProducerBuilder = (ProducerBuilder<byte[]>) mock(ProducerBuilder.class);
        controlProducer = (Producer<byte[]>) mock(Producer.class);
        controlMsgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        consumerBuilder = (ConsumerBuilder<byte[]>) mock(ConsumerBuilder.class);
        consumer = (Consumer<byte[]>) mock(Consumer.class);

        when(admin.topics()).thenReturn(topics);
        when(admin.topicPolicies()).thenReturn(topicPolicies);

        when(client.newProducer(any(Schema.class))).thenReturn(controlProducerBuilder);
        when(controlProducerBuilder.topic(anyString())).thenReturn(controlProducerBuilder);
        when(controlProducerBuilder.producerName(anyString())).thenReturn(controlProducerBuilder);
        when(controlProducerBuilder.enableBatching(anyBoolean())).thenReturn(controlProducerBuilder);
        when(controlProducerBuilder.create()).thenReturn(controlProducer);

        when(controlProducer.newMessage()).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.key(anyString())).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.value(any())).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.value(isNull())).thenReturn(controlMsgBuilder);

        when(client.newConsumer(any(Schema.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionInitialPosition(any(SubscriptionInitialPosition.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscribe()).thenReturn(consumer);
    }

    private Session newSession(PrivateTopicPolicy policy) throws Exception {
        // Existing tests are deterministic about wire publishes. Disable the heartbeat scheduler
        // so background ticks cannot contaminate verify() assertions; heartbeat behavior has its
        // own focused tests below.
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/orchestrator/filter-requests")
                .privateTopicTenant("public")
                .privateTopicNamespace("filtered")
                .privateTopicPolicy(policy)
                .disableHeartbeat()
                .build();
    }

    private static FilterControl asControl(Consumer<byte[]> c) {
        assertThat(c).isInstanceOf(FilterControl.class);
        return (FilterControl) c;
    }

    /** Capture the most recent published payload and parse it as a FilterRequest. */
    private FilterRequest capturePublished() throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder).value(payload.capture());
        return MAPPER.readValue(payload.getValue(), FilterRequest.class);
    }

    private List<FilterRequest> captureAllPublished(int expectedCount) throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder, times(expectedCount)).value(payload.capture());
        return payload.getAllValues().stream()
                .map(b -> {
                    try { return MAPPER.readValue(b, FilterRequest.class); }
                    catch (Exception e) { throw new RuntimeException(e); }
                }).toList();
    }

    // ---------------------------------------------------------------- Subscribe path

    @Test
    void newConsumerReturnsConsumerThatIsAlsoFilterControl() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();
        assertThat(c).isInstanceOf(FilterControl.class);
    }

    @Test
    void subscribeCreatesPrivateTopicAndAppliesPolicies() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();
        FilterControl fc = asControl(c);

        assertThat(fc.privateTopic()).startsWith("persistent://public/filtered/filtered-");
        verify(topics).createNonPartitionedTopic(fc.privateTopic());
        verify(topicPolicies).setMessageTTL(fc.privateTopic(), 300);
        verify(topicPolicies).setRetention(eq(fc.privateTopic()), any(RetentionPolicies.class));
        verify(topicPolicies).setInactiveTopicPolicies(eq(fc.privateTopic()), any(InactiveTopicPolicies.class));
    }

    @Test
    void subscribePublishesRegisterFilterRequest() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();
        FilterControl fc = asControl(c);

        verify(controlMsgBuilder).key(fc.queryId());
        FilterRequest req = capturePublished();
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(req.getInputTopics()).containsExactly("persistent://public/in/trades");
        assertThat(req.getTopicsPattern()).isNull();
        assertThat(req.getOutputTopic()).isEqualTo(fc.privateTopic());
        // Raw user selector — NO __topic__ composition (the orchestrator routes by input topic).
        assertThat(req.getSelector()).isEqualTo("region = 'US'");
        assertThat(req.getClientType()).isEqualTo(ClientType.PULSAR);
        assertThat(req.getKeyDeserializer()).isNull();
        assertThat(req.getValueDeserializer()).isNull();
        // No-schema path forwards no value schema — wire-identical to the pre-schema format.
        assertThat(req.getValueSchema()).isNull();
    }

    @Test
    void subscribeUsesPrivateTopicAndDefaultSettings() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();
        FilterControl fc = asControl(c);

        verify(consumerBuilder).topic(fc.privateTopic());
        verify(consumerBuilder).subscriptionType(SubscriptionType.Exclusive);
        verify(consumerBuilder).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        verify(consumerBuilder).subscribe();
    }

    @Test
    void multipleSourceTopicsLandInInputTopicsArray() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topics(List.of("persistent://public/in/a", "persistent://public/in/b"))
                .selector("region = 'US'")
                .subscribe();
        FilterControl fc = asControl(c);

        FilterRequest req = capturePublished();
        assertThat(req.getInputTopics()).containsExactly(
                "persistent://public/in/a", "persistent://public/in/b");
        assertThat(req.getSelector()).isEqualTo("region = 'US'");
        assertThat(fc.sourceTopics()).containsExactly(
                "persistent://public/in/a", "persistent://public/in/b");
    }

    @Test
    void nullSelectorIsPassedThroughAsNull() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        session.newConsumer().topic("persistent://public/in/trades").subscribe();

        FilterRequest req = capturePublished();
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(req.getSelector()).isNull();
        assertThat(req.getInputTopics()).containsExactly("persistent://public/in/trades");
    }

    // ---------------------------------------------------------------- Close path (UNREGISTER)

    @Test
    void closePublishesUnregisterAndDeletesPrivateTopic() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();
        FilterControl fc = asControl(c);

        c.close();

        verify(consumer).close();
        // Two publishes total: REGISTER on subscribe, UNREGISTER on close. Verify the second.
        List<FilterRequest> all = captureAllPublished(2);
        assertThat(all.get(0).getCommand()).isEqualTo(Command.REGISTER);
        assertThat(all.get(1).getCommand()).isEqualTo(Command.UNREGISTER);
        verify(topics).delete(fc.privateTopic(), true);
    }

    @Test
    void closeWithDeleteOnCloseFalseStillPublishesUnregister() throws Exception {
        PrivateTopicPolicy policy = PrivateTopicPolicy.builder().deleteOnClose(false).build();
        Session session = newSession(policy);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();

        c.close();

        List<FilterRequest> all = captureAllPublished(2);
        assertThat(all.get(1).getCommand()).isEqualTo(Command.UNREGISTER);
        verify(topics, never()).delete(anyString(), anyBoolean());
    }

    @Test
    void closeIsIdempotent() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector("region = 'US'")
                .subscribe();

        c.close();
        c.close();
        verify(consumer, times(1)).close();
        // Two publishes total even after a redundant close: REGISTER + ONE UNREGISTER.
        captureAllPublished(2);
        verify(topics, times(1)).delete(anyString(), anyBoolean());
    }

    // ---------------------------------------------------------------- Policy-controlled flows

    @Test
    void applyPerTopicPoliciesFalseSkipsAdminCalls() throws Exception {
        PrivateTopicPolicy policy = PrivateTopicPolicy.builder().applyPerTopicPolicies(false).build();
        Session session = newSession(policy);

        session.newConsumer().topic("persistent://public/in/trades").selector("region = 'US'").subscribe();

        verify(topics, never()).createNonPartitionedTopic(anyString());
        verify(topicPolicies, never()).setMessageTTL(anyString(), anyInt());
        verify(topicPolicies, never()).setRetention(anyString(), any(RetentionPolicies.class));
    }

    @Test
    void sessionCloseClosesProducerOnly_whenHandlesNotOwned() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        session.close();
        verify(controlProducer).close();
        verify(client, never()).close();
        verify(admin, never()).close();
    }

    // ---------------------------------------------------------------- Validation

    @Test
    void subscribeWithMalformedSelectorThrowsIllegalArgumentBeforePublishing() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> session.newConsumer()
                        .topic("persistent://public/in/trades")
                        .selector("THIS IS NOT VALID JMS @@##")
                        .subscribe())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid selector syntax");

        // No private topic created, no control-topic publish, no subscribe.
        verify(topics, never()).createNonPartitionedTopic(anyString());
        verify(controlMsgBuilder, never()).value(any(byte[].class));
        verify(consumerBuilder, never()).subscribe();
    }

    // ---------------------------------------------------------------- Public API for Kafka SDK

    @Test
    void registerPrivateQueryWithSerializersAndClientTypeKafka() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        PrivateQuery pq = session.registerPrivateQuery(
                List.of("persistent://public/in/trades"),
                "region = 'US'",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                ClientType.KAFKA);

        verify(controlMsgBuilder).key(pq.queryId());
        FilterRequest req = capturePublished();
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(req.getClientType()).isEqualTo(ClientType.KAFKA);
        assertThat(req.getKeyDeserializer())
                .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
        assertThat(req.getValueDeserializer())
                .isEqualTo("org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    // ---------------------------------------------------------------- Typed value schema (Pulsar)

    /** Minimal POJO for building a real {@code Schema.AVRO} / {@code Schema.JSON} in tests. */
    public static class Trade {
        public String sym;
        public double price;
    }

    @Test
    void typedValueSchemaForwardsFullSchemaInfo() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        Schema<Trade> schema = Schema.AVRO(Trade.class);

        session.newConsumer(schema)
                .topic("persistent://public/in/trades")
                .selector("price > 100")
                .subscribe();

        FilterRequest req = capturePublished();
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(req.getClientType()).isEqualTo(ClientType.PULSAR);
        // Pulsar has no key schema — key deserializer is always null on this path.
        assertThat(req.getKeyDeserializer()).isNull();
        // valueDeserializer mirrors the schema name (parity with Kafka mismatch detection).
        assertThat(req.getValueDeserializer()).isEqualTo(schema.getSchemaInfo().getName());

        ValueSchemaInfo vs = req.getValueSchema();
        assertThat(vs).isNotNull();
        assertThat(vs.getName()).isEqualTo(schema.getSchemaInfo().getName());
        assertThat(vs.getType()).isEqualTo("AVRO");
        // The full definition (base64 of the .avsc) travels — a name alone can't decode Avro.
        assertThat(vs.getDefinition()).isNotBlank();
    }

    @Test
    void typedStringSchemaForwardsTypeWithoutDefinition() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);

        session.newConsumer(Schema.STRING)
                .topic("persistent://public/in/trades")
                .subscribe();

        ValueSchemaInfo vs = capturePublished().getValueSchema();
        assertThat(vs).isNotNull();
        assertThat(vs.getType()).isEqualTo("STRING");
        // STRING/BYTES carry no structured definition.
        assertThat(vs.getDefinition()).isNull();
    }

    @Test
    void newConsumerRejectsNonNativeProtobuf() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);

        // Mockito cannot mock the Schema interface, but it can mock SchemaInfo; wrap a SchemaInfo
        // reporting the non-native PROTOBUF type in a minimal Schema test-double.
        SchemaInfo si = mock(SchemaInfo.class);
        when(si.getType()).thenReturn(SchemaType.PROTOBUF);
        Schema<Object> schema = new ProtobufStubSchema(si);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> session.newConsumer(schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PROTOBUF_NATIVE");

        // Guard fires before any control-topic publish.
        verify(controlMsgBuilder, never()).value(any(byte[].class));
    }

    /** Minimal {@link Schema} test-double exposing a chosen {@link SchemaInfo}; only used above. */
    private static final class ProtobufStubSchema implements Schema<Object> {
        private final SchemaInfo info;
        ProtobufStubSchema(SchemaInfo info) { this.info = info; }
        @Override public byte[] encode(Object message) { return new byte[0]; }
        @Override public SchemaInfo getSchemaInfo() { return info; }
        @Override public Schema<Object> clone() { return new ProtobufStubSchema(info); }
    }

    // ---------------------------------------------------------------- Heartbeat (Phase 4)

    /**
     * Build a Session with heartbeats enabled but at a long interval so the scheduler does NOT
     * fire during the test. Tests invoke {@code sendHeartbeatQuietly()} directly to drive the
     * publish path deterministically.
     */
    private Session newSessionWithHeartbeats() throws Exception {
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/orchestrator/filter-requests")
                .privateTopicTenant("public")
                .privateTopicNamespace("filtered")
                .privateTopicPolicy(PrivateTopicPolicy.DEFAULT)
                .heartbeatIntervalSeconds(3600L)   // long enough not to fire during the test
                .build();
    }

    @Test
    void heartbeatPublishesAllLiveQueryIds() throws Exception {
        try (Session session = newSessionWithHeartbeats()) {
            // Two registrations leave both queryIds in the heartbeat set.
            PrivateQuery pq1 = session.registerPrivateQuery(
                    List.of("persistent://t/n/a"), "x = 1");
            PrivateQuery pq2 = session.registerPrivateQuery(
                    List.of("persistent://t/n/b"), "x = 2");

            // Drive one heartbeat tick manually.
            session.sendHeartbeatQuietly();

            // Wire shape: capture the last byte[] published — must be a HEARTBEAT containing both
            // queryIds.
            ArgumentCaptor<byte[]> payloads = ArgumentCaptor.forClass(byte[].class);
            verify(controlMsgBuilder, times(3)).value(payloads.capture());  // 2 REGISTER + 1 HB
            byte[] hb = payloads.getAllValues().get(2);
            FilterRequest req = MAPPER.readValue(hb, FilterRequest.class);
            assertThat(req.getCommand()).isEqualTo(Command.HEARTBEAT);
            assertThat(req.getQueryIds()).containsExactlyInAnyOrder(pq1.queryId(), pq2.queryId());
        }
    }

    @Test
    void heartbeatWithEmptyLiveSetPublishesNothing() throws Exception {
        try (Session session = newSessionWithHeartbeats()) {
            // No registrations — heartbeat task must NOT publish.
            session.sendHeartbeatQuietly();
            verify(controlMsgBuilder, never()).value(any(byte[].class));
        }
    }

    @Test
    void heartbeatAfterRetireOmitsRetiredQueryId() throws Exception {
        try (Session session = newSessionWithHeartbeats()) {
            PrivateQuery pq1 = session.registerPrivateQuery(
                    List.of("persistent://t/n/a"), "x = 1");
            PrivateQuery pq2 = session.registerPrivateQuery(
                    List.of("persistent://t/n/b"), "x = 2");
            // Retire pq1.
            session.retirePrivateQuery(pq1);

            session.sendHeartbeatQuietly();

            // Captured payloads: REGISTER pq1, REGISTER pq2, UNREGISTER pq1, HEARTBEAT.
            ArgumentCaptor<byte[]> payloads = ArgumentCaptor.forClass(byte[].class);
            verify(controlMsgBuilder, times(4)).value(payloads.capture());
            byte[] hb = payloads.getAllValues().get(3);
            FilterRequest req = MAPPER.readValue(hb, FilterRequest.class);
            assertThat(req.getCommand()).isEqualTo(Command.HEARTBEAT);
            assertThat(req.getQueryIds()).containsExactly(pq2.queryId());
        }
    }

    @Test
    void disableHeartbeatSkipsScheduler() throws Exception {
        // With heartbeats disabled, no background scheduler thread is created. The proof is that
        // close() returns promptly (no awaitTermination on a missing scheduler) and that the
        // only publish on the wire is the REGISTER. The sendHeartbeatQuietly method itself stays
        // package-visible and would publish if called — but a disabled session never triggers it.
        Session session = newSession(PrivateTopicPolicy.DEFAULT); // helper disables HB
        session.registerPrivateQuery(List.of("persistent://t/n/a"), "x = 1");
        session.close();
        ArgumentCaptor<byte[]> payloads = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder, times(1)).value(payloads.capture());
        FilterRequest req = MAPPER.readValue(payloads.getValue(), FilterRequest.class);
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
    }

    @Test
    void heartbeatPublishKeyIsArbitraryNotAQueryId() throws Exception {
        try (Session session = newSessionWithHeartbeats()) {
            session.registerPrivateQuery(List.of("persistent://t/n/a"), "x = 1");
            session.sendHeartbeatQuietly();

            // Last key() invocation belongs to the HEARTBEAT; assert it starts with "heartbeat-".
            ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
            verify(controlMsgBuilder, times(2)).key(keys.capture());  // REGISTER key + HEARTBEAT key
            String hbKey = keys.getAllValues().get(1);
            assertThat(hbKey).startsWith("heartbeat-");
        }
    }
}
