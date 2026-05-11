package io.streamnative.filter.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.QueryMessage;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class FilteringPulsarConsumerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private PulsarClient client;
    private PulsarAdmin admin;
    private Topics topics;
    private TopicPolicies topicPolicies;
    private Producer<byte[]> controlProducer;
    private TypedMessageBuilder<byte[]> controlMsgBuilder;
    private ConsumerBuilder<byte[]> consumerBuilder;
    private Consumer<byte[]> underlying;
    private Session session;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
        client = mock(PulsarClient.class);
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        topicPolicies = mock(TopicPolicies.class);
        ProducerBuilder<byte[]> producerBuilder = (ProducerBuilder<byte[]>) mock(ProducerBuilder.class);
        controlProducer = (Producer<byte[]>) mock(Producer.class);
        controlMsgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        consumerBuilder = (ConsumerBuilder<byte[]>) mock(ConsumerBuilder.class);
        underlying = (Consumer<byte[]>) mock(Consumer.class);

        when(admin.topics()).thenReturn(topics);
        when(admin.topicPolicies()).thenReturn(topicPolicies);

        when(client.newProducer(any(Schema.class))).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.producerName(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(controlProducer);

        when(controlProducer.newMessage()).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.key(anyString())).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.value(any())).thenReturn(controlMsgBuilder);
        when(controlMsgBuilder.value(isNull())).thenReturn(controlMsgBuilder);

        when(client.newConsumer(any(Schema.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionInitialPosition(any(SubscriptionInitialPosition.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscribe()).thenReturn(underlying);

        session = Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/control/queries")
                .privateTopicTenant("public")
                .privateTopicNamespace("filtered")
                .build();
    }

    private Consumer<byte[]> subscribe(String selector) throws Exception {
        return session.newConsumer()
                .topic("persistent://public/in/trades")
                .selector(selector)
                .subscribe();
    }

    // --------------------------------------------------------------- Delegation

    @Test
    void receiveDelegatesToUnderlying() throws Exception {
        @SuppressWarnings("unchecked")
        Message<byte[]> m = mock(Message.class);
        when(underlying.receive()).thenReturn(m);

        Consumer<byte[]> c = subscribe("region = 'US'");
        assertThat(c.receive()).isSameAs(m);
        verify(underlying).receive();
    }

    @Test
    void acknowledgeDelegatesToUnderlying() throws Exception {
        MessageId id = mock(MessageId.class);
        Consumer<byte[]> c = subscribe(null);
        c.acknowledge(id);
        verify(underlying).acknowledge(id);
    }

    @Test
    void negativeAcknowledgeDelegatesToUnderlying() throws Exception {
        MessageId id = mock(MessageId.class);
        Consumer<byte[]> c = subscribe(null);
        c.negativeAcknowledge(id);
        verify(underlying).negativeAcknowledge(id);
    }

    @Test
    void seekDelegatesToUnderlying() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        c.seek(123L);
        verify(underlying).seek(123L);
    }

    @Test
    void pauseResumeDelegate() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        c.pause();
        c.resume();
        verify(underlying).pause();
        verify(underlying).resume();
    }

    @Test
    void receiveWithTimeoutDelegates() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        c.receive(5, TimeUnit.SECONDS);
        verify(underlying).receive(5, TimeUnit.SECONDS);
    }

    @Test
    void getTopicReturnsUnderlyingTopic() throws Exception {
        when(underlying.getTopic()).thenReturn("private-topic");
        Consumer<byte[]> c = subscribe(null);
        assertThat(c.getTopic()).isEqualTo("private-topic");
    }

    // --------------------------------------------------------------- FilterControl

    @Test
    void filterControlInitialStateAfterSubscribe() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        assertThat(fc.currentSelector()).isEqualTo("region = 'US'");
        assertThat(fc.queryId()).isNotBlank();
        assertThat(fc.privateTopic()).startsWith("persistent://public/filtered/filtered-");
        assertThat(fc.sourceTopics()).containsExactly("persistent://public/in/trades");
    }

    @Test
    void filterControlBlankSelectorAtSubscribeRecordedAsNull() throws Exception {
        Consumer<byte[]> c = subscribe("   ");
        assertThat(((FilterControl) c).currentSelector()).isNull();
    }

    @Test
    void setSelectorRepublishesOnSameQueryIdAndComposesTopicConstraint() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        String initialQueryId = fc.queryId();

        // Initial publish from subscribe()
        ArgumentCaptor<byte[]> firstPayload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder).value(firstPayload.capture());
        verify(controlMsgBuilder).key(initialQueryId);
        QueryMessage initialQm = MAPPER.readValue(firstPayload.getValue(), QueryMessage.class);
        assertThat(initialQm.getSelector())
                .isEqualTo("__topic__ = 'persistent://public/in/trades' AND (region = 'US')");

        // setSelector publishes again on SAME queryId
        fc.setSelector("region = 'UK'");
        assertThat(fc.queryId()).isEqualTo(initialQueryId);
        assertThat(fc.currentSelector()).isEqualTo("region = 'UK'");

        ArgumentCaptor<byte[]> secondPayload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder, times(2)).value(secondPayload.capture());
        verify(controlMsgBuilder, times(2)).key(initialQueryId);
        QueryMessage updated = MAPPER.readValue(secondPayload.getValue(), QueryMessage.class);
        assertThat(updated.getSelector())
                .isEqualTo("__topic__ = 'persistent://public/in/trades' AND (region = 'UK')");
        assertThat(updated.getOutputTopic()).isEqualTo(fc.privateTopic());
    }

    @Test
    void setSelectorNullPublishesTopicConstraintOnly() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;

        fc.setSelector(null);
        assertThat(fc.currentSelector()).isNull();

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder, times(2)).value(payload.capture());
        QueryMessage updated = MAPPER.readValue(payload.getAllValues().get(1), QueryMessage.class);
        assertThat(updated.getSelector()).isEqualTo("__topic__ = 'persistent://public/in/trades'");
    }

    @Test
    void setSelectorEmptyTreatedSameAsNull() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        fc.setSelector("   ");
        assertThat(fc.currentSelector()).isNull();
    }

    @Test
    void setSelectorMalformedThrowsAndDoesNotPublish() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;

        // One publish from subscribe()
        verify(controlMsgBuilder, times(1)).value(any(byte[].class));

        assertThatThrownBy(() -> fc.setSelector("THIS IS NOT @@## VALID"))
                .isInstanceOf(IllegalArgumentException.class);

        // Still only the initial publish — failed validation must not republish.
        verify(controlMsgBuilder, times(1)).value(any(byte[].class));
        assertThat(fc.currentSelector()).isEqualTo("region = 'US'"); // unchanged
    }

    @Test
    void setSelectorAfterCloseThrows() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        c.close();

        assertThatThrownBy(() -> fc.setSelector("region = 'UK'"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    // --------------------------------------------------------------- Close semantics

    @Test
    void closeRetractsThenCallsUnderlyingClose() throws Exception {
        Consumer<byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        c.close();

        verify(underlying).close();
        verify(controlMsgBuilder).value(isNull()); // tombstone
        verify(topics).delete(eq(fc.privateTopic()), eq(true));
    }

    @Test
    void closeIsIdempotent() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        c.close();
        c.close();
        verify(underlying, times(1)).close();
        verify(topics, times(1)).delete(anyString(), anyBoolean());
    }

    @Test
    void closeStillRetractsWhenUnderlyingCloseFails() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        FilterControl fc = (FilterControl) c;
        PulsarClientException boom = new PulsarClientException("boom");
        org.mockito.Mockito.doThrow(boom).when(underlying).close();

        assertThatThrownBy(c::close).isSameAs(boom);

        // Tombstone was still sent and topic still deleted, even though delegate.close() failed.
        verify(controlMsgBuilder).value(isNull());
        verify(topics).delete(eq(fc.privateTopic()), eq(true));
    }

    @Test
    void getStatsDelegatesAndIsNotInterceptedByWrapper() throws Exception {
        Consumer<byte[]> c = subscribe(null);
        c.getStats();
        verify(underlying).getStats();
        // No other underlying calls beyond getStats + the close() chain that close-tests cover.
        verifyNoMoreInteractions(underlying);
    }
}
