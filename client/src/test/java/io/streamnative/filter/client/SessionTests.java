package io.streamnative.filter.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.QueryMessage;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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

    private static final ObjectMapper MAPPER = new ObjectMapper();

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
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/control/queries")
                .privateTopicTenant("public")
                .privateTopicNamespace("filtered")
                .privateTopicPolicy(policy)
                .build();
    }

    @Test
    void createConsumerCreatesPrivateTopicAndAppliesPolicies() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);

        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", "region = 'US'");

        assertThat(fc.getPrivateTopic()).startsWith("persistent://public/filtered/filtered-");
        assertThat(fc.getComposedSelector())
                .isEqualTo("__topic__ = 'persistent://public/in/trades' AND (region = 'US')");

        verify(topics).createNonPartitionedTopic(fc.getPrivateTopic());
        verify(topicPolicies).setMessageTTL(fc.getPrivateTopic(), 300);
        verify(topicPolicies).setRetention(eq(fc.getPrivateTopic()), any(RetentionPolicies.class));
        verify(topicPolicies).setInactiveTopicPolicies(eq(fc.getPrivateTopic()), any(InactiveTopicPolicies.class));
    }

    @Test
    void createConsumerPublishesQueryToControlTopic() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);

        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", "region = 'US'");

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(controlMsgBuilder).value(payload.capture());
        verify(controlMsgBuilder).key(fc.getQueryId());
        verify(controlMsgBuilder, times(1)).value(any(byte[].class));

        QueryMessage qm = MAPPER.readValue(payload.getValue(), QueryMessage.class);
        assertThat(qm.getSelector()).isEqualTo(fc.getComposedSelector());
        assertThat(qm.getOutputTopic()).isEqualTo(fc.getPrivateTopic());
    }

    @Test
    void createConsumerSubscribesOnPrivateTopic() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", null);

        verify(consumerBuilder).topic(fc.getPrivateTopic());
        verify(consumerBuilder).subscriptionType(SubscriptionType.Exclusive);
        verify(consumerBuilder).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
        verify(consumerBuilder).subscribe();
    }

    @Test
    void multipleSourceTopicsRenderInClause() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        FilteredConsumer fc = session.createConsumer(
                List.of("persistent://public/in/a", "persistent://public/in/b"), "region = 'US'");

        assertThat(fc.getComposedSelector()).isEqualTo(
                "__topic__ IN ('persistent://public/in/a','persistent://public/in/b') AND (region = 'US')");
    }

    @Test
    void closeFilteredConsumerRetractsQueryAndDeletesPrivateTopic() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", null);

        fc.close();

        verify(consumer).close();
        verify(controlMsgBuilder).value(isNull()); // tombstone
        verify(topics).delete(fc.getPrivateTopic(), true);
    }

    @Test
    void closeWithDeleteOnCloseFalseSkipsDelete() throws Exception {
        PrivateTopicPolicy policy = PrivateTopicPolicy.builder().deleteOnClose(false).build();
        Session session = newSession(policy);
        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", null);

        fc.close();

        verify(controlMsgBuilder).value(isNull()); // tombstone still sent
        verify(topics, never()).delete(anyString(), anyBoolean());
    }

    @Test
    void closeIsIdempotent() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        FilteredConsumer fc = session.createConsumer("persistent://public/in/trades", null);

        fc.close();
        fc.close();
        verify(consumer, times(1)).close();
        verify(topics, times(1)).delete(anyString(), anyBoolean());
    }

    @Test
    void applyPerTopicPoliciesFalseSkipsAdminCalls() throws Exception {
        PrivateTopicPolicy policy = PrivateTopicPolicy.builder().applyPerTopicPolicies(false).build();
        Session session = newSession(policy);

        session.createConsumer("persistent://public/in/trades", null);

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

    @Test
    void selectorEscapesSingleQuotesInSourceTopic() throws Exception {
        Session session = newSession(PrivateTopicPolicy.DEFAULT);
        FilteredConsumer fc = session.createConsumer("topic-with-'-quote", null);
        assertThat(fc.getComposedSelector()).isEqualTo("__topic__ = 'topic-with-''-quote'");
    }
}
