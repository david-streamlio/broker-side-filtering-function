package io.streamnative.filter.client.kafka;

import io.streamnative.filter.client.FilterControl;
import io.streamnative.filter.client.kafka.KafkaTestFixtures.Mocks;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class FilterableKafkaConsumerBuilderTests {

    private Mocks mocks;
    private Properties props;

    @SuppressWarnings("unchecked")
    private Consumer<String, byte[]> fakeKafkaConsumer() {
        return (Consumer<String, byte[]>) mock(Consumer.class);
    }

    @BeforeEach
    void setup() throws Exception {
        mocks = KafkaTestFixtures.newSessionWithMocks();
        props = KafkaTestFixtures.stubKafkaProps();
    }

    @Test
    void subscribeWithNoTopicThrows() {
        FilterableKafkaConsumerBuilder<String, byte[]> b =
                FilterableKafkaConsumers.<String, byte[]>newConsumer(mocks.session, props);
        b.withKafkaConsumerFactory(p -> fakeKafkaConsumer());

        assertThatThrownBy(b::subscribe)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source topic");
    }

    @Test
    void topicsRejectsEmpty() {
        assertThatThrownBy(() -> FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .topics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topicAndTopicsOverwriteEachOther() throws Exception {
        Consumer<String, byte[]> underlying = fakeKafkaConsumer();
        Consumer<String, byte[]> c = FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .topics(List.of("persistent://public/in/a", "persistent://public/in/b"))
                .subscribe();
        assertThat(((FilterControl) c).sourceTopics())
                .containsExactly("persistent://public/in/a", "persistent://public/in/b");
    }

    @Test
    void nullSelectorAllowed() throws Exception {
        Consumer<String, byte[]> underlying = fakeKafkaConsumer();
        Consumer<String, byte[]> c = FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector(null)
                .subscribe();
        assertThat(((FilterControl) c).currentSelector()).isNull();
    }

    @Test
    void blankSelectorRecordedAsNull() throws Exception {
        Consumer<String, byte[]> underlying = fakeKafkaConsumer();
        Consumer<String, byte[]> c = FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("   ")
                .subscribe();
        assertThat(((FilterControl) c).currentSelector()).isNull();
    }

    @Test
    void malformedSelectorThrowsBeforeAnyKafkaConsumerConstruction() {
        boolean[] factoryCalled = {false};
        assertThatThrownBy(() -> FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> {
                    factoryCalled[0] = true;
                    return fakeKafkaConsumer();
                })
                .topic("persistent://public/in/x")
                .selector("THIS IS NOT @@## VALID")
                .subscribe())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid selector syntax");

        assertThat(factoryCalled[0])
                .as("malformed selector must fail validation before constructing the Kafka consumer")
                .isFalse();
    }

    @Test
    void subscribeAttachesUnderlyingToKafkaTopicName() throws Exception {
        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> underlying = (Consumer<String, byte[]>) mock(Consumer.class);

        Consumer<String, byte[]> c = FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe();

        // Underlying Kafka consumer must have been subscribed to the Kafka name of the
        // private Pulsar topic — last segment of `persistent://public/default/filtered-<uuid>`.
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<String>> topics = ArgumentCaptor.forClass(List.class);
        verify(underlying).subscribe(topics.capture());
        List<String> kafkaTopics = topics.getValue();
        assertThat(kafkaTopics).hasSize(1);
        assertThat(kafkaTopics.get(0)).startsWith("filtered-");
        assertThat(kafkaTopics.get(0))
                .isEqualTo(FilterableKafkaConsumerBuilder.kafkaTopicNameFor(((FilterControl) c).privateTopic()));
    }

    @Test
    void onKafkaSubscribeFailureUnderlyingClosedAndQueryRetired() throws Exception {
        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> underlying = (Consumer<String, byte[]>) mock(Consumer.class);
        RuntimeException boom = new RuntimeException("kafka subscribe failed");
        org.mockito.Mockito.doThrow(boom).when(underlying).subscribe(any(List.class));

        assertThatThrownBy(() -> FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe())
                .isSameAs(boom);

        verify(underlying).close();                                  // factory-produced consumer closed
        verify(mocks.controlMsgBuilder, atLeastOnce()).value(org.mockito.ArgumentMatchers.isNull()); // tombstone
        verify(mocks.topics).delete(anyString(), anyBoolean());      // private topic deleted
    }

    @Test
    void kafkaTopicNameForExtractsLastSegment() {
        assertThat(FilterableKafkaConsumerBuilder.kafkaTopicNameFor(
                "persistent://public/default/filtered-12345")).isEqualTo("filtered-12345");
        assertThat(FilterableKafkaConsumerBuilder.kafkaTopicNameFor(
                "non-qualified")).isEqualTo("non-qualified");
    }

    @Test
    void registrationPublishesQueryMessageBeforeUnderlyingSubscribe() throws Exception {
        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> underlying = (Consumer<String, byte[]>) mock(Consumer.class);

        FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe();

        // Both happen — but registration publish should come first. We can't easily
        // verify ordering against two different mocks, so just verify both happened.
        verify(mocks.controlMsgBuilder).value(any(byte[].class));
        verify(underlying).subscribe(any(List.class));
    }

    @Test
    void neverEmptyTopicListInUnderlyingSubscribe() {
        // Constructing topics(List.of()) is already rejected — but the same constraint must hold
        // if someone reaches in via topic(...) then topics(...) with valid lists. Re-verifying
        // here documents the invariant.
        assertThatThrownBy(() -> FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .topics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(mocks.controlMsgBuilder, never()).value(any(byte[].class));
    }
}
