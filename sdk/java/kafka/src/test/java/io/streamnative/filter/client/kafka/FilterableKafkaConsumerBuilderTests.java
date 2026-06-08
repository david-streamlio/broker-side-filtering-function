/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.streamnative.filter.client.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.client.FilterControl;
import io.streamnative.filter.client.kafka.KafkaTestFixtures.Mocks;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.Command;
import io.streamnative.filter.common.FilterRequest;
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

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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

    private FilterRequest captureLatestPublished() throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder, atLeastOnce()).value(payload.capture());
        return MAPPER.readValue(payload.getAllValues().get(payload.getAllValues().size() - 1),
                FilterRequest.class);
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
    void onKafkaSubscribeFailureUnderlyingClosedAndQueryUnregistered() throws Exception {
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
        // Two publishes: REGISTER from registerPrivateQuery, then UNREGISTER from retirePrivateQuery
        // when the subscribe failure triggered cleanup.
        ArgumentCaptor<byte[]> payloads = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder, atLeastOnce()).value(payloads.capture());
        FilterRequest last = MAPPER.readValue(
                payloads.getAllValues().get(payloads.getAllValues().size() - 1),
                FilterRequest.class);
        assertThat(last.getCommand()).isEqualTo(Command.UNREGISTER);
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
    void registrationPublishesFilterRequestBeforeUnderlyingSubscribe() throws Exception {
        @SuppressWarnings("unchecked")
        Consumer<String, byte[]> underlying = (Consumer<String, byte[]>) mock(Consumer.class);

        FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe();

        verify(mocks.controlMsgBuilder).value(any(byte[].class));
        verify(underlying).subscribe(any(List.class));
    }

    @Test
    void registerPropagatesKafkaSerializersAndClientType() throws Exception {
        Consumer<String, byte[]> underlying = fakeKafkaConsumer();
        FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe();

        FilterRequest req = captureLatestPublished();
        assertThat(req.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(req.getClientType()).isEqualTo(ClientType.KAFKA);
        // KafkaTestFixtures.stubKafkaProps sets these.
        assertThat(req.getKeyDeserializer())
                .isEqualTo("org.apache.kafka.common.serialization.StringDeserializer");
        assertThat(req.getValueDeserializer())
                .isEqualTo("org.apache.kafka.common.serialization.ByteArrayDeserializer");
    }

    @Test
    void registerHandlesMissingSerializerProperties() throws Exception {
        // Operator may pass props without key/value deserializer (unusual but valid for testing).
        Properties bareProps = new Properties();
        bareProps.put("bootstrap.servers", "stub:9094");
        bareProps.put("group.id", "stub-group");

        Consumer<String, byte[]> underlying = fakeKafkaConsumer();
        FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, bareProps)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/x")
                .selector("region = 'US'")
                .subscribe();

        FilterRequest req = captureLatestPublished();
        assertThat(req.getClientType()).isEqualTo(ClientType.KAFKA);
        assertThat(req.getKeyDeserializer()).isNull();
        assertThat(req.getValueDeserializer()).isNull();
    }

    @Test
    void neverEmptyTopicListInUnderlyingSubscribe() {
        assertThatThrownBy(() -> FilterableKafkaConsumers
                .<String, byte[]>newConsumer(mocks.session, props)
                .topics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        verify(mocks.controlMsgBuilder, never()).value(any(byte[].class));
    }
}
