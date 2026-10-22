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

package io.streamnative.filter.client;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FilterableConsumerBuilderTests {

    @SuppressWarnings("unchecked")
    private Session session(ConsumerBuilder<byte[]>[] consumerBuilderOut,
                            Producer<byte[]>[] controlProducerOut,
                            TypedMessageBuilder<byte[]>[] controlMsgBuilderOut) throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        Topics topics = mock(Topics.class);
        TopicPolicies topicPolicies = mock(TopicPolicies.class);
        ProducerBuilder<byte[]> producerBuilder = (ProducerBuilder<byte[]>) mock(ProducerBuilder.class);
        Producer<byte[]> producer = (Producer<byte[]>) mock(Producer.class);
        TypedMessageBuilder<byte[]> msgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        ConsumerBuilder<byte[]> consumerBuilder = (ConsumerBuilder<byte[]>) mock(ConsumerBuilder.class);
        Consumer<byte[]> consumer = (Consumer<byte[]>) mock(Consumer.class);

        when(admin.topics()).thenReturn(topics);
        when(admin.topicPolicies()).thenReturn(topicPolicies);

        when(client.newProducer(any(Schema.class))).thenReturn(producerBuilder);
        when(producerBuilder.topic(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.producerName(anyString())).thenReturn(producerBuilder);
        when(producerBuilder.enableBatching(anyBoolean())).thenReturn(producerBuilder);
        when(producerBuilder.create()).thenReturn(producer);

        when(producer.newMessage()).thenReturn(msgBuilder);
        when(msgBuilder.key(anyString())).thenReturn(msgBuilder);
        when(msgBuilder.value(any())).thenReturn(msgBuilder);
        when(msgBuilder.value(isNull())).thenReturn(msgBuilder);

        when(client.newConsumer(any(Schema.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.topic(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionName(anyString())).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscriptionInitialPosition(any(SubscriptionInitialPosition.class))).thenReturn(consumerBuilder);
        when(consumerBuilder.subscribe()).thenReturn(consumer);

        consumerBuilderOut[0] = consumerBuilder;
        controlProducerOut[0] = producer;
        controlMsgBuilderOut[0] = msgBuilder;

        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/control/queries")
                .privateTopicTenant("public")
                .privateTopicNamespace("filtered")
                .build();
    }

    @SuppressWarnings("unchecked")
    private final ConsumerBuilder<byte[]>[] consumerBuilderHolder = new ConsumerBuilder[1];
    @SuppressWarnings("unchecked")
    private final Producer<byte[]>[] controlProducerHolder = new Producer[1];
    @SuppressWarnings("unchecked")
    private final TypedMessageBuilder<byte[]>[] controlMsgBuilderHolder = new TypedMessageBuilder[1];

    private Session session;

    @BeforeEach
    void setup() throws Exception {
        session = session(consumerBuilderHolder, controlProducerHolder, controlMsgBuilderHolder);
    }

    @Test
    void subscribeWithNoTopicThrows() {
        assertThatThrownBy(() -> session.newConsumer().subscribe())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("source topic");
    }

    @Test
    void topicAndTopicsOverwriteEachOther() throws Exception {
        Consumer<byte[]> c = session.newConsumer()
                .topic("persistent://public/in/x")
                .topics(List.of("persistent://public/in/a", "persistent://public/in/b"))
                .subscribe();
        assertThat(((FilterControl) c).sourceTopics()).containsExactly(
                "persistent://public/in/a", "persistent://public/in/b");
    }

    @Test
    void topicsThenTopicOverwrites() throws Exception {
        Consumer<byte[]> c = session.newConsumer()
                .topics(List.of("persistent://public/in/a", "persistent://public/in/b"))
                .topic("persistent://public/in/x")
                .subscribe();
        assertThat(((FilterControl) c).sourceTopics())
                .containsExactly("persistent://public/in/x");
    }

    @Test
    void topicsRejectsEmpty() {
        assertThatThrownBy(() -> session.newConsumer().topics(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullAndBlankSelectorsAreAllowed() throws Exception {
        assertThat(session.newConsumer()
                .topic("persistent://public/in/x")
                .selector(null)
                .subscribe()).isInstanceOf(FilterControl.class);
        assertThat(((FilterControl) session.newConsumer()
                .topic("persistent://public/in/x")
                .selector("   ")
                .subscribe()).currentSelector()).isNull();
    }

    @Test
    void malformedSelectorThrowsIllegalArgumentException() {
        assertThatThrownBy(() -> session.newConsumer()
                .topic("persistent://public/in/x")
                .selector("garbage SQL @@##")
                .subscribe())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("garbage SQL @@##");
    }

    @Test
    void defaultSubscriptionTypeAndPositionApplied() throws Exception {
        session.newConsumer().topic("persistent://public/in/x").subscribe();
        verify(consumerBuilderHolder[0]).subscriptionType(SubscriptionType.Exclusive);
        verify(consumerBuilderHolder[0]).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);
    }

    @Test
    void customSubscriptionTypeAndPositionPassedThrough() throws Exception {
        session.newConsumer()
                .topic("persistent://public/in/x")
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe();
        verify(consumerBuilderHolder[0]).subscriptionType(SubscriptionType.Shared);
        verify(consumerBuilderHolder[0]).subscriptionInitialPosition(SubscriptionInitialPosition.Latest);
    }

    @Test
    void customSubscriptionNameUsed() throws Exception {
        session.newConsumer()
                .topic("persistent://public/in/x")
                .subscriptionName("my-custom-sub")
                .subscribe();
        verify(consumerBuilderHolder[0]).subscriptionName("my-custom-sub");
    }

    @Test
    void omittedSubscriptionNameDefaultsToSessionPrefix() throws Exception {
        session.newConsumer().topic("persistent://public/in/x").subscribe();
        ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
        verify(consumerBuilderHolder[0]).subscriptionName(name.capture());
        assertThat(name.getValue()).startsWith("session-");
    }

    @Test
    void blankSubscriptionNameTreatedAsOmitted() throws Exception {
        session.newConsumer()
                .topic("persistent://public/in/x")
                .subscriptionName("   ")
                .subscribe();
        ArgumentCaptor<String> name = ArgumentCaptor.forClass(String.class);
        verify(consumerBuilderHolder[0]).subscriptionName(name.capture());
        assertThat(name.getValue()).startsWith("session-");
    }
}
