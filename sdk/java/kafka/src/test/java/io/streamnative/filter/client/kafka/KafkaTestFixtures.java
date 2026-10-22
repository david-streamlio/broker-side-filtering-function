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

import io.streamnative.filter.client.PrivateTopicPolicy;
import io.streamnative.filter.client.Session;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.Properties;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Shared mock-Session builder for Kafka-SDK unit tests. Returns a real {@link Session}
 * built atop mocked Pulsar internals so {@code registerPrivateQuery} / {@code updatePrivateQuery}
 * / {@code retirePrivateQuery} return real {@code PrivateQuery} objects and call into the
 * captured mocks.
 */
final class KafkaTestFixtures {

    /** Holders so tests can verify against the underlying Pulsar mocks. */
    static final class Mocks {
        final PulsarClient client;
        final PulsarAdmin admin;
        final Topics topics;
        final TopicPolicies topicPolicies;
        final Producer<byte[]> controlProducer;
        final TypedMessageBuilder<byte[]> controlMsgBuilder;
        final Session session;

        Mocks(PulsarClient client, PulsarAdmin admin, Topics topics, TopicPolicies topicPolicies,
              Producer<byte[]> controlProducer, TypedMessageBuilder<byte[]> controlMsgBuilder, Session session) {
            this.client            = client;
            this.admin             = admin;
            this.topics            = topics;
            this.topicPolicies     = topicPolicies;
            this.controlProducer   = controlProducer;
            this.controlMsgBuilder = controlMsgBuilder;
            this.session           = session;
        }
    }

    @SuppressWarnings("unchecked")
    static Mocks newSessionWithMocks() throws Exception {
        PulsarClient client = mock(PulsarClient.class);
        PulsarAdmin admin = mock(PulsarAdmin.class);
        Topics topics = mock(Topics.class);
        TopicPolicies topicPolicies = mock(TopicPolicies.class);

        ProducerBuilder<byte[]> producerBuilder = (ProducerBuilder<byte[]>) mock(ProducerBuilder.class);
        Producer<byte[]> controlProducer = (Producer<byte[]>) mock(Producer.class);
        TypedMessageBuilder<byte[]> controlMsgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);

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

        Session session = Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic("persistent://public/default/queries")
                .privateTopicTenant("public")
                .privateTopicNamespace("default")
                .privateTopicPolicy(PrivateTopicPolicy.DEFAULT)
                .build();

        return new Mocks(client, admin, topics, topicPolicies, controlProducer, controlMsgBuilder, session);
    }

    static Properties stubKafkaProps() {
        Properties p = new Properties();
        p.put("bootstrap.servers", "stub:9094");
        p.put("group.id", "stub-group");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return p;
    }

    private KafkaTestFixtures() {}
}
