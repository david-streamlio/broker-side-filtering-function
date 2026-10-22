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

package io.streamnative.filter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spike: validates the round-trip required by a server-side-KoP Kafka SDK for the filter
 * function. Specifically — can a <b>Pulsar producer</b> (which is what the function uses to write
 * filtered output) write to a {@code kafkaEntryFormat=pulsar_non_batched} topic in such a way
 * that a real {@link KafkaConsumer} reads the payload and headers correctly?
 *
 * <p>The existing {@code KoPFilterFunctionIT} only exercised the <i>input</i> direction of
 * {@code pulsar_non_batched} (Kafka producer → Pulsar function-as-consumer) and documented a KoP
 * 4.2.0.6 framing bug there. The output direction is a different code path. If this spike passes,
 * the Kafka SDK can be built on top of real Kafka clients pointed at KoP. If it fails, we need to
 * pivot architectures (e.g. {@code pulsar-client-kafka} adapter, or function-side change).
 *
 * <p>Does NOT deploy the filter function — this is purely about raw broker capability.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KoPOutputFormatSpikeIT {

    private static final Logger LOG = LoggerFactory.getLogger(KoPOutputFormatSpikeIT.class);

    private static final String IMAGE = "streamnative/sn-platform:4.2.0.6";
    private static final int KAFKA_HOST_PORT = 9094; // distinct from KoPFilterFunctionIT's 9092

    private static final String KAFKA_TOPIC = "kop-output-spike";
    private static final String PULSAR_TOPIC = "persistent://public/default/" + KAFKA_TOPIC;

    /** {@link GenericContainer} with the protected {@code addFixedExposedPort} hook exposed. */
    private static final class FixedPortPulsarContainer extends GenericContainer<FixedPortPulsarContainer> {
        FixedPortPulsarContainer(DockerImageName image) { super(image); }
        FixedPortPulsarContainer bindHostPort(int hostPort, int containerPort) {
            addFixedExposedPort(hostPort, containerPort);
            return this;
        }
    }

    @Test
    void pulsarProducerToKafkaConsumerRoundTripsOnPulsarNonBatched() throws Exception {
        try (FixedPortPulsarContainer pulsar = new FixedPortPulsarContainer(DockerImageName.parse(IMAGE))
                .withExposedPorts(6650, 8080)
                .bindHostPort(KAFKA_HOST_PORT, KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                .withEnv("PULSAR_PREFIX_kafkaListeners",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners",
                        "PLAINTEXT://localhost:" + KAFKA_HOST_PORT)
                .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "false")
                .withEnv("PULSAR_PREFIX_kafkaNamespace", "public/default")
                .withEnv("PULSAR_PREFIX_kafkaEntryFormat", "pulsar_non_batched")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                .withCommand("sh", "-c",
                        "bin/apply-config-from-env.py conf/standalone.conf "
                                + "&& bin/pulsar standalone -nss")
                .waitingFor(Wait.forLogMessage(".*messaging service is ready.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(5)))) {

            pulsar.withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("pulsar-broker"));
            pulsar.start();

            String httpUrl = "http://" + pulsar.getHost() + ":" + pulsar.getMappedPort(8080);
            String pulsarUrl = "pulsar://" + pulsar.getHost() + ":" + pulsar.getMappedPort(6650);
            String kafkaBootstrap = pulsar.getHost() + ":" + KAFKA_HOST_PORT;

            try (PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl).build();
                 PulsarAdmin admin  = PulsarAdmin.builder().serviceHttpUrl(httpUrl).build()) {

                // Pre-create the topic in Pulsar; KoP exposes it as kafka-topic
                // "kop-output-spike" via the configured kafkaNamespace.
                admin.topics().createNonPartitionedTopic(PULSAR_TOPIC);

                // ----------------- Pulsar PRODUCER (what the function does on output) -----------------
                try (Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                        .topic(PULSAR_TOPIC)
                        .enableBatching(false) // function disables batching on its output too
                        .create()) {

                    producer.newMessage()
                            .key("BTC").property("region", "US")
                            .value("p-btc".getBytes(StandardCharsets.UTF_8))
                            .send();
                    producer.newMessage()
                            .key("ETH").property("region", "DE")
                            .value("p-eth".getBytes(StandardCharsets.UTF_8))
                            .send();
                    producer.newMessage()
                            .key("LTC").property("region", "US")
                            .value("p-ltc".getBytes(StandardCharsets.UTF_8))
                            .send();
                }

                // ----------------- Kafka CONSUMER (what an end-user app would do) ------------------
                Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "spike-" + UUID.randomUUID());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                        ByteArrayDeserializer.class.getName());

                Map<String, String> payloadByKey = new HashMap<>();
                Map<String, String> regionByKey = new HashMap<>();
                Set<String> keysSeen = new HashSet<>();
                try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
                    consumer.subscribe(List.of(KAFKA_TOPIC));

                    long deadline = System.currentTimeMillis() + 30_000L;
                    while (keysSeen.size() < 3 && System.currentTimeMillis() < deadline) {
                        ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(2));
                        for (ConsumerRecord<String, byte[]> r : records) {
                            String key = r.key();
                            keysSeen.add(key);
                            payloadByKey.put(key,
                                    new String(r.value(), StandardCharsets.UTF_8));
                            for (Header h : r.headers()) {
                                if ("region".equals(h.key())) {
                                    regionByKey.put(key,
                                            new String(h.value(), StandardCharsets.UTF_8));
                                }
                            }
                            LOG.info("Kafka consumer received: key={} payload={} headers={}",
                                    key, new String(r.value(), StandardCharsets.UTF_8), r.headers());
                        }
                    }
                }

                // ---------- Verdict ----------
                // What we're proving (or failing to prove):
                //   1. Kafka client even SEES the messages (proves KoP wire-format compat for output)
                //   2. Keys round-trip as UTF-8 strings (or document the encoding KoP applies)
                //   3. Payloads are byte-identical
                //   4. Pulsar user properties surface as Kafka headers (proves the filter function's
                //      forwarded properties will reach Kafka consumers)
                assertThat(keysSeen)
                        .as("Kafka consumer should see all 3 messages from the Pulsar producer "
                                + "via kafkaEntryFormat=pulsar_non_batched. If empty: KoP output "
                                + "direction has the same framing problem as the input direction "
                                + "documented in KoPFilterFunctionIT.")
                        .containsExactlyInAnyOrder("BTC", "ETH", "LTC");

                assertThat(payloadByKey).containsEntry("BTC", "p-btc");
                assertThat(payloadByKey).containsEntry("ETH", "p-eth");
                assertThat(payloadByKey).containsEntry("LTC", "p-ltc");

                assertThat(regionByKey)
                        .as("Pulsar user properties should be visible to the Kafka consumer as "
                                + "headers. If absent: KoP doesn't translate properties → headers "
                                + "in this direction, which means the filter function's "
                                + "forwardSourceMessageProperty=true is lost on the Kafka boundary.")
                        .containsEntry("BTC", "US")
                        .containsEntry("ETH", "DE")
                        .containsEntry("LTC", "US");
            }
        }
    }
}
