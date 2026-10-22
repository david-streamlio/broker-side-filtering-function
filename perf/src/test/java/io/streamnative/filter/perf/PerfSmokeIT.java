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

package io.streamnative.filter.perf;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase-2 smoke test for the perf harness. Boots a KoP-enabled Pulsar broker and a ShadowTraffic
 * container on a private network, drives 1,000 synthetic trade records through ShadowTraffic onto
 * a Kafka topic, and verifies that a Pulsar consumer reads them back from the equivalent
 * {@code persistent://public/default/...} topic.
 *
 * <p>Skipped unless {@code SHADOWTRAFFIC_LICENSE_ID} is exported — the harness requires a paid
 * license. Activate the perf profile to include this in a build: {@code mvn -Pperf verify}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EnabledIfEnvironmentVariable(
        named = "SHADOWTRAFFIC_LICENSE_ID",
        matches = ".+",
        disabledReason = "ShadowTraffic license env vars not set; perf suite skipped.")
class PerfSmokeIT extends PerfHarnessIT {

    private static final Logger LOG = LoggerFactory.getLogger(PerfSmokeIT.class);

    private static final int SMOKE_RECORD_COUNT = 1_000;
    private static final String KAFKA_TOPIC = "perf-smoke-" + UUID.randomUUID();
    private static final String PULSAR_TOPIC = "persistent://public/default/" + KAFKA_TOPIC;

    private Network network;
    private GenericContainer<?> pulsar;
    private GenericContainer<?> shadowTraffic;
    private PulsarClient client;

    @BeforeAll
    void setup() throws Exception {
        // Defense in depth: even though the class-level annotation skips us when env vars are
        // absent, validate explicitly so missing-var failures land with a clear message rather
        // than a NullPointerException somewhere deep inside the container.
        requireLicenseEnvOrFail();

        network = Network.newNetwork();
        pulsar = newPulsarKoPContainer(network);
        pulsar.start();
        LOG.info("Pulsar/KoP broker up at {}:{} (Pulsar binary), {}:{} (HTTP admin)",
                pulsar.getHost(), pulsar.getMappedPort(PULSAR_BINARY_PORT),
                pulsar.getHost(), pulsar.getMappedPort(PULSAR_HTTP_PORT));

        Path configPath = renderConfig(
                "shadowtraffic/trades-smoke.json",
                commonPlaceholders(KAFKA_TOPIC, SMOKE_RECORD_COUNT));
        shadowTraffic = newShadowTrafficContainer(network, configPath);
        shadowTraffic.start();
        LOG.info("ShadowTraffic container up; generating {} records onto topic {}",
                SMOKE_RECORD_COUNT, KAFKA_TOPIC);

        String serviceUrl = "pulsar://" + pulsar.getHost() + ":"
                + pulsar.getMappedPort(PULSAR_BINARY_PORT);
        client = PulsarClient.builder().serviceUrl(serviceUrl).build();
    }

    @AfterAll
    void teardown() throws Exception {
        if (client != null) client.close();
        if (shadowTraffic != null) shadowTraffic.stop();
        if (pulsar != null) pulsar.stop();
        if (network != null) network.close();
    }

    @Test
    void shadowTrafficDeliversRecordsToBroker() throws Exception {
        try (Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(PULSAR_TOPIC)
                .subscriptionName("perf-smoke-probe")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {

            int received = 0;
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);
            while (received < SMOKE_RECORD_COUNT && System.nanoTime() < deadline) {
                Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    continue;
                }
                received++;
                consumer.acknowledge(msg);
            }
            assertThat(received)
                    .as("probe consumer should receive every generated record")
                    .isGreaterThanOrEqualTo(SMOKE_RECORD_COUNT);
        }
    }
}
