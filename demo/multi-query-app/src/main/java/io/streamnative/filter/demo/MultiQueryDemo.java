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

package io.streamnative.filter.demo;

import io.streamnative.filter.api.FilterControl;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.client.kafka.FilterableKafkaConsumerBuilder;
import io.streamnative.filter.client.kafka.FilterableKafkaConsumers;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Stand-up demo: read a list of queries from {@code queries.yaml}, register each
 * one through {@code selector-filter-client-kafka}, and consume from the private
 * filtered output topic the SDK + orchestrator wire up for each query.
 *
 * <p>Each query gets its own thread that drains its private topic at a steady
 * 1s poll cadence and logs a sample of received records. After the operator
 * adds queries to the watchlist, the {@code broker-side-filter-observability}
 * UI should show the spawned filter, the registered selectors, drop %s
 * differing across queries, and (for the {@code bad-syntax-demo} query) a
 * fired {@code selector-compile-fail} diagnostic.
 *
 * <p>Env vars required:
 * <ul>
 *   <li>{@code KAFKA_BOOTSTRAP} — KoP SASL_SSL bootstrap (e.g. {@code pc-...:9093}).</li>
 *   <li>{@code KAFKA_USERNAME} / {@code KAFKA_PASSWORD} — SASL/PLAIN creds.</li>
 *   <li>{@code PULSAR_SERVICE_URL} / {@code PULSAR_ADMIN_URL} — the SDK uses these
 *       for the control-plane Session that talks to the orchestrator.</li>
 *   <li>{@code PULSAR_TOKEN_FILE} — JWT (the SDK can also use OAuth2 if the
 *       inherited Pulsar client builder picks it up).</li>
 *   <li>{@code DEMO_SOURCE_TOPIC} — defaults to
 *       {@code persistent://public/default/stock-quotes}.</li>
 *   <li>{@code DEMO_CONTROL_TOPIC} — defaults to
 *       {@code persistent://public/default/filter-requests}.</li>
 * </ul>
 *
 * <p>Optional: {@code DEMO_QUERIES_PATH} (defaults to {@code /app/queries.yaml})
 * and {@code DEMO_LOG_SAMPLE_EVERY} (log one in N records per query; default 50).
 *
 * <p>SIGTERM / Ctrl-C closes every consumer cleanly, which triggers SDK-level
 * UNREGISTER for every queryId — the orchestrator then tombstones the matching
 * registry entries within ~1 sweep interval.
 */
public final class MultiQueryDemo {

    private static final Logger logger = LoggerFactory.getLogger(MultiQueryDemo.class);

    public static void main(String[] args) throws Exception {
        Path queriesPath = Paths.get(System.getenv().getOrDefault(
                "DEMO_QUERIES_PATH", "/app/queries.yaml"));
        // Per-query topics in queries.yaml take precedence (see
        // QueryConfig.Query#topics). DEMO_SOURCE_TOPIC remains the fallback
        // when a query does not specify its own topics — preserves the
        // single-topic stock-quotes flow without edits.
        String sourceTopic = System.getenv().getOrDefault(
                "DEMO_SOURCE_TOPIC", "persistent://public/default/stock-quotes");
        String controlTopic = System.getenv().getOrDefault(
                "DEMO_CONTROL_TOPIC", "persistent://public/default/filter-requests");
        long logSampleEvery = Long.parseLong(
                System.getenv().getOrDefault("DEMO_LOG_SAMPLE_EVERY", "50"));

        QueryConfig config = QueryConfig.load(queriesPath);
        logger.info("Loaded {} queries from {}", config.queries().size(), queriesPath);

        Properties kafkaProps = buildKafkaProps();
        // Pin private filtered topics to public/default. The SDK default is
        // public/filtered which doesn't exist on a fresh standalone broker —
        // auto-topic-creation creates topics, not namespaces, so spawned
        // filters silently fail to publish their output. public/default is
        // bootstrapped on every Pulsar cluster AND is KoP's default
        // kafkaNamespace, so Kafka clients can also read the filtered topics
        // via SASL/PLAINTEXT on :9092 / :9093.
        // Build the Pulsar client + admin ourselves so auth (and any
        // failover/TLS tuning) lives in one place, then inject them into the
        // Session. The SDK's serviceUrl()/adminUrl() path builds an
        // *unauthenticated* client — which fails immediately on SN Cloud
        // ("Failed to authenticate"). SessionBuilder.pulsarClient()/
        // pulsarAdmin() take pre-built clients; Session tracks
        // ownsPulsarClient/ownsPulsarAdmin and will NOT close injected ones,
        // so we own their lifecycle (closed in the shutdown hook below).
        // Auth mode auto-selects, mirroring buildKafkaProps: when
        // PULSAR_TOKEN_FILE is set we authenticate the Pulsar client + admin
        // with that JWT (SN Cloud / production); when it's absent we build
        // unauthenticated clients (local Pulsar standalone with anonymous auth).
        String tokenFile = System.getenv("PULSAR_TOKEN_FILE");
        boolean usePulsarAuth = tokenFile != null && !tokenFile.isBlank();
        java.util.function.Supplier<String> tokenSupplier = () -> {
            try {
                return Files.readString(Path.of(tokenFile)).trim();
            } catch (IOException e) {
                throw new IllegalStateException(
                        "cannot read PULSAR_TOKEN_FILE=" + tokenFile, e);
            }
        };
        var clientBuilder = PulsarClient.builder().serviceUrl(env("PULSAR_SERVICE_URL"));
        var adminBuilder = PulsarAdmin.builder().serviceHttpUrl(env("PULSAR_ADMIN_URL"));
        if (usePulsarAuth) {
            clientBuilder.authentication(AuthenticationFactory.token(tokenSupplier));
            adminBuilder.authentication(AuthenticationFactory.token(tokenSupplier));
        }
        PulsarClient pulsarClient = clientBuilder.build();
        PulsarAdmin pulsarAdmin = adminBuilder.build();
        Session session = Session.builder()
                .pulsarClient(pulsarClient)
                .pulsarAdmin(pulsarAdmin)
                .controlTopic(controlTopic)
                .privateTopicTenant("public")
                .privateTopicNamespace("default")
                .build();

        ExecutorService pool = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(false);
            t.setName("query-drainer-" + UUID.randomUUID().toString().substring(0, 6));
            return t;
        });
        List<Consumer<String, String>> consumers = new ArrayList<>();
        CountDownLatch shutdownLatch = new CountDownLatch(1);

        try {
            for (QueryConfig.Query q : config.queries()) {
                try {
                    FilterableKafkaConsumerBuilder<String, String> builder =
                            FilterableKafkaConsumers.<String, String>newConsumer(session, kafkaProps);
                    List<String> queryTopics;
                    if (q.topics().isEmpty()) {
                        builder.topic(sourceTopic);
                        queryTopics = List.of(sourceTopic);
                    } else {
                        builder.topics(q.topics());
                        queryTopics = q.topics();
                    }
                    Consumer<String, String> consumer = builder
                            .selector(q.selector())
                            .subscribe();
                    consumers.add(consumer);
                    // The SDK's consumer implements both Kafka's Consumer<K,V> AND
                    // io.streamnative.filter.api.FilterControl — cast to read
                    // the orchestrator-assigned queryId for correlation with the UI.
                    String queryId = ((FilterControl) consumer).queryId();
                    logger.info("Registered '{}' as queryId={}  (topics: {})  (selector: {})",
                            q.id(), queryId, queryTopics, q.selector());
                    pool.submit(() -> drainLoop(q.id(), queryId, consumer, logSampleEvery, shutdownLatch));
                } catch (Exception e) {
                    // Don't take the whole demo down for one bad query — the
                    // selector-compile-fail demo query is *expected* to surface
                    // here on registration. Log + continue.
                    logger.warn("Failed to register '{}' (selector={}): {} — skipping",
                            q.id(), q.selector(), e.toString());
                }
            }

            // Shutdown hook: wake the drain loops + close consumers cleanly so
            // the SDK can UNREGISTER each queryId before the JVM exits.
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutdown signal — closing {} consumers", consumers.size());
                shutdownLatch.countDown();
                for (Consumer<String, String> c : consumers) {
                    try {
                        c.close(Duration.ofSeconds(5));
                    } catch (Exception e) {
                        String qid = c instanceof FilterControl fc ? fc.queryId() : "<unknown>";
                        logger.warn("Error closing consumer queryId={}: {}", qid, e.toString());
                    }
                }
                try {
                    pool.shutdown();
                    if (!pool.awaitTermination(10, TimeUnit.SECONDS)) pool.shutdownNow();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                try { session.close(); } catch (Exception ignored) {}
                // Session does not own these (we injected them) — close them.
                try { pulsarAdmin.close(); } catch (Exception ignored) {}
                try { pulsarClient.close(); } catch (Exception ignored) {}
            }, "multi-query-demo-shutdown"));

            logger.info("Demo running with {} active consumers. Press Ctrl-C to exit.", consumers.size());
            shutdownLatch.await();
        } catch (Throwable fatal) {
            logger.error("Fatal error in demo main: {}", fatal.toString(), fatal);
            throw fatal;
        }
    }

    private static void drainLoop(String label,
                                  String queryId,
                                  Consumer<String, String> consumer,
                                  long logEvery,
                                  CountDownLatch shutdown) {
        long seen = 0;
        try {
            while (shutdown.getCount() > 0) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    seen++;
                    if (seen <= 3 || seen % logEvery == 0) {
                        String value = r.value();
                        String preview = value == null ? "<null>"
                                : value.length() > 120 ? value.substring(0, 120) + "…"
                                                       : value;
                        logger.info("[{} q={}] #{}  key={}  value={}",
                                label, queryId, seen, r.key(), preview);
                    }
                }
            }
        } catch (Throwable t) {
            if (shutdown.getCount() > 0) {
                logger.error("[{} q={}] drain loop crashed: {}", label, queryId, t.toString(), t);
            }
        } finally {
            logger.info("[{} q={}] drained {} records total", label, queryId, seen);
        }
    }

    /**
     * Build the Properties the SDK forwards to the underlying Kafka consumer.
     * Auth mode auto-selects: when {@code KAFKA_PASSWORD} is set we configure
     * SASL_SSL + PLAIN (KoP-on-SN-Cloud convention, token-as-password); when
     * it's absent we fall through to PLAINTEXT (local Pulsar standalone with
     * KoP and anonymous auth).
     */
    private static Properties buildKafkaProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env("KAFKA_BOOTSTRAP"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-query-demo-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        String pass = System.getenv("KAFKA_PASSWORD");
        if (pass != null && !pass.isBlank()) {
            String user = System.getenv().getOrDefault("KAFKA_USERNAME", "stock-demo-app");
            p.put("security.protocol", "SASL_SSL");
            p.put("sasl.mechanism", "PLAIN");
            p.put("sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\"" + user + "\" password=\"token:" + pass + "\";");
        } else {
            p.put("security.protocol", "PLAINTEXT");
        }
        return p;
    }

    private static String env(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("Required env var not set: " + name);
        }
        return v;
    }

    private MultiQueryDemo() {}
}
