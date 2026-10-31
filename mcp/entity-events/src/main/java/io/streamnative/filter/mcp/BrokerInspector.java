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

package io.streamnative.filter.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The {@link FieldCatalog.Inspector} over a real broker: recent messages through the Pulsar admin API, and
 * the value schema from the registry that matches how the topic is written.
 *
 * <p>A topic is <em>Pulsar</em>-written (its schema, if any, is in Pulsar's schema registry) or
 * <em>Kafka</em>-written through KoP (Kafka producers register schemas, if at all, in a Kafka Schema
 * Registry; KoP stores no Pulsar schema for them). Which one is decided, in order, by:
 * <ol>
 *   <li>the operator: {@code KAFKA_TOPICS} / {@code PULSAR_TOPICS} (full or short names);</li>
 *   <li>the messages: KoP marks what it stores, with a base64-encoded key ({@code hasBase64EncodedKey}) or
 *       its {@code __ksn_internal_header_format} property;</li>
 *   <li>the topic: a {@code kafkaEntryFormat} topic property, which is set per topic for KoP;</li>
 *   <li>otherwise Pulsar.</li>
 * </ol>
 *
 * <p>The Kafka Schema Registry is any Confluent-compatible REST registry at {@code KAFKA_SCHEMA_REGISTRY_URL}
 * (KoP's own, or Confluent's); the subject is {@code KAFKA_SCHEMA_REGISTRY_SUBJECT} with {@code {topic}}
 * replaced by the Kafka topic name (default {@code {topic}-value}, the TopicNameStrategy), latest version.
 * Without a URL a Kafka topic has no registered schema, and its value fields are learned from samples.
 */
final class BrokerInspector implements FieldCatalog.Inspector {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerInspector.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** KoP's property naming which header values it base64-encoded (see {@code MessageBindings}). */
    static final String KOP_HEADER_FORMAT = "__ksn_internal_header_format";
    /** The topic property KoP reads to choose how it stores a topic's entries. */
    static final String KAFKA_ENTRY_FORMAT = "kafkaEntryFormat";
    static final String DEFAULT_SUBJECT = "{topic}-value";
    /** Messages read to look for KoP's marks when deciding a topic's storage. */
    private static final int DETECT_SAMPLE = 3;

    enum Storage { PULSAR, KAFKA }

    /**
     * A GET against the Kafka Schema Registry: the body, empty on 404.
     *
     * @throws Exception on any other failure
     */
    @FunctionalInterface
    interface Fetch {
        Optional<String> get(String url) throws Exception;
    }

    private final Supplier<PulsarAdmin> admin;
    private final Set<String> kafkaTopics;
    private final Set<String> pulsarTopics;
    private final String registryUrl;
    private final String subject;
    private final Fetch fetch;

    BrokerInspector(Supplier<PulsarAdmin> admin, Set<String> kafkaTopics, Set<String> pulsarTopics,
                    String registryUrl, String subject, Fetch fetch) {
        this.admin = admin;
        this.kafkaTopics = Set.copyOf(kafkaTopics);
        this.pulsarTopics = Set.copyOf(pulsarTopics);
        this.registryUrl = registryUrl == null || registryUrl.isBlank() ? null : registryUrl.replaceAll("/+$", "");
        this.subject = subject == null || subject.isBlank() ? DEFAULT_SUBJECT : subject;
        this.fetch = fetch;
    }

    /**
     * The production inspector, configured by {@code KAFKA_TOPICS}, {@code PULSAR_TOPICS},
     * {@code KAFKA_SCHEMA_REGISTRY_URL}, {@code KAFKA_SCHEMA_REGISTRY_SUBJECT}, and
     * {@code KAFKA_SCHEMA_REGISTRY_USERNAME}/{@code _PASSWORD} (basic auth) or
     * {@code KAFKA_SCHEMA_REGISTRY_TOKEN} (bearer).
     */
    static FieldCatalog.Inspector fromEnv(Supplier<PulsarAdmin> admin) {
        String auth = authorization(System.getenv("KAFKA_SCHEMA_REGISTRY_USERNAME"),
                System.getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD"), System.getenv("KAFKA_SCHEMA_REGISTRY_TOKEN"));
        return new BrokerInspector(admin, list(System.getenv("KAFKA_TOPICS")), list(System.getenv("PULSAR_TOPICS")),
                System.getenv("KAFKA_SCHEMA_REGISTRY_URL"), System.getenv("KAFKA_SCHEMA_REGISTRY_SUBJECT"),
                httpFetch(auth));
    }

    @Override
    public List<FieldCatalog.Sample> recent(String topic, int n) throws Exception {
        List<FieldCatalog.Sample> out = new ArrayList<>();
        for (Message<byte[]> m : latest(topic, n)) {
            out.add(new FieldCatalog.Sample(key(m), m.getProperties(), m.getData(), m.getPublishTime()));
        }
        return out;
    }

    @Override
    public Optional<FieldCatalog.RegisteredSchema> schema(String topic) throws Exception {
        return storage(topic) == Storage.KAFKA ? kafkaSchema(topic) : pulsarSchema(topic);
    }

    /** How {@code topic} is written; see the class comment for the order of evidence. */
    Storage storage(String topic) throws Exception {
        if (named(kafkaTopics, topic)) {
            return Storage.KAFKA;
        }
        if (named(pulsarTopics, topic)) {
            return Storage.PULSAR;
        }
        for (Message<byte[]> m : latest(topic, DETECT_SAMPLE)) {
            if (m.hasBase64EncodedKey() || m.getProperties().containsKey(KOP_HEADER_FORMAT)) {
                return Storage.KAFKA;
            }
        }
        try {
            Map<String, String> props = admin.get().topics().getProperties(topic);
            if (props != null && props.containsKey(KAFKA_ENTRY_FORMAT)) {
                return Storage.KAFKA;
            }
        } catch (PulsarAdminException e) {
            LOG.debug("couldn't read the properties of {}: {}", topic, e.getMessage());
        }
        return Storage.PULSAR;
    }

    private Optional<FieldCatalog.RegisteredSchema> pulsarSchema(String topic) throws PulsarAdminException {
        SchemaInfo info;
        try {
            info = admin.get().schemas().getSchemaInfo(topic);
        } catch (PulsarAdminException.NotFoundException e) {
            return Optional.empty();
        }
        if (info == null || info.getType() == null) {
            return Optional.empty();
        }
        byte[] def = info.getSchema();
        return Optional.of(new FieldCatalog.RegisteredSchema(info.getType().name(),
                def == null ? "" : new String(def, StandardCharsets.UTF_8)));
    }

    /** The latest version of the topic's subject in the Kafka Schema Registry. */
    Optional<FieldCatalog.RegisteredSchema> kafkaSchema(String topic) throws Exception {
        if (registryUrl == null) {
            return Optional.empty();
        }
        String subj = subject.replace("{topic}", kafkaTopicName(topic));
        Optional<String> body = fetch.get(registryUrl + "/subjects/"
                + URLEncoder.encode(subj, StandardCharsets.UTF_8).replace("+", "%20") + "/versions/latest");
        if (body.isEmpty()) {
            return Optional.empty();
        }
        JsonNode root = JSON.readTree(body.get());
        String schema = root.path("schema").asText("");
        if (schema.isEmpty()) {
            return Optional.empty();
        }
        // The registry omits schemaType for AVRO, its original and default type.
        String type = root.path("schemaType").asText("AVRO").toUpperCase(Locale.ROOT);
        return Optional.of(new FieldCatalog.RegisteredSchema(type, schema));
    }

    /**
     * Up to {@code n} of the newest messages, newest first. A partitioned topic is read per partition and
     * merged by publish time.
     */
    private List<Message<byte[]>> latest(String topic, int n) throws PulsarAdminException {
        int partitions = admin.get().topics().getPartitionedTopicMetadata(topic).partitions;
        if (partitions <= 0) {
            return latestOf(topic, n);
        }
        List<Message<byte[]>> all = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            all.addAll(latestOf(topic + "-partition-" + p, n));
        }
        all.sort(Comparator.comparingLong((Message<byte[]> m) -> m.getPublishTime()).reversed());
        return all.size() > n ? all.subList(0, n) : all;
    }

    private List<Message<byte[]>> latestOf(String topic, int n) throws PulsarAdminException {
        List<Message<byte[]>> out = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            try {
                out.add(admin.get().topics().examineMessage(topic, "latest", i));
            } catch (PulsarAdminException.NotFoundException | PulsarAdminException.ConflictException
                     | PulsarAdminException.PreconditionFailedException e) {
                break; // fewer than n messages (or none)
            } catch (PulsarAdminException e) {
                if (i == 1) {
                    throw e;
                }
                break; // past the start of what's retained
            }
        }
        return out;
    }

    /**
     * The logical key, as the filter's {@code __key__} sees it: {@code getKeyBytes()} decodes the base64 key
     * KoP stores for a Kafka key.
     */
    private static String key(Message<byte[]> m) {
        if (!m.hasKey()) {
            return null;
        }
        byte[] bytes = m.getKeyBytes();
        return bytes == null ? m.getKey() : new String(bytes, StandardCharsets.UTF_8);
    }

    /** The Kafka topic name KoP shows for a Pulsar topic: its last segment, without a partition suffix. */
    static String kafkaTopicName(String topic) {
        String s = shortName(topic);
        return s.replaceFirst("-partition-\\d+$", "");
    }

    private static String shortName(String topic) {
        int slash = topic.lastIndexOf('/');
        return slash < 0 ? topic : topic.substring(slash + 1);
    }

    private static boolean named(Set<String> names, String topic) {
        return names.contains(topic) || names.contains(shortName(topic));
    }

    static Set<String> list(String csv) {
        Set<String> out = new LinkedHashSet<>();
        if (csv != null) {
            for (String t : csv.split(",")) {
                if (!t.isBlank()) {
                    out.add(t.trim());
                }
            }
        }
        return out;
    }

    /** The registry's {@code Authorization} header value, or null for none. A token wins over basic auth. */
    static String authorization(String username, String password, String token) {
        if (token != null && !token.isBlank()) {
            return "Bearer " + token.trim();
        }
        if (username != null && !username.isBlank()) {
            String pair = username + ":" + (password == null ? "" : password);
            return "Basic " + Base64.getEncoder().encodeToString(pair.getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

    private static Fetch httpFetch(String authorization) {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        return url -> {
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Accept", "application/vnd.schemaregistry.v1+json, application/json")
                    .GET();
            if (authorization != null) {
                req.header("Authorization", authorization);
            }
            HttpResponse<String> res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 404) {
                return Optional.empty(); // no such subject, or no versions
            }
            if (res.statusCode() / 100 != 2) {
                throw new IllegalStateException("schema registry " + url + " returned HTTP " + res.statusCode());
            }
            return Optional.of(res.body());
        };
    }
}
