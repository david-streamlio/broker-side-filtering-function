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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BrokerInspectorTests {

    private static final String TOPIC = "persistent://public/default/orders";
    private static final String REGISTRY = "http://registry:8081";
    private static final String AVRO = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
            + "{\"name\":\"status\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"}]}";

    private final PulsarAdmin admin = mock(PulsarAdmin.class);
    private final Topics topics = mock(Topics.class);
    private final Schemas schemas = mock(Schemas.class);
    /** Registry responses by URL; every URL asked for is recorded. */
    private final Map<String, String> registry = new HashMap<>();
    private final List<String> fetched = new ArrayList<>();
    private final BrokerInspector.Fetch fetch = url -> {
        fetched.add(url);
        return Optional.ofNullable(registry.get(url));
    };

    @BeforeEach
    void setUp() throws Exception {
        when(admin.topics()).thenReturn(topics);
        when(admin.schemas()).thenReturn(schemas);
        when(topics.getPartitionedTopicMetadata(anyString())).thenReturn(new PartitionedTopicMetadata(0));
        when(topics.getProperties(anyString())).thenReturn(Map.of());
        doThrow(notFound()).when(topics).examineMessage(anyString(), eq("latest"), anyLong());
        doThrow(notFound()).when(schemas).getSchemaInfo(anyString());
    }

    private BrokerInspector inspector() {
        return inspector(Set.of(), Set.of(), REGISTRY);
    }

    private BrokerInspector inspector(Set<String> kafka, Set<String> pulsar, String url) {
        return new BrokerInspector(() -> admin, kafka, pulsar, url, null, fetch);
    }

    private static PulsarAdminException.NotFoundException notFound() {
        return new PulsarAdminException.NotFoundException(new RuntimeException("not found"), "not found", 404);
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> message(String key, boolean base64Key, Map<String, String> props, String value,
                                           long publishTime) {
        Message<byte[]> m = mock(Message.class);
        when(m.hasKey()).thenReturn(key != null);
        when(m.hasBase64EncodedKey()).thenReturn(base64Key);
        if (key != null) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            when(m.getKeyBytes()).thenReturn(keyBytes);
            when(m.getKey()).thenReturn(base64Key
                    ? java.util.Base64.getEncoder().encodeToString(keyBytes) : key);
        }
        when(m.getProperties()).thenReturn(props);
        when(m.getData()).thenReturn(value == null ? null : value.getBytes(StandardCharsets.UTF_8));
        when(m.getPublishTime()).thenReturn(publishTime);
        return m;
    }

    @SafeVarargs
    private final void messages(String topic, Message<byte[]>... newestFirst) throws Exception {
        for (int i = 0; i < newestFirst.length; i++) {
            doReturn(newestFirst[i]).when(topics).examineMessage(topic, "latest", i + 1);
        }
    }

    private void pulsarSchema(SchemaType type, String definition) throws Exception {
        SchemaInfo info = SchemaInfo.builder().name("orders").type(type)
                .schema(definition.getBytes(StandardCharsets.UTF_8)).build();
        doReturn(info).when(schemas).getSchemaInfo(TOPIC);
    }

    private void kafkaSubject(String body) {
        registry.put(REGISTRY + "/subjects/orders-value/versions/latest", body);
    }

    // --- storage type ---

    @Test
    void pulsarWrittenTopicUsesThePulsarRegistry() throws Exception {
        messages(TOPIC, message("o-1", false, Map.of("region", "US"), "{}", 2));
        pulsarSchema(SchemaType.AVRO, AVRO);
        kafkaSubject("{\"schema\":\"{}\"}");

        assertThat(inspector().storage(TOPIC)).isEqualTo(BrokerInspector.Storage.PULSAR);
        assertThat(inspector().schema(TOPIC)).contains(new FieldCatalog.RegisteredSchema("AVRO", AVRO));
        assertThat(fetched).isEmpty();
    }

    @Test
    void base64KeyMarksAKafkaWrittenTopic() throws Exception {
        messages(TOPIC, message("o-1", true, Map.of(), "{}", 2));
        pulsarSchema(SchemaType.JSON, "{\"type\":\"record\",\"fields\":[]}");
        kafkaSubject("{\"subject\":\"orders-value\",\"version\":3,\"id\":7,\"schema\":" + quote(AVRO) + "}");

        assertThat(inspector().storage(TOPIC)).isEqualTo(BrokerInspector.Storage.KAFKA);
        assertThat(inspector().schema(TOPIC)).contains(new FieldCatalog.RegisteredSchema("AVRO", AVRO));
        assertThat(fetched).containsOnly(REGISTRY + "/subjects/orders-value/versions/latest");
        verify(schemas, never()).getSchemaInfo(anyString());
    }

    @Test
    void kopHeaderFormatPropertyMarksAKafkaWrittenTopic() throws Exception {
        messages(TOPIC, message(null, false, Map.of(BrokerInspector.KOP_HEADER_FORMAT, "{}"), "{}", 2));

        assertThat(inspector().storage(TOPIC)).isEqualTo(BrokerInspector.Storage.KAFKA);
    }

    @Test
    void kafkaEntryFormatTopicPropertyMarksAKafkaWrittenTopic() throws Exception {
        messages(TOPIC, message(null, false, Map.of(), "{}", 2)); // a keyless, headerless Kafka record
        when(topics.getProperties(TOPIC)).thenReturn(Map.of("kafkaEntryFormat", "pulsar_non_batched"));

        assertThat(inspector().storage(TOPIC)).isEqualTo(BrokerInspector.Storage.KAFKA);
    }

    @Test
    void operatorListsOverrideDetection() throws Exception {
        messages(TOPIC, message("o-1", true, Map.of(), "{}", 2));
        assertThat(inspector(Set.of(), Set.of("orders"), REGISTRY).storage(TOPIC)).isEqualTo(BrokerInspector.Storage.PULSAR);

        when(topics.getProperties(TOPIC)).thenReturn(Map.of());
        messages(TOPIC, message("o-1", false, Map.of(), "{}", 2));
        assertThat(inspector(Set.of(TOPIC), Set.of(), REGISTRY).storage(TOPIC)).isEqualTo(BrokerInspector.Storage.KAFKA);
    }

    @Test
    void emptyTopicWithoutMarksIsPulsar() throws Exception {
        assertThat(inspector().storage(TOPIC)).isEqualTo(BrokerInspector.Storage.PULSAR);
    }

    // --- not found ---

    @Test
    void noPulsarSchemaIsEmpty() throws Exception {
        assertThat(inspector().schema(TOPIC)).isEmpty();
    }

    @Test
    void noKafkaSubjectIsEmpty() throws Exception {
        assertThat(inspector(Set.of("orders"), Set.of(), REGISTRY).schema(TOPIC)).isEmpty();
        assertThat(fetched).hasSize(1);
    }

    @Test
    void kafkaTopicWithoutARegistryIsEmpty() throws Exception {
        assertThat(inspector(Set.of("orders"), Set.of(), null).schema(TOPIC)).isEmpty();
        assertThat(fetched).isEmpty();
    }

    @Test
    void registryErrorsPropagate() {
        BrokerInspector broken = new BrokerInspector(() -> admin, Set.of("orders"), Set.of(), REGISTRY, null, url -> {
            throw new IllegalStateException("HTTP 500");
        });
        assertThatThrownBy(() -> broken.schema(TOPIC)).hasMessageContaining("HTTP 500");
    }

    // --- subjects and types ---

    @Test
    void subjectTemplateAndPartitionSuffix() throws Exception {
        BrokerInspector custom = new BrokerInspector(() -> admin, Set.of("orders-partition-0"), Set.of(), REGISTRY + "/",
                "acme.{topic}", fetch);
        custom.schema(TOPIC + "-partition-0");
        assertThat(fetched).containsExactly(REGISTRY + "/subjects/acme.orders/versions/latest");
    }

    @Test
    void confluentJsonSchemaBecomesFields() throws Exception {
        String jsonSchema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\",\"type\":\"object\",\"properties\":{"
                + "\"status\":{\"type\":\"string\"},\"amount\":{\"type\":\"number\"},\"qty\":{\"type\":\"integer\"},"
                + "\"gift\":{\"type\":[\"boolean\",\"null\"]},\"tier\":{\"enum\":[\"GOLD\",\"SILVER\"]},"
                + "\"tags\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
                + "\"ship\":{\"type\":\"object\",\"properties\":{\"carrier\":{\"type\":\"string\"}}}}}";
        kafkaSubject("{\"schemaType\":\"JSON\",\"schema\":" + quote(jsonSchema) + "}");

        Optional<FieldCatalog.RegisteredSchema> schema = inspector(Set.of("orders"), Set.of(), REGISTRY).schema(TOPIC);

        assertThat(schema).contains(new FieldCatalog.RegisteredSchema("JSON", jsonSchema));
        assertThat(FieldCatalog.schemaFields(schema.get())).containsExactly(
                Map.entry("status", "string"), Map.entry("amount", "number"), Map.entry("qty", "number"),
                Map.entry("gift", "boolean"), Map.entry("tier", "string"), Map.entry("ship.carrier", "string"));
    }

    @Test
    void protobufSubjectKeepsItsTypeAndFallsBackToSampling() throws Exception {
        kafkaSubject("{\"schemaType\":\"PROTOBUF\",\"schema\":\"syntax = \\\"proto3\\\"; message Order {}\"}");

        FieldCatalog.RegisteredSchema schema = inspector(Set.of("orders"), Set.of(), REGISTRY).schema(TOPIC).orElseThrow();

        assertThat(schema.type()).isEqualTo("PROTOBUF");
        assertThat(FieldCatalog.schemaFields(schema)).isEmpty();
    }

    // --- recent messages ---

    @Test
    void recentReadsNewestFirstAndStopsWhenTheTopicRunsOut() throws Exception {
        messages(TOPIC,
                message("o-2", false, Map.of("region", "US"), "{\"status\":\"shipped\"}", 2),
                message("o-1", false, Map.of("region", "UK"), "{\"status\":\"new\"}", 1));

        List<FieldCatalog.Sample> samples = inspector().recent(TOPIC, 25);

        assertThat(samples).extracting(FieldCatalog.Sample::key).containsExactly("o-2", "o-1");
        assertThat(samples.get(0).properties()).containsEntry("region", "US");
        assertThat(new String(samples.get(1).value(), StandardCharsets.UTF_8)).isEqualTo("{\"status\":\"new\"}");
    }

    @Test
    void recentUsesTheLogicalKeyOfAKoPMessage() throws Exception {
        messages(TOPIC, message("BTC", true, Map.of(), "{}", 1));

        assertThat(inspector().recent(TOPIC, 5)).extracting(FieldCatalog.Sample::key).containsExactly("BTC");
    }

    @Test
    void recentMergesPartitionsByPublishTime() throws Exception {
        when(topics.getPartitionedTopicMetadata(TOPIC)).thenReturn(new PartitionedTopicMetadata(2));
        messages(TOPIC + "-partition-0", message("a", false, Map.of(), "{}", 30), message("b", false, Map.of(), "{}", 10));
        messages(TOPIC + "-partition-1", message("c", false, Map.of(), "{}", 20));

        assertThat(inspector().recent(TOPIC, 2)).extracting(FieldCatalog.Sample::key).containsExactly("a", "c");
    }

    @Test
    void recentFailsWhenTheTopicCantBeRead() throws Exception {
        doThrow(new PulsarAdminException.NotAuthorizedException(new RuntimeException("no"), "not authorized", 401))
                .when(topics).examineMessage(TOPIC, "latest", 1);

        assertThatThrownBy(() -> inspector().recent(TOPIC, 5)).isInstanceOf(PulsarAdminException.class);
    }

    // --- Confluent wire format ---

    @Test
    void wireFormatJsonValuesAreParsedButTheirBodyFieldsArentOffered() {
        byte[] json = "{\"status\":\"shipped\",\"amount\":12.5}".getBytes(StandardCharsets.UTF_8);
        byte[] framed = ByteBuffer.allocate(5 + json.length).put((byte) 0).putInt(42).put(json).array();

        FieldCatalog.Fields f = FieldCatalog.build(TOPIC, Optional.empty(),
                List.of(new FieldCatalog.Sample("o-1", Map.of(), framed)));

        // The payload is found past the prefix (it parses as JSON), but the filter reads the raw value, so a
        // selector on these fields couldn't match: only the key and headers are offered.
        assertThat(f.bodyFieldsFrom()).contains("sampled JSON values").contains("wire format");
        assertThat(f.names()).containsExactly("__key__", "__publish_time__");
    }

    @Test
    void unframedValuesAreLeftAlone() {
        assertThat(FieldCatalog.unframed("{\"a\":1}".getBytes(StandardCharsets.UTF_8))).isZero();
        assertThat(FieldCatalog.unframed(new byte[] {0, 0, 0, 0, 1})).isZero(); // prefix only, no payload
        assertThat(FieldCatalog.unframed(new byte[] {0, 0, 0, 0, 1, '{', '}'})).isEqualTo(5);
    }

    // --- configuration ---

    @Test
    void authorizationPrefersATokenOverBasicAuth() {
        assertThat(BrokerInspector.authorization(null, null, null)).isNull();
        assertThat(BrokerInspector.authorization("u", "p", null)).isEqualTo("Basic dTpw");
        assertThat(BrokerInspector.authorization("u", "p", "t")).isEqualTo("Bearer t");
    }

    @Test
    void topicListsAreCommaSeparated() {
        assertThat(BrokerInspector.list(" orders, ,persistent://public/default/trades ")).containsExactly(
                "orders", "persistent://public/default/trades");
        assertThat(BrokerInspector.list(null)).isEmpty();
    }

    private static String quote(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
