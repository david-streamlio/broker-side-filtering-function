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
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TopicToolsTests {

    private static final String ORDERS = "persistent://public/default/orders";
    private static final String SESSIONS = "persistent://public/default/session_events";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static TopicTools tools(FieldCatalog.Inspector inspector) {
        return new TopicTools(Topics.of(ORDERS, SESSIONS),
                new FieldCatalog(inspector, 25, Duration.ofMinutes(5), Clock.systemUTC()));
    }

    private static FieldCatalog.Inspector stub(List<FieldCatalog.Sample> samples) {
        return new FieldCatalog.Inspector() {
            @Override
            public List<FieldCatalog.Sample> recent(String topic, int n) {
                return samples;
            }

            @Override
            public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
                return Optional.empty();
            }
        };
    }

    private static final FieldCatalog.Inspector UNREACHABLE = new FieldCatalog.Inspector() {
        @Override
        public List<FieldCatalog.Sample> recent(String topic, int n) {
            throw new AssertionError("must not reach the broker");
        }

        @Override
        public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
            throw new AssertionError("must not reach the broker");
        }
    };

    private static String text(McpSchema.CallToolResult result) {
        return ((McpSchema.TextContent) result.content().get(0)).text();
    }

    @Test
    void listTopicsReturnsTheAllowlistAndWhatToDoNext() throws Exception {
        McpSchema.CallToolResult result = tools(UNREACHABLE).listTopics(Map.of());

        assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) result.structuredContent();
        assertThat(out.get("topics")).isEqualTo(List.of(
                Map.of("name", "orders", "topic", ORDERS),
                Map.of("name", "session_events", "topic", SESSIONS)));
        assertThat((String) out.get("next")).contains("get_schema");
        assertThat(JSON.readTree(text(result)).path("topics").get(1).path("name").asText()).isEqualTo("session_events");
    }

    @Test
    void theDefinitionsNameTheTools() {
        TopicTools tools = tools(UNREACHABLE);

        assertThat(tools.listTopicsDefinition().name()).isEqualTo("list_topics");
        McpSchema.Tool schema = tools.getSchemaDefinition();
        assertThat(schema.name()).isEqualTo("get_schema");
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) schema.inputSchema().get("properties");
        assertThat(properties).containsOnlyKeys("topic");
    }

    @Test
    void getSchemaReturnsTheFieldsGuidanceAndAnExampleSelector() throws Exception {
        TopicTools tools = tools(stub(List.of(new FieldCatalog.Sample("s-1",
                Map.of("event_type", "login", "__src_msgid__", "1:2"),
                "{\"device\":{\"os\":\"ios\"}}".getBytes(StandardCharsets.UTF_8)))));

        McpSchema.CallToolResult result = tools.getSchema(Map.of("topic", "session_events"));

        assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
        JsonNode out = JSON.readTree(text(result));
        assertThat(out.path("topic").asText()).isEqualTo(SESSIONS);
        assertThat(out.path("sampled").asInt()).isEqualTo(1);
        assertThat(out.path("body_fields_from").asText()).isEqualTo("sampled JSON values");
        assertThat(out.path("fields").findValuesAsText("name"))
                .containsExactly("__key__", "__publish_time__", "event_type", "device.os");
        assertThat(out.path("fields").get(2).path("examples").get(0).asText()).isEqualTo("login");
        assertThat(out.path("how_to_filter").asText()).contains("selector").contains("IN").contains("lookback");
        assertThat(out.path("example").asText()).isEqualTo("__key__ = '<id>' AND event_type = 'login'");
        assertThat(result.structuredContent()).isInstanceOf(Map.class);
    }

    @Test
    void withoutAHeaderExampleTheExampleSelectorIsTheKey() {
        McpSchema.CallToolResult result = tools(stub(List.of())).getSchema(Map.of());

        @SuppressWarnings("unchecked")
        Map<String, Object> out = (Map<String, Object>) result.structuredContent();
        assertThat(out.get("topic")).isEqualTo(ORDERS);
        assertThat(out.get("example")).isEqualTo("__key__ = '<id>'");
        assertThat(out.get("body_fields_from")).isEqualTo("none");
    }

    @Test
    void theExampleSkipsHeadersASelectorCantNameAndEscapesQuotes() {
        List<FieldCatalog.Field> fields = List.of(
                new FieldCatalog.Field("__key__", "key", "string", List.of()),
                new FieldCatalog.Field("x-trace", "header", "string", List.of("abc")),
                new FieldCatalog.Field("note", "header", "string", List.of("it's")));

        assertThat(TopicTools.example(fields)).isEqualTo("__key__ = '<id>' AND note = 'it''s'");
    }

    @Test
    void anUnknownTopicIsAToolErrorThatPointsAtListTopics() {
        McpSchema.CallToolResult result = tools(UNREACHABLE).getSchema(Map.of("topic", "secrets"));

        assertThat(result.isError()).isTrue();
        assertThat(text(result)).contains("secrets").contains("list_topics");
    }

    @Test
    void aBrokerFailureIsAToolError() {
        FieldCatalog.Inspector broken = new FieldCatalog.Inspector() {
            @Override
            public List<FieldCatalog.Sample> recent(String topic, int n) throws Exception {
                throw new Exception("broker down");
            }

            @Override
            public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
                return Optional.empty();
            }
        };

        McpSchema.CallToolResult result = tools(broken).getSchema(Map.of("topic", "orders"));

        assertThat(result.isError()).isTrue();
        assertThat(text(result)).contains("couldn't read the fields").contains("broker down");
    }
}
