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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class SelectorCheckToolTests {

    private static final String SESSIONS = "persistent://public/default/session_events";
    private static final String ORDERS = "persistent://public/default/orders";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static FieldCatalog.Sample sample(String key, String eventType, String json) {
        return new FieldCatalog.Sample(key, Map.of("event_type", eventType, "seq", "1"),
                json.getBytes(StandardCharsets.UTF_8));
    }

    private static final List<FieldCatalog.Sample> SAMPLES = List.of(
            sample("u-1", "login", "{\"amount\":5,\"device\":{\"os\":\"ios\"}}"),
            sample("u-2", "3ds_challenge", "{\"amount\":50,\"device\":{\"os\":\"android\"}}"),
            sample("u-1", "3ds_passed", "{\"amount\":500,\"device\":{\"os\":\"ios\"}}"),
            sample("u-3", "logout", "{\"amount\":7,\"device\":{\"os\":\"web\"}}"));

    private final AtomicInteger reads = new AtomicInteger();

    private SelectorCheckTool tool(List<FieldCatalog.Sample> samples) {
        FieldCatalog.Inspector inspector = new FieldCatalog.Inspector() {
            @Override
            public List<FieldCatalog.Sample> recent(String topic, int n) {
                reads.incrementAndGet();
                return samples;
            }

            @Override
            public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
                return Optional.empty();
            }
        };
        return new SelectorCheckTool(Topics.of(SESSIONS, ORDERS),
                new FieldCatalog(inspector, 25, Duration.ofMinutes(5), Clock.systemUTC()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(McpSchema.CallToolResult result) {
        assertThat(result.isError()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) result.structuredContent();
    }

    @SuppressWarnings("unchecked")
    private static List<String> warnings(Map<String, Object> r) {
        return (List<String>) r.get("warnings");
    }

    @Test
    void theDefinitionTakesASelectorOrSql() {
        McpSchema.Tool def = tool(SAMPLES).definition();

        assertThat(def.name()).isEqualTo("check_selector");
        assertThat(def.inputSchema()).doesNotContainKey("required"); // a selector or sql
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) def.inputSchema().get("properties");
        assertThat(properties).containsOnlyKeys("selector", "sql", "topic", "key");
        assertThat(def.description()).contains("SQL query");
    }

    @Test
    @SuppressWarnings("unchecked")
    void sqlIsTranslatedThenCheckedLikeASelector() {
        String sql = "SELECT * FROM session_events WHERE event_type LIKE '3ds_%' AND amount > 10 "
                + "AND ts > now() - INTERVAL '2' HOUR LIMIT 25";
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("sql", sql)));

        assertThat(r).containsEntry("topic", SESSIONS).containsEntry("sql", sql).containsEntry("valid", true)
                .containsEntry("filter", "event_type LIKE '3ds_%' AND amount > 10")
                .containsEntry("matched_in_sample", 2);
        Map<String, Object> translation = (Map<String, Object>) r.get("translation");
        assertThat(translation).containsEntry("selector", "event_type LIKE '3ds_%' AND amount > 10")
                .containsEntry("lookback", "2h").containsEntry("max_events", 25).containsEntry("topic", SESSIONS);
        assertThat((List<String>) translation.get("notes")).anyMatch(n -> n.contains("→ lookback 2h"));
    }

    @Test
    void sqlThatCanNotBeTranslatedIsInvalidWithTheReason() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("sql", "SELECT count(*) FROM session_events")));
        assertThat(r).containsEntry("valid", false).containsEntry("sql", "SELECT count(*) FROM session_events");
        assertThat((String) r.get("error")).contains("aggregates");

        Map<String, Object> header = body(tool(SAMPLES).call(Map.of("sql", "seq > 5")));
        assertThat(header).containsEntry("valid", true);
        assertThat(warnings(header)).anyMatch(w -> w.contains("seq"));

        Map<String, Object> onlyTime = body(tool(SAMPLES).call(Map.of("sql", "ts > now() - INTERVAL '5' MINUTE")));
        assertThat(onlyTime).containsEntry("valid", false);
        assertThat((String) onlyTime.get("error")).contains("no condition besides the time window");
    }

    @Test
    void sqlAndSelectorTogetherIsInvalid() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("sql", "seq = '1'", "selector", "seq = '1'")));
        assertThat(r).containsEntry("valid", false);
        assertThat((String) r.get("error")).contains("either sql or selector, not both");
    }

    @Test
    void aGoodSelectorIsTriedOnTheSample() throws Exception {
        McpSchema.CallToolResult result = tool(SAMPLES).call(Map.of("selector", "event_type LIKE '3ds_%'"));
        Map<String, Object> r = body(result);

        assertThat(r).containsEntry("valid", true).containsEntry("filter", "event_type LIKE '3ds_%'")
                .containsEntry("cost", "metadata only").containsEntry("sampled", 4).containsEntry("matched_in_sample", 2);
        assertThat(r.get("fields_used")).isEqualTo(List.of(Map.of("name", "event_type", "source", "header", "type", "string")));
        assertThat(warnings(r)).singleElement().asString().startsWith("Nothing in the filter picks exact values");
        assertThat((String) r.get("sample_note")).contains("small and recent").contains("4 newest");
        assertThat(JSON.readTree(((McpSchema.TextContent) result.content().get(0)).text()).path("valid").asBoolean()).isTrue();
    }

    @Test
    void aKeyIsCheckedWithTheSelector() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("key", "u-1", "selector", "device.os = 'ios'")));

        assertThat(r).containsEntry("filter", "__key__ = 'u-1' AND (device.os = 'ios')")
                .containsEntry("cost", "decodes each event's value")
                .containsEntry("matched_in_sample", 2).containsEntry("selector_matched_in_sample", 2);
        assertThat(r.get("fields_used")).isEqualTo(List.of(
                Map.of("name", "__key__", "source", "key", "type", "string"),
                Map.of("name", "device.os", "source", "body", "type", "string")));
        assertThat(warnings(r)).singleElement().asString().startsWith("device.os is a body field")
                .contains("event_type").contains("seq");
    }

    @Test
    void aSelectorThatMatchesEverythingOrNothingIsFlagged() {
        SelectorCheckTool tool = tool(SAMPLES);

        assertThat(warnings(body(tool.call(Map.of("selector", "amount > 1")))))
                .anyMatch(w -> w.startsWith("The selector matched all 4 sampled events"));
        List<String> none = warnings(body(tool.call(Map.of("selector", "event_type = 'Login'"))));
        assertThat(none).anyMatch(w -> w.startsWith("The selector matched none of the 4 sampled events")
                && w.contains("event_type: 'login', '3ds_challenge', '3ds_passed', 'logout'"));
        assertThat(none).noneMatch(w -> w.startsWith("Nothing in the filter"));
    }

    @Test
    void typeHintsAndLeadingWildcardsAreWarnings() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("selector", "seq > 0 OR event_type LIKE '%out'")));

        assertThat(r).containsEntry("valid", true).containsEntry("matched_in_sample", 1);
        assertThat(warnings(r)).anyMatch(w -> w.startsWith("event_type LIKE '%out' starts with %"))
                .anyMatch(w -> w.startsWith("seq is a header, and header values are always strings, so seq > 0 matches no event"));
    }

    @Test
    void aTypoIsInvalidWithADidYouMean() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("selector", "event_typ = 'login'")));

        assertThat(r).containsEntry("valid", false).doesNotContainKey("filter");
        assertThat((String) r.get("error")).contains("doesn't have: event_typ")
                .contains("did you mean event_type?").contains("get_schema");
    }

    @Test
    void aSyntaxErrorIsInvalidWithoutReadingTheTopic() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("selector", "event_type = \"login\"")));

        assertThat(r).containsEntry("valid", false);
        assertThat((String) r.get("error")).contains("isn't valid JMS selector syntax").contains("single quotes");
        assertThat(reads).hasValue(0);
    }

    @Test
    void aBadTopicOrKeyIsAToolError() {
        assertThat(tool(SAMPLES).call(Map.of("selector", "seq = '1'", "topic", "secrets")).isError()).isTrue();
        assertThat(tool(SAMPLES).call(Map.of("selector", "seq = '1'", "key", "x' OR '1'='1")).isError()).isTrue();
    }

    @Test
    void anEmptyTopicSaysNothingWasChecked() {
        Map<String, Object> r = body(tool(List.of()).call(Map.of("selector", "__key__ = 'u-1'")));

        assertThat(r).containsEntry("valid", true).containsEntry("sampled", 0).doesNotContainKey("matched_in_sample");
        assertThat(warnings(r)).singleElement().asString().contains("no recent events");
    }

    @Test
    void aPublishTimeTermIsNotTriedOnSamplesWithoutTimes() {
        Map<String, Object> r = body(tool(SAMPLES).call(Map.of("selector", "__publish_time__ > 0 AND seq = '1'")));

        assertThat(r).doesNotContainKey("matched_in_sample");
        assertThat((String) r.get("sample_skipped")).contains("publish time");
    }
}
