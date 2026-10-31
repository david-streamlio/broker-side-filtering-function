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
import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.common.WindowStats;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EntityEventsToolTests {

    private static final EntityQuery.Limits LIMITS =
            new EntityQuery.Limits(Duration.ofHours(24), Duration.ofSeconds(60));
    private static final String TOPIC = "persistent://public/default/orders";

    private static EntityEventsTool toolWithoutBroker() {
        return new EntityEventsTool(() -> {
            throw new AssertionError("an invalid call must not reach the broker");
        }, TOPIC, LIMITS, Clock.systemUTC());
    }

    private static WindowStats stats(long scanned, WindowStats.Status status) {
        return new WindowStats("q1", 1_000L, 2_000L, scanned, 7, 0, null, null, 0, WindowStats.EndedBy.LOG_END, status);
    }

    private static WindowStats truncatedFrom(long firstScannedMs) {
        return new WindowStats("q1", 1_000L, 9_000L, 10, 1, 0, firstScannedMs, 8_000L, 0, WindowStats.EndedBy.LOG_END,
                WindowStats.Status.TRUNCATED_BY_RETENTION);
    }

    @Test
    void theDefinitionNamesTheToolTheTopicAndTheServerLimits() {
        McpSchema.Tool tool = toolWithoutBroker().definition();

        assertThat(tool.name()).isEqualTo("get_entity_events");
        assertThat(tool.description()).contains(TOPIC).contains("TRUNCATED_BY_RETENTION");
        Map<String, Object> schema = tool.inputSchema();
        assertThat(schema.get("required")).isEqualTo(List.of("key"));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        assertThat(properties).containsOnlyKeys("key", "lookback", "max_events");
        assertThat(properties.get("lookback").toString()).contains("At most 24h");
    }

    @Test
    void anInvalidCallIsAToolErrorTheAgentCanRead() {
        McpSchema.CallToolResult result = toolWithoutBroker().call(Map.of("key", "x' OR '1'='1"));

        assertThat(result.isError()).isTrue();
        assertThat(((McpSchema.TextContent) result.content().get(0)).text()).contains("key must be");
    }

    @Test
    void aCompleteResultSaysAnEmptyListMeansNoEvents() {
        EntityQuery q = new EntityQuery("8675309", Duration.ofHours(1), 50);

        Map<String, Object> r = EntityEventsTool.result(q, TOPIC, stats(2_000_000, WindowStats.Status.COMPLETE), List.of(), 0, 42);

        assertThat(r).containsEntry("status", "COMPLETE").containsEntry("scanned", 2_000_000L)
                .containsEntry("matched", 0L).containsEntry("returned", 0).containsEntry("more_events", false);
        assertThat((String) r.get("status_meaning")).contains("no events");
        assertThat(r.get("window")).isEqualTo(Map.of("from", "1970-01-01T00:00:01Z", "to", "1970-01-01T00:00:02Z"));
    }

    @Test
    void aTruncatedResultWarnsAgainstConcludingNothingHappened() {
        EntityQuery q = new EntityQuery("8675309", Duration.ofHours(1), 50);

        Map<String, Object> r = EntityEventsTool.result(q, TOPIC, stats(0, WindowStats.Status.TRUNCATED_BY_RETENTION),
                List.of(), 0, 42);

        assertThat(r).containsEntry("status", "TRUNCATED_BY_RETENTION");
        assertThat((String) r.get("status_meaning")).contains("it holds none").contains("don't conclude that nothing happened");
    }

    @Test
    void aTruncatedResultSaysWhereTheLogStartsAndNamesBothCauses() {
        EntityQuery q = new EntityQuery("8675309", Duration.ofHours(1), 50);

        Map<String, Object> r = EntityEventsTool.result(q, TOPIC, truncatedFrom(5_000L), List.of(), 1, 1);

        assertThat((String) r.get("status_meaning")).contains("(1970-01-01T00:00:05Z)")
                .contains("retention removed older events, or the topic is newer than the window");
    }

    @Test
    void moreMatchesThanReturnedAreFlagged() {
        EntityQuery q = new EntityQuery("8675309", Duration.ofHours(1), 1);
        Map<String, Object> one = EntityEventsTool.event("8675309", Map.of(), "x".getBytes(StandardCharsets.UTF_8), 0);

        Map<String, Object> r = EntityEventsTool.result(q, TOPIC, stats(10, WindowStats.Status.COMPLETE), List.of(one), 3, 1);

        assertThat(r).containsEntry("matched", 3L).containsEntry("returned", 1).containsEntry("more_events", true);
        assertThat((String) r.get("omitted")).startsWith("2 older matching events");
    }

    @Test
    void anEventKeepsItsOwnPropertiesAndParsesAJsonValue() {
        Map<String, String> props = Map.of("event_type", "shipped", "seq", "4",
                "__src_msgid__", "9:12:-1", "__src_publish_time__", "1792701425414");

        Map<String, Object> e = EntityEventsTool.event("8675309", props,
                "{\"carrier\":\"ups\",\"items\":2}".getBytes(StandardCharsets.UTF_8), 0);

        assertThat(e).containsEntry("time", "2026-10-22T20:37:05.414Z").containsEntry("key", "8675309");
        assertThat(e.get("properties")).isEqualTo(Map.of("event_type", "shipped", "seq", "4"));
        assertThat(((JsonNode) e.get("value")).get("carrier").asText()).isEqualTo("ups");
    }

    @Test
    void aValueThatIsNotJsonStaysText() {
        Map<String, Object> e = EntityEventsTool.event("k", Map.of(), "plain text".getBytes(StandardCharsets.UTF_8), 1_000);

        assertThat(e).containsEntry("value", "plain text").containsEntry("time", "1970-01-01T00:00:01Z");
    }

    @Test
    void keepNewestKeepsTheNewestMaxEventsOldestFirst() {
        List<Map<String, Object>> events = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            EntityEventsTool.keepNewest(events, Map.of("n", i), 3);
        }

        assertThat(events).extracting(e -> e.get("n")).containsExactly(3, 4, 5);
    }
}
