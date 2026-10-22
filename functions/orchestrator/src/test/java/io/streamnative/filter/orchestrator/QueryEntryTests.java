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

package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryEntryTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void legacyConstructorOmitsRegistryKeys() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"));
        assertThat(q.getOutputTopic()).isEqualTo("persistent://public/out/x");
        assertThat(q.getControlTopics()).containsExactly("persistent://public/orchestrator/ctrl-a");
        assertThat(q.getRegistryKeys()).isNull();
    }

    @Test
    void newConstructorStoresParallelRegistryKeys() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a",
                        "persistent://public/orchestrator/ctrl-b"),
                List.of("persistent://public/in/a",
                        "persistent://public/in/b"));
        assertThat(q.getControlTopics()).hasSize(2);
        assertThat(q.getRegistryKeys()).containsExactly(
                "persistent://public/in/a", "persistent://public/in/b");
    }

    @Test
    void serdeRoundTripWithRegistryKeys() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"));
        byte[] payload = MAPPER.writeValueAsBytes(q);
        QueryEntry parsed = MAPPER.readValue(payload, QueryEntry.class);
        assertThat(parsed).isEqualTo(q);
        assertThat(parsed.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }

    @Test
    void serdeOmitsNullRegistryKeys() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"));
        String json = MAPPER.writeValueAsString(q);
        assertThat(json).doesNotContain("registryKeys");
    }

    @Test
    void legacyJsonWithoutRegistryKeysDeserializesNull() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"]
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getRegistryKeys()).isNull();
        assertThat(parsed.getOutputTopic()).isEqualTo("persistent://public/out/x");
    }

    @Test
    void unknownFieldsTolerated() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"],
                  "registryKeys": ["persistent://public/in/a"],
                  "someFutureField": 42
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getOutputTopic()).isEqualTo("persistent://public/out/x");
        assertThat(parsed.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }

    @Test
    void registryKeysListIsImmutable() {
        java.util.ArrayList<String> mutableKeys = new java.util.ArrayList<>();
        mutableKeys.add("persistent://public/in/a");
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                mutableKeys);
        // Caller mutating after construction must not affect the stored list.
        mutableKeys.add("persistent://public/in/b");
        assertThat(q.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }

    // ---------------------------------------------------------------- lastSeen / heartbeat

    @Test
    void withLastSeenStampsTimestamp() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"));
        assertThat(q.getLastSeen()).isNull();

        java.time.Instant now = java.time.Instant.parse("2026-05-12T22:00:00Z");
        QueryEntry stamped = q.withLastSeen(now);
        assertThat(stamped.getLastSeen()).isEqualTo("2026-05-12T22:00:00Z");
        // Other fields preserved.
        assertThat(stamped.getOutputTopic()).isEqualTo(q.getOutputTopic());
        assertThat(stamped.getControlTopics()).isEqualTo(q.getControlTopics());
        assertThat(stamped.getRegistryKeys()).isEqualTo(q.getRegistryKeys());
    }

    @Test
    void withLastSeenIsIndependentOfPriorLastSeen() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z");
        QueryEntry refreshed = q.withLastSeen(java.time.Instant.parse("2026-05-12T22:00:30Z"));
        assertThat(refreshed.getLastSeen()).isEqualTo("2026-05-12T22:00:30Z");
    }

    @Test
    void serdeRoundTripWithLastSeen() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z");
        byte[] payload = MAPPER.writeValueAsBytes(q);
        QueryEntry parsed = MAPPER.readValue(payload, QueryEntry.class);
        assertThat(parsed).isEqualTo(q);
        assertThat(parsed.getLastSeen()).isEqualTo("2026-05-12T22:00:00Z");
    }

    @Test
    void legacyJsonWithoutLastSeenDeserializesNull() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"],
                  "registryKeys":  ["persistent://public/in/a"]
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getLastSeen()).isNull();
    }

    // ---------------------------------------------------------------- selector (reconcile repopulation)

    @Test
    void withSelectorReplacesAndPreservesOtherFields() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z");
        assertThat(q.getSelector()).isNull();

        QueryEntry withSel = q.withSelector("region = 'US'");
        assertThat(withSel.getSelector()).isEqualTo("region = 'US'");
        // Other fields preserved, including lastSeen.
        assertThat(withSel.getOutputTopic()).isEqualTo(q.getOutputTopic());
        assertThat(withSel.getControlTopics()).isEqualTo(q.getControlTopics());
        assertThat(withSel.getRegistryKeys()).isEqualTo(q.getRegistryKeys());
        assertThat(withSel.getLastSeen()).isEqualTo("2026-05-12T22:00:00Z");
    }

    @Test
    void withLastSeenPreservesSelector() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"),
                null,
                "region = 'US'");
        QueryEntry stamped = q.withLastSeen(java.time.Instant.parse("2026-05-12T22:00:00Z"));
        assertThat(stamped.getSelector()).isEqualTo("region = 'US'");
        assertThat(stamped.getLastSeen()).isEqualTo("2026-05-12T22:00:00Z");
    }

    @Test
    void serdeRoundTripWithSelector() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"),
                "2026-05-12T22:00:00Z",
                "__key__ IN ('AAPL','MSFT')");
        byte[] payload = MAPPER.writeValueAsBytes(q);
        QueryEntry parsed = MAPPER.readValue(payload, QueryEntry.class);
        assertThat(parsed).isEqualTo(q);
        assertThat(parsed.getSelector()).isEqualTo("__key__ IN ('AAPL','MSFT')");
    }

    @Test
    void serdeOmitsNullSelector() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"));
        assertThat(MAPPER.writeValueAsString(q)).doesNotContain("selector");
    }

    @Test
    void legacyJsonWithoutSelectorDeserializesNull() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"],
                  "registryKeys":  ["persistent://public/in/a"]
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getSelector()).isNull();
    }
}
