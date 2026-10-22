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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link QueryConfig} — the demo's YAML query loader. These are pure
 * (no broker / no Docker) and exercise the {@code topics} parsing the parcels second
 * data source relies on: optional, null-safe, order-preserving, and defensively copied.
 */
class QueryConfigTests {

    @TempDir
    Path tmp;

    private QueryConfig loadYaml(String yaml) throws IOException {
        Path f = tmp.resolve("queries.yaml");
        Files.writeString(f, yaml);
        return QueryConfig.load(f);
    }

    @Test
    void topicsAbsent_defaultsToEmptyList() throws IOException {
        QueryConfig config = loadYaml("""
                queries:
                  - id: no-topics
                    selector: "sector = 'tech'"
                """);

        assertThat(config.queries()).hasSize(1);
        QueryConfig.Query q = config.queries().get(0);
        assertThat(q.id()).isEqualTo("no-topics");
        assertThat(q.selector()).isEqualTo("sector = 'tech'");
        // The parcels fallback contract: absent topics -> empty list, never null.
        assertThat(q.topics()).isNotNull().isEmpty();
    }

    @Test
    void singleTopic_preserved() throws IOException {
        QueryConfig config = loadYaml("""
                queries:
                  - id: one-topic
                    topics: [ "persistent://public/default/parcel-scans" ]
                    selector: "origin_region = 'APAC'"
                """);

        assertThat(config.queries().get(0).topics())
                .containsExactly("persistent://public/default/parcel-scans");
    }

    @Test
    void multipleTopics_preservedInOrder() throws IOException {
        QueryConfig config = loadYaml("""
                queries:
                  - id: multi-topic
                    topics:
                      - "persistent://public/default/parcel-shipments"
                      - "persistent://public/default/parcel-scans"
                      - "persistent://public/default/parcel-exceptions"
                    selector: "customer_tier = 'BUSINESS'"
                """);

        assertThat(config.queries().get(0).topics()).containsExactly(
                "persistent://public/default/parcel-shipments",
                "persistent://public/default/parcel-scans",
                "persistent://public/default/parcel-exceptions");
    }

    @Test
    void originalStockShape_loadsAllQueries() throws IOException {
        // The pre-parcels {id, selector} shape must still parse unchanged.
        QueryConfig config = loadYaml("""
                queries:
                  - id: tech-stocks
                    selector: "sector = 'tech'"
                  - id: high-value
                    selector: "price > 500"
                """);

        assertThat(config.queries())
                .extracting(QueryConfig.Query::id)
                .containsExactly("tech-stocks", "high-value");
        assertThat(config.queries()).allSatisfy(q -> assertThat(q.topics()).isEmpty());
    }

    @Test
    void topicsList_isImmutable() throws IOException {
        QueryConfig config = loadYaml("""
                queries:
                  - id: one-topic
                    topics: [ "persistent://public/default/parcel-scans" ]
                    selector: "scan_type = 'DELIVERED'"
                """);

        List<String> topics = config.queries().get(0).topics();
        assertThatThrownBy(() -> topics.add("persistent://public/default/sneaky"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void queriesList_isImmutable() throws IOException {
        QueryConfig config = loadYaml("""
                queries:
                  - id: only
                    selector: "x = 1"
                """);

        assertThatThrownBy(() -> config.queries().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void queryRecord_nullTopics_becomesEmptyList() {
        // Direct record construction (not via YAML) must also null-guard.
        QueryConfig.Query q = new QueryConfig.Query("id", "selector", null);
        assertThat(q.topics()).isNotNull().isEmpty();
    }
}
