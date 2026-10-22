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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Loads {@code queries.yaml} from the demo classpath or filesystem.
 * The YAML shape is one list of {@code {id, selector, topics?}} objects.
 * Selectors are passed through to the SDK without parsing on this side; the
 * orchestrator's selector-filter is the canonical evaluator.
 *
 * <p>{@code topics} is optional. When present and non-empty, the demo
 * registers the query against every listed source topic (the SDK composes
 * a {@code __topic__ IN (...)} constraint into the selector). When absent
 * or empty, the demo falls back to the single {@code DEMO_SOURCE_TOPIC}
 * env var — matching the stock-quotes pattern so the original
 * {@code queries.yaml} stays valid without edits.
 */
public record QueryConfig(List<Query> queries) {

    public record Query(String id, String selector, List<String> topics) {
        @JsonCreator
        public Query(@JsonProperty("id") String id,
                     @JsonProperty("selector") String selector,
                     @JsonProperty("topics") List<String> topics) {
            this.id = id;
            this.selector = selector;
            this.topics = (topics == null) ? List.of() : List.copyOf(topics);
        }
    }

    @JsonCreator
    public QueryConfig(@JsonProperty("queries") List<Query> queries) {
        this.queries = List.copyOf(queries);
    }

    public static QueryConfig load(Path path) throws IOException {
        ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
        return yaml.readValue(Files.readAllBytes(path), QueryConfig.class);
    }
}
