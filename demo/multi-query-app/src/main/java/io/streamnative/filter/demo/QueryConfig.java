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
