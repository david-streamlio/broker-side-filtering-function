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
 * The YAML shape is intentionally minimal — one list of {@code {id, selector}}
 * objects. Selectors are passed through to the SDK without parsing on this
 * side; the orchestrator's selector-filter is the canonical evaluator.
 */
public record QueryConfig(List<Query> queries) {

    public record Query(String id, String selector) {
        @JsonCreator
        public Query(@JsonProperty("id") String id,
                     @JsonProperty("selector") String selector) {
            this.id = id;
            this.selector = selector;
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
