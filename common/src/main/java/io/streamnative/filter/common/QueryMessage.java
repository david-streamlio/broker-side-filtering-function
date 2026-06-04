package io.streamnative.filter.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class QueryMessage {

    private final String name;
    private final String selector;
    private final String outputTopic;
    private final ValueSchemaInfo valueSchema;

    @JsonCreator
    public QueryMessage(@JsonProperty("name") String name,
                        @JsonProperty("selector") String selector,
                        @JsonProperty("outputTopic") String outputTopic,
                        @JsonProperty("valueSchema") ValueSchemaInfo valueSchema) {
        this.name = name;
        this.selector = selector;
        this.outputTopic = outputTopic;
        this.valueSchema = valueSchema;
    }

    public String getName() {
        return name;
    }

    public String getSelector() {
        return selector;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    /**
     * The consumer's value schema, forwarded by the orchestrator so the filter function can decode
     * the message value for schema-aware filtering. {@code null} when the registering SDK supplied
     * no value schema (e.g. the byte[] / Kafka paths).
     */
    public ValueSchemaInfo getValueSchema() {
        return valueSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryMessage that)) return false;
        return Objects.equals(name, that.name)
                && Objects.equals(selector, that.selector)
                && Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(valueSchema, that.valueSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, selector, outputTopic, valueSchema);
    }

    @Override
    public String toString() {
        return "QueryMessage{name='" + name + "', selector='" + selector + "', outputTopic='"
                + outputTopic + "', valueSchema=" + valueSchema + '}';
    }
}
