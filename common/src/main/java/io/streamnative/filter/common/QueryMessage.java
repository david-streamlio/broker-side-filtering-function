package io.streamnative.filter.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class QueryMessage {

    private final String name;
    private final String selector;
    private final String outputTopic;

    @JsonCreator
    public QueryMessage(@JsonProperty("name") String name,
                        @JsonProperty("selector") String selector,
                        @JsonProperty("outputTopic") String outputTopic) {
        this.name = name;
        this.selector = selector;
        this.outputTopic = outputTopic;
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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryMessage that)) return false;
        return Objects.equals(name, that.name)
                && Objects.equals(selector, that.selector)
                && Objects.equals(outputTopic, that.outputTopic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, selector, outputTopic);
    }

    @Override
    public String toString() {
        return "QueryMessage{name='" + name + "', selector='" + selector + "', outputTopic='" + outputTopic + "'}";
    }
}
