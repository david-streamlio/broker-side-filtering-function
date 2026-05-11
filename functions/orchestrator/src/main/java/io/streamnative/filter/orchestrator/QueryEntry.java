package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Index entry stored under {@code q:<queryId>} in the orchestrator's state TableView. Records the
 * SDK-supplied outputTopic plus the list of per-function control topics where the queryId was
 * published — needed for MODIFY (to reconstruct the downstream QueryMessage) and UNREGISTER fan-out.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class QueryEntry {

    private final String outputTopic;
    private final List<String> controlTopics;

    @JsonCreator
    QueryEntry(@JsonProperty("outputTopic") String outputTopic,
               @JsonProperty("controlTopics") List<String> controlTopics) {
        this.outputTopic   = Objects.requireNonNull(outputTopic, "outputTopic");
        this.controlTopics = List.copyOf(controlTopics);
    }

    public String getOutputTopic()         { return outputTopic; }
    public List<String> getControlTopics() { return controlTopics; }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryEntry that)) return false;
        return Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(controlTopics, that.controlTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputTopic, controlTopics);
    }

    @Override
    public String toString() {
        return "QueryEntry{outputTopic='" + outputTopic + "', controlTopics=" + controlTopics + '}';
    }
}
