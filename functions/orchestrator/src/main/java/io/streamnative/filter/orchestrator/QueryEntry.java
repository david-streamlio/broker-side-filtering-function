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
 *
 * <p>{@code registryKeys} is parallel to {@code controlTopics}: index {@code i} is the
 * {@code t:}-keyspace key whose RegistryEntry owns {@code controlTopics[i]}. Idle filter GC uses
 * this to do O(1) updates to {@code activeQueryIds} during UNREGISTER. {@code null} on read for
 * pre-GC-awareness entries (v1 deployments upgrading) — callers fall back to today's behavior.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class QueryEntry {

    private final String outputTopic;
    private final List<String> controlTopics;
    private final List<String> registryKeys;

    @JsonCreator
    QueryEntry(@JsonProperty("outputTopic") String outputTopic,
               @JsonProperty("controlTopics") List<String> controlTopics,
               @JsonProperty("registryKeys") List<String> registryKeys) {
        this.outputTopic   = Objects.requireNonNull(outputTopic, "outputTopic");
        this.controlTopics = List.copyOf(controlTopics);
        this.registryKeys  = registryKeys == null ? null : List.copyOf(registryKeys);
    }

    /** Legacy 2-arg constructor — used by callers that don't yet write registryKeys. */
    QueryEntry(String outputTopic, List<String> controlTopics) {
        this(outputTopic, controlTopics, null);
    }

    public String getOutputTopic()         { return outputTopic; }
    public List<String> getControlTopics() { return controlTopics; }
    public List<String> getRegistryKeys()  { return registryKeys; }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryEntry that)) return false;
        return Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(controlTopics, that.controlTopics)
                && Objects.equals(registryKeys, that.registryKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputTopic, controlTopics, registryKeys);
    }

    @Override
    public String toString() {
        return "QueryEntry{outputTopic='" + outputTopic
                + "', controlTopics=" + controlTopics
                + ", registryKeys=" + registryKeys + '}';
    }
}
