package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
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
 *
 * <p>{@code lastSeen} (ISO-8601 Instant string) is stamped by the orchestrator on
 * REGISTER / MODIFY / HEARTBEAT and used by the sweeper for dead-consumer detection: a queryId
 * whose {@code lastSeen} is older than {@code heartbeat-max-stale-seconds} is treated as dead and
 * its filters get a synthesized UNREGISTER. {@code null} on read for entries written before
 * heartbeat support — those queryIds will be refreshed on first MODIFY or HEARTBEAT they receive,
 * or persist forever if neither happens (no false-positive dead detection).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class QueryEntry {

    private final String outputTopic;
    private final List<String> controlTopics;
    private final List<String> registryKeys;
    private final String lastSeen;

    @JsonCreator
    QueryEntry(@JsonProperty("outputTopic") String outputTopic,
               @JsonProperty("controlTopics") List<String> controlTopics,
               @JsonProperty("registryKeys") List<String> registryKeys,
               @JsonProperty("lastSeen") String lastSeen) {
        this.outputTopic   = Objects.requireNonNull(outputTopic, "outputTopic");
        this.controlTopics = List.copyOf(controlTopics);
        this.registryKeys  = registryKeys == null ? null : List.copyOf(registryKeys);
        this.lastSeen      = lastSeen;
    }

    /** 3-arg constructor — used by callers that don't yet stamp lastSeen. */
    QueryEntry(String outputTopic, List<String> controlTopics, List<String> registryKeys) {
        this(outputTopic, controlTopics, registryKeys, null);
    }

    /** Legacy 2-arg constructor — used by callers that don't yet write registryKeys. */
    QueryEntry(String outputTopic, List<String> controlTopics) {
        this(outputTopic, controlTopics, null, null);
    }

    public String getOutputTopic()         { return outputTopic; }
    public List<String> getControlTopics() { return controlTopics; }
    public List<String> getRegistryKeys()  { return registryKeys; }
    public String getLastSeen()            { return lastSeen; }

    /**
     * Return a copy with {@code lastSeen} stamped to {@code now}. Used by REGISTER (creation),
     * MODIFY (touch), and HEARTBEAT (touch). All other fields are preserved.
     */
    QueryEntry withLastSeen(Instant now) {
        return new QueryEntry(outputTopic, controlTopics, registryKeys, now.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryEntry that)) return false;
        return Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(controlTopics, that.controlTopics)
                && Objects.equals(registryKeys, that.registryKeys)
                && Objects.equals(lastSeen, that.lastSeen);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputTopic, controlTopics, registryKeys, lastSeen);
    }

    @Override
    public String toString() {
        return "QueryEntry{outputTopic='" + outputTopic
                + "', controlTopics=" + controlTopics
                + ", registryKeys=" + registryKeys
                + ", lastSeen='" + lastSeen + "'}";
    }
}
