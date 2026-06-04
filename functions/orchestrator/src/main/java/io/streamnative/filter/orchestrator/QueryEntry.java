package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.streamnative.filter.common.ValueSchemaInfo;

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
 *
 * <p>{@code selector} (the normalized JMS selector, never null on entries written from
 * {@link OrchestratorFunction#handleRegister}/{@code handleModify}) is persisted so the existence
 * reconcile loop can rebuild a per-function control topic that was lost alongside its function —
 * republishing the same {@code QueryMessage} the SDK originally produced. {@code null} on read for
 * entries written before selector persistence; the reconciler falls back to the identity selector
 * and logs that the original was unrecoverable.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class QueryEntry {

    private final String outputTopic;
    private final List<String> controlTopics;
    private final List<String> registryKeys;
    private final String lastSeen;
    private final String selector;
    /**
     * Value schema forwarded to the per-function {@code QueryMessage}. Persisted on the QueryEntry
     * (not just the RegistryEntry) because MODIFY and the existence-reconcile loop republish the
     * QueryMessage from this entry and must preserve the schema across hot-reloads. {@code null} for
     * the byte[] / Kafka paths.
     */
    private final ValueSchemaInfo valueSchema;

    @JsonCreator
    QueryEntry(@JsonProperty("outputTopic") String outputTopic,
               @JsonProperty("controlTopics") List<String> controlTopics,
               @JsonProperty("registryKeys") List<String> registryKeys,
               @JsonProperty("lastSeen") String lastSeen,
               @JsonProperty("selector") String selector,
               @JsonProperty("valueSchema") ValueSchemaInfo valueSchema) {
        this.outputTopic   = Objects.requireNonNull(outputTopic, "outputTopic");
        this.controlTopics = List.copyOf(controlTopics);
        this.registryKeys  = registryKeys == null ? null : List.copyOf(registryKeys);
        this.lastSeen      = lastSeen;
        this.selector      = selector;
        this.valueSchema   = valueSchema;
    }

    /** 5-arg constructor without a value schema — preserved for existing callers/tests. */
    QueryEntry(String outputTopic, List<String> controlTopics, List<String> registryKeys,
               String lastSeen, String selector) {
        this(outputTopic, controlTopics, registryKeys, lastSeen, selector, null);
    }

    /** 4-arg constructor — used by callers that don't yet persist the selector. */
    QueryEntry(String outputTopic, List<String> controlTopics, List<String> registryKeys,
               String lastSeen) {
        this(outputTopic, controlTopics, registryKeys, lastSeen, null);
    }

    /** 3-arg constructor — used by callers that don't yet stamp lastSeen. */
    QueryEntry(String outputTopic, List<String> controlTopics, List<String> registryKeys) {
        this(outputTopic, controlTopics, registryKeys, null, null);
    }

    /** Legacy 2-arg constructor — used by callers that don't yet write registryKeys. */
    QueryEntry(String outputTopic, List<String> controlTopics) {
        this(outputTopic, controlTopics, null, null, null);
    }

    public String getOutputTopic()         { return outputTopic; }
    public List<String> getControlTopics() { return controlTopics; }
    public List<String> getRegistryKeys()  { return registryKeys; }
    public String getLastSeen()            { return lastSeen; }
    public String getSelector()            { return selector; }
    public ValueSchemaInfo getValueSchema() { return valueSchema; }

    /**
     * Return a copy with {@code lastSeen} stamped to {@code now}. Used by REGISTER (creation),
     * MODIFY (touch), and HEARTBEAT (touch). All other fields are preserved.
     */
    QueryEntry withLastSeen(Instant now) {
        return new QueryEntry(outputTopic, controlTopics, registryKeys, now.toString(), selector,
                valueSchema);
    }

    /**
     * Return a copy with {@code selector} replaced. Used by MODIFY so the persisted selector stays
     * authoritative for the reconciler's control-topic repopulation. {@code lastSeen} is preserved
     * — callers that also want a liveness touch chain {@link #withLastSeen(Instant)}.
     */
    QueryEntry withSelector(String newSelector) {
        return new QueryEntry(outputTopic, controlTopics, registryKeys, lastSeen, newSelector,
                valueSchema);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof QueryEntry that)) return false;
        return Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(controlTopics, that.controlTopics)
                && Objects.equals(registryKeys, that.registryKeys)
                && Objects.equals(lastSeen, that.lastSeen)
                && Objects.equals(selector, that.selector)
                && Objects.equals(valueSchema, that.valueSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputTopic, controlTopics, registryKeys, lastSeen, selector, valueSchema);
    }

    @Override
    public String toString() {
        return "QueryEntry{outputTopic='" + outputTopic
                + "', controlTopics=" + controlTopics
                + ", registryKeys=" + registryKeys
                + ", lastSeen='" + lastSeen
                + "', selector='" + selector
                + "', valueSchema=" + valueSchema + "}";
    }
}
