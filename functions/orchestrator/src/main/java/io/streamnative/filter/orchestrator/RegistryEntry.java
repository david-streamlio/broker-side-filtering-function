package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;

import java.time.Instant;
import java.util.Objects;

/**
 * One entry in the orchestrator's input-topic registry TableView. Keyed by the input topic FQN
 * (or topicsPattern string) — see {@link RegistryStore#TOPIC_KEY_PREFIX}.
 *
 * <p>Recording {@code keyDeserializer} / {@code valueDeserializer} lets the orchestrator detect
 * schema mismatches across registrations on the same input topic; see {@link #check(FilterRequest)}
 * for the compatibility matrix.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class RegistryEntry {

    private final String controlTopic;
    private final String filterFunctionName;
    private final String keyDeserializer;
    private final String valueDeserializer;
    private final ClientType clientType;
    private final String createdAt;

    @JsonCreator
    RegistryEntry(@JsonProperty("controlTopic") String controlTopic,
                  @JsonProperty("filterFunctionName") String filterFunctionName,
                  @JsonProperty("keyDeserializer") String keyDeserializer,
                  @JsonProperty("valueDeserializer") String valueDeserializer,
                  @JsonProperty("clientType") ClientType clientType,
                  @JsonProperty("createdAt") String createdAt) {
        this.controlTopic       = Objects.requireNonNull(controlTopic, "controlTopic");
        this.filterFunctionName = Objects.requireNonNull(filterFunctionName, "filterFunctionName");
        this.keyDeserializer    = keyDeserializer;
        this.valueDeserializer  = valueDeserializer;
        this.clientType         = clientType;
        this.createdAt          = createdAt;
    }

    static RegistryEntry newEntry(String controlTopic, String filterFunctionName,
                                  FilterRequest req) {
        return new RegistryEntry(controlTopic, filterFunctionName,
                req.getKeyDeserializer(), req.getValueDeserializer(), req.getClientType(),
                Instant.now().toString());
    }

    public String getControlTopic()                  { return controlTopic; }
    public String getFilterFunctionName()            { return filterFunctionName; }
    public String getKeyDeserializer()               { return keyDeserializer; }
    public String getValueDeserializer()             { return valueDeserializer; }
    public ClientType getClientType()                { return clientType; }
    public String getCreatedAt()                     { return createdAt; }

    /**
     * Compatibility matrix for serializer fields, applied per-field:
     * <pre>
     *   existing | new    | action
     *   ---------+--------+---------------------------------
     *   null     | null   | accept (no change)
     *   null     | X      | UPGRADE entry (write back)
     *   X        | null   | accept (no change)
     *   X        | X      | accept (no change)
     *   X        | Y      | REJECT (schema mismatch)
     * </pre>
     */
    CompatibilityResult check(FilterRequest req) {
        boolean upgrade = false;
        Mismatch keyMismatch = checkField(keyDeserializer, req.getKeyDeserializer(), "keyDeserializer");
        if (keyMismatch == Mismatch.REJECT) return CompatibilityResult.reject("keyDeserializer mismatch: "
                + "existing=" + keyDeserializer + " new=" + req.getKeyDeserializer());
        if (keyMismatch == Mismatch.UPGRADE) upgrade = true;

        Mismatch valMismatch = checkField(valueDeserializer, req.getValueDeserializer(), "valueDeserializer");
        if (valMismatch == Mismatch.REJECT) return CompatibilityResult.reject("valueDeserializer mismatch: "
                + "existing=" + valueDeserializer + " new=" + req.getValueDeserializer());
        if (valMismatch == Mismatch.UPGRADE) upgrade = true;

        return upgrade ? CompatibilityResult.upgrade() : CompatibilityResult.compatible();
    }

    /**
     * Apply the null→X upgrades described in {@link #check(FilterRequest)}. Existing non-null
     * fields are preserved. Caller is responsible for writing the merged entry back to the
     * registry store.
     */
    RegistryEntry mergedWith(FilterRequest req) {
        return new RegistryEntry(
                controlTopic,
                filterFunctionName,
                keyDeserializer == null ? req.getKeyDeserializer() : keyDeserializer,
                valueDeserializer == null ? req.getValueDeserializer() : valueDeserializer,
                clientType == null ? req.getClientType() : clientType,
                createdAt);
    }

    private enum Mismatch { NONE, UPGRADE, REJECT }

    private static Mismatch checkField(String existing, String incoming, String name) {
        if (existing == null && incoming == null) return Mismatch.NONE;
        if (existing == null) return Mismatch.UPGRADE;          // null → X
        if (incoming == null) return Mismatch.NONE;             // X → null is "not asserted" by caller
        if (existing.equals(incoming)) return Mismatch.NONE;    // X → X
        return Mismatch.REJECT;                                  // X → Y mismatch
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RegistryEntry that)) return false;
        return Objects.equals(controlTopic, that.controlTopic)
                && Objects.equals(filterFunctionName, that.filterFunctionName)
                && Objects.equals(keyDeserializer, that.keyDeserializer)
                && Objects.equals(valueDeserializer, that.valueDeserializer)
                && clientType == that.clientType
                && Objects.equals(createdAt, that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(controlTopic, filterFunctionName, keyDeserializer,
                valueDeserializer, clientType, createdAt);
    }

    @Override
    public String toString() {
        return "RegistryEntry{controlTopic='" + controlTopic
                + "', filterFunctionName='" + filterFunctionName
                + "', keyDeserializer='" + keyDeserializer
                + "', valueDeserializer='" + valueDeserializer
                + "', clientType=" + clientType
                + ", createdAt='" + createdAt + "'}";
    }

    /** Tri-state result from {@link RegistryEntry#check(FilterRequest)}. */
    static final class CompatibilityResult {
        enum Kind { COMPATIBLE, UPGRADE_NEEDED, REJECT }

        private final Kind kind;
        private final String reason;

        private CompatibilityResult(Kind kind, String reason) {
            this.kind = kind;
            this.reason = reason;
        }

        static CompatibilityResult compatible() { return new CompatibilityResult(Kind.COMPATIBLE, null); }
        static CompatibilityResult upgrade()    { return new CompatibilityResult(Kind.UPGRADE_NEEDED, null); }
        static CompatibilityResult reject(String reason) { return new CompatibilityResult(Kind.REJECT, reason); }

        boolean isCompatible()    { return kind == Kind.COMPATIBLE; }
        boolean needsUpgrade()    { return kind == Kind.UPGRADE_NEEDED; }
        boolean isRejected()      { return kind == Kind.REJECT; }
        String reason()           { return reason; }
    }
}
