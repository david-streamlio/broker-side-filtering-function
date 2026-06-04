package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.ValueSchemaInfo;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * One entry in the orchestrator's input-topic registry TableView. Keyed by the input topic FQN
 * (or topicsPattern string) — see {@link RegistryStore#TOPIC_KEY_PREFIX}.
 *
 * <p>Recording {@code keyDeserializer} / {@code valueDeserializer} lets the orchestrator detect
 * schema mismatches across registrations on the same input topic; see {@link #check(FilterRequest)}
 * for the compatibility matrix.
 *
 * <p>{@code activeQueryIds} tracks which queryIds currently hold this filter alive. Idle GC reads
 * three states:
 * <ul>
 *   <li>{@code null} — pre-GC-awareness entry (v1 deployments upgrading). Sweeper skips.</li>
 *   <li>empty set + {@code idleSince != null} — eligible for GC at {@code idleSince + ttl}.</li>
 *   <li>non-empty — in use.</li>
 * </ul>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
final class RegistryEntry {

    private final String controlTopic;
    private final String filterFunctionName;
    private final String keyDeserializer;
    private final String valueDeserializer;
    /**
     * Full value schema forwarded from the originating {@link FilterRequest}, carried so the
     * orchestrator can pass it into each per-function {@code QueryMessage}. Schema-mismatch detection
     * still uses {@link #valueDeserializer} (the schema name); this field is the decode payload.
     * {@code null} for the byte[] / Kafka paths.
     */
    private final ValueSchemaInfo valueSchema;
    private final ClientType clientType;
    private final String createdAt;
    private final Set<String> activeQueryIds;
    private final String idleSince;
    /**
     * Whether this entry's registry key is a topicsPattern (vs a concrete input topic FQN). Needed
     * by the existence reconcile loop to reconstruct the spawn request — {@code topicsPattern}
     * vs {@code inputTopics} — when re-creating a deleted function. {@code false} on read for
     * entries written before this flag existed; such legacy entries are re-spawned as input-topic
     * registrations (the common case), so a pre-flag pattern filter re-spawned after an upgrade
     * would be reconstructed incorrectly — acceptable given pattern use is rare and the flag is
     * written on every fresh REGISTER.
     */
    private final boolean isPattern;

    @JsonCreator
    RegistryEntry(@JsonProperty("controlTopic") String controlTopic,
                  @JsonProperty("filterFunctionName") String filterFunctionName,
                  @JsonProperty("keyDeserializer") String keyDeserializer,
                  @JsonProperty("valueDeserializer") String valueDeserializer,
                  @JsonProperty("valueSchema") ValueSchemaInfo valueSchema,
                  @JsonProperty("clientType") ClientType clientType,
                  @JsonProperty("createdAt") String createdAt,
                  @JsonProperty("activeQueryIds") Set<String> activeQueryIds,
                  @JsonProperty("idleSince") String idleSince,
                  @JsonProperty("isPattern") boolean isPattern) {
        this.controlTopic       = Objects.requireNonNull(controlTopic, "controlTopic");
        this.filterFunctionName = Objects.requireNonNull(filterFunctionName, "filterFunctionName");
        this.keyDeserializer    = keyDeserializer;
        this.valueDeserializer  = valueDeserializer;
        this.valueSchema        = valueSchema;
        this.clientType         = clientType;
        this.createdAt          = createdAt;
        this.activeQueryIds     = activeQueryIds == null
                ? null
                : Collections.unmodifiableSet(new LinkedHashSet<>(activeQueryIds));
        this.idleSince          = idleSince;
        this.isPattern          = isPattern;
    }

    /**
     * 9-arg constructor without a value schema — preserved for existing callers/tests. Delegates
     * to the canonical constructor with {@code valueSchema = null}.
     */
    RegistryEntry(String controlTopic, String filterFunctionName,
                  String keyDeserializer, String valueDeserializer,
                  ClientType clientType, String createdAt,
                  Set<String> activeQueryIds, String idleSince, boolean isPattern) {
        this(controlTopic, filterFunctionName, keyDeserializer, valueDeserializer, null,
                clientType, createdAt, activeQueryIds, idleSince, isPattern);
    }

    /** Legacy 8-arg constructor — pre-dates the isPattern flag; defaults it to {@code false}. */
    RegistryEntry(String controlTopic, String filterFunctionName,
                  String keyDeserializer, String valueDeserializer,
                  ClientType clientType, String createdAt,
                  Set<String> activeQueryIds, String idleSince) {
        this(controlTopic, filterFunctionName, keyDeserializer, valueDeserializer,
                clientType, createdAt, activeQueryIds, idleSince, false);
    }

    /** Legacy 6-arg constructor — used by callers that don't yet write activeQueryIds. */
    RegistryEntry(String controlTopic, String filterFunctionName,
                  String keyDeserializer, String valueDeserializer,
                  ClientType clientType, String createdAt) {
        this(controlTopic, filterFunctionName, keyDeserializer, valueDeserializer,
                clientType, createdAt, null, null, false);
    }

    static RegistryEntry newEntry(String controlTopic, String filterFunctionName,
                                  FilterRequest req) {
        boolean pattern = req.getTopicsPattern() != null && !req.getTopicsPattern().isBlank();
        return new RegistryEntry(controlTopic, filterFunctionName,
                req.getKeyDeserializer(), req.getValueDeserializer(), req.getValueSchema(),
                req.getClientType(), Instant.now().toString(), null, null, pattern);
    }

    public String getControlTopic()                  { return controlTopic; }
    public String getFilterFunctionName()            { return filterFunctionName; }
    public String getKeyDeserializer()               { return keyDeserializer; }
    public String getValueDeserializer()             { return valueDeserializer; }
    public ValueSchemaInfo getValueSchema()          { return valueSchema; }
    public ClientType getClientType()                { return clientType; }
    public String getCreatedAt()                     { return createdAt; }
    public Set<String> getActiveQueryIds()           { return activeQueryIds; }
    public String getIdleSince()                     { return idleSince; }
    // Pin the JSON name: Jackson would otherwise derive "pattern" from the boolean getter and
    // mismatch the @JsonCreator's "isPattern" param (→ always deserializes false).
    @JsonProperty("isPattern")
    public boolean isPattern()                       { return isPattern; }

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
     * registry store. activeQueryIds / idleSince are carried through unchanged — use
     * {@link #withQueryId(String)} and {@link #withoutQueryId(String, Instant)} for those.
     */
    RegistryEntry mergedWith(FilterRequest req) {
        return new RegistryEntry(
                controlTopic,
                filterFunctionName,
                keyDeserializer == null ? req.getKeyDeserializer() : keyDeserializer,
                valueDeserializer == null ? req.getValueDeserializer() : valueDeserializer,
                valueSchema == null ? req.getValueSchema() : valueSchema,
                clientType == null ? req.getClientType() : clientType,
                createdAt,
                activeQueryIds,
                idleSince,
                isPattern);
    }

    /**
     * Return a copy with {@code queryId} added to {@code activeQueryIds}. Clears {@code idleSince}
     * (the filter is no longer idle). If {@code activeQueryIds} was null (pre-GC-awareness entry),
     * this initializes it to a 1-element set, so the entry becomes GC-eligible after subsequent
     * UNREGISTERs.
     */
    RegistryEntry withQueryId(String queryId) {
        Set<String> next = new LinkedHashSet<>();
        if (activeQueryIds != null) next.addAll(activeQueryIds);
        next.add(queryId);
        return new RegistryEntry(controlTopic, filterFunctionName,
                keyDeserializer, valueDeserializer, valueSchema, clientType, createdAt,
                next, null, isPattern);
    }

    /**
     * Return a copy with {@code queryId} removed from {@code activeQueryIds}. If the set becomes
     * empty, {@code idleSince} is set to {@code now} so the sweeper can start the TTL clock.
     * If {@code activeQueryIds} is null this returns the entry unchanged (no-op for pre-GC entries).
     * Removing a queryId not in the set is a silent no-op (idempotent).
     */
    RegistryEntry withoutQueryId(String queryId, Instant now) {
        if (activeQueryIds == null) return this;
        Set<String> next = new LinkedHashSet<>(activeQueryIds);
        next.remove(queryId);
        String nextIdleSince = next.isEmpty() ? now.toString() : null;
        return new RegistryEntry(controlTopic, filterFunctionName,
                keyDeserializer, valueDeserializer, valueSchema, clientType, createdAt,
                next, nextIdleSince, isPattern);
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
                && Objects.equals(valueSchema, that.valueSchema)
                && clientType == that.clientType
                && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(activeQueryIds, that.activeQueryIds)
                && Objects.equals(idleSince, that.idleSince)
                && isPattern == that.isPattern;
    }

    @Override
    public int hashCode() {
        return Objects.hash(controlTopic, filterFunctionName, keyDeserializer,
                valueDeserializer, valueSchema, clientType, createdAt, activeQueryIds, idleSince,
                isPattern);
    }

    @Override
    public String toString() {
        return "RegistryEntry{controlTopic='" + controlTopic
                + "', filterFunctionName='" + filterFunctionName
                + "', keyDeserializer='" + keyDeserializer
                + "', valueDeserializer='" + valueDeserializer
                + "', valueSchema=" + valueSchema
                + ", clientType=" + clientType
                + ", createdAt='" + createdAt
                + "', activeQueryIds=" + activeQueryIds
                + ", idleSince='" + idleSince
                + "', isPattern=" + isPattern + "}";
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
