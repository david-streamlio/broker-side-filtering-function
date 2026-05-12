package io.streamnative.filter.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * SDK→Orchestrator wire-format record. Published to the orchestrator's input topic; the message key
 * is the SDK-generated queryId (UUIDv4 string). The orchestrator parses one of these per message,
 * routes by {@link #getCommand()}, and translates accepted REGISTER/MODIFY into per-filter-function
 * {@link QueryMessage}s downstream (UNREGISTER fans out tombstones).
 *
 * <p>See {@code protocol/filter-request.schema.json} for the wire contract and command-specific
 * field rules. Mirrored here in {@link #validate()}.
 *
 * <p>The class is immutable. Unknown JSON fields are tolerated for forward-compat.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class FilterRequest {

    private final Command command;
    private final List<String> inputTopics;
    private final String topicsPattern;
    private final String outputTopic;
    private final String selector;
    private final String keyDeserializer;
    private final String valueDeserializer;
    private final ClientType clientType;
    private final List<String> queryIds;

    @JsonCreator
    public FilterRequest(@JsonProperty("command") Command command,
                         @JsonProperty("inputTopics") List<String> inputTopics,
                         @JsonProperty("topicsPattern") String topicsPattern,
                         @JsonProperty("outputTopic") String outputTopic,
                         @JsonProperty("selector") String selector,
                         @JsonProperty("keyDeserializer") String keyDeserializer,
                         @JsonProperty("valueDeserializer") String valueDeserializer,
                         @JsonProperty("clientType") ClientType clientType,
                         @JsonProperty("queryIds") List<String> queryIds) {
        this.command           = command;
        this.inputTopics       = inputTopics == null ? null : List.copyOf(inputTopics);
        this.topicsPattern     = topicsPattern;
        this.outputTopic       = outputTopic;
        this.selector          = selector;
        this.keyDeserializer   = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.clientType        = clientType;
        this.queryIds          = queryIds == null ? null : List.copyOf(queryIds);
    }

    // ---------- factory helpers (typed per command — simpler call sites in SDK) ----------

    /** Build a REGISTER request for a single input topic. */
    public static FilterRequest register(String inputTopic, String outputTopic, String selector,
                                         String keyDeserializer, String valueDeserializer,
                                         ClientType clientType) {
        return new FilterRequest(Command.REGISTER,
                Collections.singletonList(inputTopic), null,
                outputTopic, selector, keyDeserializer, valueDeserializer, clientType, null);
    }

    /** Build a REGISTER request for multiple input topics. */
    public static FilterRequest register(List<String> inputTopics, String outputTopic, String selector,
                                         String keyDeserializer, String valueDeserializer,
                                         ClientType clientType) {
        return new FilterRequest(Command.REGISTER,
                inputTopics, null,
                outputTopic, selector, keyDeserializer, valueDeserializer, clientType, null);
    }

    /** Build a REGISTER request matching a regex pattern. */
    public static FilterRequest registerPattern(String topicsPattern, String outputTopic, String selector,
                                                String keyDeserializer, String valueDeserializer,
                                                ClientType clientType) {
        return new FilterRequest(Command.REGISTER,
                null, topicsPattern,
                outputTopic, selector, keyDeserializer, valueDeserializer, clientType, null);
    }

    /** Build a MODIFY request replacing the selector on an existing queryId. */
    public static FilterRequest modify(String newSelector) {
        return new FilterRequest(Command.MODIFY, null, null, null, newSelector, null, null, null, null);
    }

    /** Build an UNREGISTER request retracting an existing queryId. */
    public static FilterRequest unregister() {
        return new FilterRequest(Command.UNREGISTER, null, null, null, null, null, null, null, null);
    }

    /**
     * Build a HEARTBEAT request for the given queryIds. One Session publishes one HEARTBEAT per
     * heartbeat tick containing all of its live queryIds, so the orchestrator can refresh their
     * {@code lastSeen} timestamps in a single message.
     */
    public static FilterRequest heartbeat(List<String> queryIds) {
        return new FilterRequest(Command.HEARTBEAT, null, null, null, null, null, null, null,
                queryIds);
    }

    // ---------- getters ----------

    public Command getCommand()           { return command; }
    public List<String> getInputTopics()  { return inputTopics; }
    public String getTopicsPattern()      { return topicsPattern; }
    public String getOutputTopic()        { return outputTopic; }
    public String getSelector()           { return selector; }
    public String getKeyDeserializer()    { return keyDeserializer; }
    public String getValueDeserializer()  { return valueDeserializer; }
    public ClientType getClientType()     { return clientType; }
    public List<String> getQueryIds()     { return queryIds; }

    // ---------- validation ----------

    /**
     * Validate command-specific field requirements. Throws {@link IllegalArgumentException} with a
     * message identifying the violated rule. Matches the {@code protocol/filter-request.schema.json}
     * contract one-for-one.
     */
    public void validate() {
        if (command == null) {
            throw new IllegalArgumentException("command is required");
        }
        switch (command) {
            case REGISTER -> validateRegister();
            case MODIFY   -> validateModify();
            case UNREGISTER -> validateUnregister();
            case HEARTBEAT -> validateHeartbeat();
        }
    }

    private void validateRegister() {
        boolean hasInputs  = inputTopics != null && !inputTopics.isEmpty();
        boolean hasPattern = topicsPattern != null && !topicsPattern.isBlank();
        if (hasInputs == hasPattern) {
            throw new IllegalArgumentException(
                    "REGISTER requires exactly one of inputTopics (non-empty) or topicsPattern (non-blank)");
        }
        if (hasInputs) {
            for (String t : inputTopics) {
                if (t == null || t.isBlank()) {
                    throw new IllegalArgumentException("REGISTER inputTopics contains a null/blank entry");
                }
            }
        }
        if (outputTopic == null || outputTopic.isBlank()) {
            throw new IllegalArgumentException("REGISTER requires outputTopic");
        }
        // selector may be null/blank — interpreted as "no user-side filter; match every record
        // arriving at the input topic". The orchestrator translates this to a JMS-selector
        // identity ("1=1") when constructing the downstream QueryMessage so the filter function
        // never sees a null selector.
        if (clientType == null) {
            throw new IllegalArgumentException("REGISTER requires clientType");
        }
    }

    private void validateModify() {
        if (selector == null || selector.isBlank()) {
            throw new IllegalArgumentException("MODIFY requires selector");
        }
    }

    private void validateUnregister() {
        // No required fields. Extra fields are tolerated (ignored downstream).
    }

    private void validateHeartbeat() {
        if (queryIds == null || queryIds.isEmpty()) {
            throw new IllegalArgumentException("HEARTBEAT requires a non-empty queryIds list");
        }
        for (String id : queryIds) {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("HEARTBEAT queryIds contains a null/blank entry");
            }
        }
    }

    // ---------- equality / debug ----------

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof FilterRequest that)) return false;
        return command == that.command
                && Objects.equals(inputTopics, that.inputTopics)
                && Objects.equals(topicsPattern, that.topicsPattern)
                && Objects.equals(outputTopic, that.outputTopic)
                && Objects.equals(selector, that.selector)
                && Objects.equals(keyDeserializer, that.keyDeserializer)
                && Objects.equals(valueDeserializer, that.valueDeserializer)
                && clientType == that.clientType
                && Objects.equals(queryIds, that.queryIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(command, inputTopics, topicsPattern, outputTopic, selector,
                keyDeserializer, valueDeserializer, clientType, queryIds);
    }

    @Override
    public String toString() {
        return "FilterRequest{command=" + command
                + ", inputTopics=" + inputTopics
                + ", topicsPattern='" + topicsPattern + '\''
                + ", outputTopic='" + outputTopic + '\''
                + ", selector='" + selector + '\''
                + ", keyDeserializer='" + keyDeserializer + '\''
                + ", valueDeserializer='" + valueDeserializer + '\''
                + ", clientType=" + clientType
                + ", queryIds=" + queryIds
                + '}';
    }
}
