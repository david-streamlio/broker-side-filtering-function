package io.streamnative.filter.client;

import java.util.List;
import java.util.Objects;

/**
 * Immutable handle for a query registered with the broker-side filter function. Returned from
 * {@link Session#registerPrivateQuery(List, String)}; passed back to
 * {@link Session#updatePrivateQuery(PrivateQuery, String)} and
 * {@link Session#retirePrivateQuery(PrivateQuery)} for lifecycle operations.
 *
 * <p>Both the Pulsar SDK ({@link FilterableConsumerBuilder}) and the Kafka SDK in the
 * {@code client-kafka} module use this as the contract between the registration layer (owned by
 * {@link Session}) and the per-transport consumer wrappers.
 */
public final class PrivateQuery {

    private final String queryId;
    private final String privateTopic;
    private final List<String> sourceTopics;
    private final String topicConstraint;

    PrivateQuery(String queryId, String privateTopic, List<String> sourceTopics, String topicConstraint) {
        this.queryId         = Objects.requireNonNull(queryId, "queryId");
        this.privateTopic    = Objects.requireNonNull(privateTopic, "privateTopic");
        this.sourceTopics    = List.copyOf(sourceTopics);
        this.topicConstraint = Objects.requireNonNull(topicConstraint, "topicConstraint");
    }

    /** Stable identifier; same value used to publish, update, and retract the query. */
    public String queryId() {
        return queryId;
    }

    /**
     * Fully-qualified Pulsar topic the function writes matching messages to. Per-transport
     * wrappers subscribe to this topic (or its KoP-mapped Kafka name).
     */
    public String privateTopic() {
        return privateTopic;
    }

    /** The list of source topics the user requested, in builder-supplied order. */
    public List<String> sourceTopics() {
        return sourceTopics;
    }

    /**
     * The {@code __topic__ = '...'} or {@code __topic__ IN (...)} fragment composed with the
     * user selector. Wrappers re-use it when republishing on {@code setSelector}.
     */
    public String topicConstraint() {
        return topicConstraint;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PrivateQuery that)) return false;
        return queryId.equals(that.queryId)
                && privateTopic.equals(that.privateTopic)
                && sourceTopics.equals(that.sourceTopics)
                && topicConstraint.equals(that.topicConstraint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, privateTopic, sourceTopics, topicConstraint);
    }

    @Override
    public String toString() {
        return "PrivateQuery{queryId='" + queryId + "', privateTopic='" + privateTopic
                + "', sourceTopics=" + sourceTopics + "}";
    }
}
