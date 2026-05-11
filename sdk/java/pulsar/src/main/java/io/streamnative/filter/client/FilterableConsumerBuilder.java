package io.streamnative.filter.client;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Fluent builder for a filtered Pulsar consumer. Returned by {@link Session#newConsumer()}.
 *
 * <p>The builder mirrors the most common subset of {@code org.apache.pulsar.client.api.ConsumerBuilder}
 * configuration. The terminal {@link #subscribe()} call returns a standard
 * {@link org.apache.pulsar.client.api.Consumer Consumer&lt;byte[]&gt;} that ALSO implements
 * {@link FilterControl} — callers cast when they want dynamic-selector ops.
 *
 * <p>Required configuration: at least one source topic. The {@code selector} may be omitted
 * (or {@code null}/blank) — in that case the consumer only filters on the topic constraint.
 */
public final class FilterableConsumerBuilder {

    private final Session session;
    private final List<String> sourceTopics = new ArrayList<>();
    private String selector;
    private String subscriptionName;
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;
    private SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Earliest;

    FilterableConsumerBuilder(Session session) {
        this.session = Objects.requireNonNull(session, "session");
    }

    /**
     * Set a single source topic. Replaces any previously-configured topics.
     */
    public FilterableConsumerBuilder topic(String topic) {
        Objects.requireNonNull(topic, "topic");
        sourceTopics.clear();
        sourceTopics.add(topic);
        return this;
    }

    /**
     * Set multiple source topics. The composed selector will use {@code __topic__ IN (...)}.
     * Replaces any previously-configured topics.
     */
    public FilterableConsumerBuilder topics(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }
        sourceTopics.clear();
        sourceTopics.addAll(topics);
        return this;
    }

    /**
     * Set the user-side JMS selector. May be {@code null} or blank to filter only on the
     * topic constraint.
     */
    public FilterableConsumerBuilder selector(String selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Subscription name used on the private output topic. Defaults to a unique
     * {@code session-<uuid>} value.
     */
    public FilterableConsumerBuilder subscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }

    /** Subscription type for the underlying consumer. Defaults to {@link SubscriptionType#Exclusive}. */
    public FilterableConsumerBuilder subscriptionType(SubscriptionType type) {
        this.subscriptionType = Objects.requireNonNull(type, "subscriptionType");
        return this;
    }

    /**
     * Initial position for the underlying subscription. Defaults to
     * {@link SubscriptionInitialPosition#Earliest}.
     */
    public FilterableConsumerBuilder subscriptionInitialPosition(SubscriptionInitialPosition pos) {
        this.initialPosition = Objects.requireNonNull(pos, "subscriptionInitialPosition");
        return this;
    }

    /**
     * Subscribe a real Pulsar consumer on the private output topic. The returned object is a
     * {@link Consumer Consumer&lt;byte[]&gt;} that also implements {@link FilterControl}.
     *
     * @throws IllegalArgumentException if {@code selector} is non-blank but fails to parse
     *                                  against the JMS-selector grammar; the cause is the
     *                                  underlying {@code jakarta.jms.JMSException}.
     * @throws PulsarClientException    if the underlying Pulsar subscribe or control-topic
     *                                  publish fails.
     */
    public Consumer<byte[]> subscribe() throws PulsarClientException {
        if (sourceTopics.isEmpty()) {
            throw new IllegalStateException("At least one source topic is required");
        }
        return session.subscribeFiltered(
                List.copyOf(sourceTopics),
                selector,
                subscriptionName,
                subscriptionType,
                initialPosition);
    }
}
