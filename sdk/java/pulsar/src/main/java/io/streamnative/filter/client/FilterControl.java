package io.streamnative.filter.client;

import org.apache.pulsar.client.api.PulsarClientException;

import java.util.List;

/**
 * Dynamic-selector operations exposed by consumers returned from
 * {@link FilterableConsumerBuilder#subscribe()}.
 *
 * <p>The returned {@link org.apache.pulsar.client.api.Consumer} also implements this interface;
 * callers cast when they want to inspect or mutate the active selector:
 *
 * <pre>{@code
 * Consumer<byte[]> c = session.newConsumer().topic(...).selector("a = 'x'").subscribe();
 * ((FilterControl) c).setSelector("a = 'y'");
 * }</pre>
 *
 * <p>Single-selector model: each consumer has exactly one user-side selector at a time.
 * {@link #setSelector(String)} replaces it. The underlying {@code queryId} is stable for the
 * lifetime of the consumer — the function module performs in-place hot-reload when the same
 * key is re-published to the control topic.
 */
public interface FilterControl {

    /**
     * Replace the active user-side selector. The session re-composes the effective selector as
     * {@code <topic-constraint> AND (<selector>)} and republishes a {@link
     * io.streamnative.filter.common.QueryMessage} on the same {@link #queryId()} key, prompting
     * the function to swap in the new compiled expression.
     *
     * @param selector a JMS selector expression, or {@code null}/blank to drop the user-side
     *                 filter (leaves only the topic constraint active).
     * @throws PulsarClientException if the publish to the control topic fails.
     */
    void setSelector(String selector) throws PulsarClientException;

    /**
     * The current user-side selector (the portion AFTER the topic constraint). May be
     * {@code null} when no user-side filter is active.
     */
    String currentSelector();

    /**
     * Stable identifier for this consumer's registered query. Same value before and after
     * {@link #setSelector(String)}.
     */
    String queryId();

    /** Source topics this consumer is filtering, in builder-supplied order. */
    List<String> sourceTopics();

    /** Private output topic the underlying Pulsar consumer is subscribed to. */
    String privateTopic();
}
