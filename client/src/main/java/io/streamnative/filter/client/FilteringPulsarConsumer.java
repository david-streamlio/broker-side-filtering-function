package io.streamnative.filter.client;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerStats;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Wraps a real Pulsar {@link Consumer} subscribed to a private filtered topic, and adds
 * {@link FilterControl} so callers can swap the active selector after construction.
 *
 * <p>All {@link Consumer} methods delegate to the underlying consumer. {@link #close()} additionally
 * tombstones the query on the control topic and (per policy) deletes the private output topic.
 */
final class FilteringPulsarConsumer implements Consumer<byte[]>, FilterControl {

    private static final Logger LOG = LoggerFactory.getLogger(FilteringPulsarConsumer.class);

    private final Session session;
    private final Consumer<byte[]> delegate;
    private final String queryId;
    private final String privateTopic;
    private final List<String> sourceTopics;
    private final String topicConstraint;
    private volatile String userSelector;
    private volatile boolean closed;

    FilteringPulsarConsumer(Session session,
                            Consumer<byte[]> delegate,
                            String queryId,
                            String privateTopic,
                            List<String> sourceTopics,
                            String topicConstraint,
                            String userSelector) {
        this.session         = session;
        this.delegate        = delegate;
        this.queryId         = queryId;
        this.privateTopic    = privateTopic;
        this.sourceTopics    = List.copyOf(sourceTopics);
        this.topicConstraint = topicConstraint;
        this.userSelector    = userSelector;
    }

    // ------------------------------------------------------------------ FilterControl

    @Override
    public synchronized void setSelector(String selector) throws PulsarClientException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        Selectors.validate(selector);
        String composed = Selectors.combine(topicConstraint, selector);
        session.republishQuery(queryId, composed, privateTopic);
        this.userSelector = (selector == null || selector.isBlank()) ? null : selector;
        LOG.info("Updated selector queryId={} selector=[{}]", queryId, composed);
    }

    @Override
    public String currentSelector() {
        return userSelector;
    }

    @Override
    public String queryId() {
        return queryId;
    }

    @Override
    public List<String> sourceTopics() {
        return sourceTopics;
    }

    @Override
    public String privateTopic() {
        return privateTopic;
    }

    // ------------------------------------------------------------------ Consumer — lifecycle

    @Override
    public synchronized void close() throws PulsarClientException {
        if (closed) return;
        closed = true;
        PulsarClientException closeError = null;
        try {
            delegate.close();
        } catch (PulsarClientException e) {
            closeError = e;
            LOG.warn("Failed to close underlying consumer for queryId={}: {}", queryId, e.getMessage());
        }
        // Always retract + (per policy) delete the private topic, even if delegate.close() failed.
        session.retractAndCleanup(queryId, privateTopic);
        if (closeError != null) {
            throw closeError;
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (this) {
            if (closed) return CompletableFuture.completedFuture(null);
            closed = true;
        }
        return delegate.closeAsync()
                .whenComplete((v, t) -> session.retractAndCleanup(queryId, privateTopic));
    }

    // ------------------------------------------------------------------ Consumer — identity / state

    @Override public String getTopic()                { return delegate.getTopic(); }
    @Override public String getSubscription()         { return delegate.getSubscription(); }
    @Override public String getConsumerName()         { return delegate.getConsumerName(); }
    @Override public ConsumerStats getStats()         { return delegate.getStats(); }
    @Override public boolean isConnected()            { return delegate.isConnected(); }
    @Override public boolean hasReachedEndOfTopic()   { return delegate.hasReachedEndOfTopic(); }
    @Override public long getLastDisconnectedTimestamp() { return delegate.getLastDisconnectedTimestamp(); }

    @Override public void pause()                     { delegate.pause(); }
    @Override public void resume()                    { delegate.resume(); }

    // ------------------------------------------------------------------ Consumer — unsubscribe

    @Override public void unsubscribe() throws PulsarClientException { delegate.unsubscribe(); }
    @Override public CompletableFuture<Void> unsubscribeAsync()      { return delegate.unsubscribeAsync(); }
    @Override public void unsubscribe(boolean force) throws PulsarClientException { delegate.unsubscribe(force); }
    @Override public CompletableFuture<Void> unsubscribeAsync(boolean force)      { return delegate.unsubscribeAsync(force); }

    // ------------------------------------------------------------------ Consumer — receive

    @Override public Message<byte[]> receive() throws PulsarClientException                       { return delegate.receive(); }
    @Override public CompletableFuture<Message<byte[]>> receiveAsync()                            { return delegate.receiveAsync(); }
    @Override public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException { return delegate.receive(timeout, unit); }
    @Override public Messages<byte[]> batchReceive() throws PulsarClientException                 { return delegate.batchReceive(); }
    @Override public CompletableFuture<Messages<byte[]>> batchReceiveAsync()                      { return delegate.batchReceiveAsync(); }

    // ------------------------------------------------------------------ Consumer — acknowledge

    @Override public void acknowledge(MessageId messageId) throws PulsarClientException { delegate.acknowledge(messageId); }
    @Override public void acknowledge(Message<?> message) throws PulsarClientException  { delegate.acknowledge(message); }
    @Override public void acknowledge(List<MessageId> messageIdList) throws PulsarClientException { delegate.acknowledge(messageIdList); }
    @Override public void acknowledge(Messages<?> messages) throws PulsarClientException { delegate.acknowledge(messages); }
    @Override public void acknowledgeCumulative(MessageId messageId) throws PulsarClientException { delegate.acknowledgeCumulative(messageId); }
    @Override public void acknowledgeCumulative(Message<?> message) throws PulsarClientException { delegate.acknowledgeCumulative(message); }

    @Override public CompletableFuture<Void> acknowledgeAsync(MessageId messageId)            { return delegate.acknowledgeAsync(messageId); }
    @Override public CompletableFuture<Void> acknowledgeAsync(MessageId messageId, Transaction txn) { return delegate.acknowledgeAsync(messageId, txn); }
    @Override public CompletableFuture<Void> acknowledgeAsync(Message<?> message)             { return delegate.acknowledgeAsync(message); }
    @Override public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages)           { return delegate.acknowledgeAsync(messages); }
    @Override public CompletableFuture<Void> acknowledgeAsync(Messages<?> messages, Transaction txn) { return delegate.acknowledgeAsync(messages, txn); }
    @Override public CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList)  { return delegate.acknowledgeAsync(messageIdList); }
    @Override public CompletableFuture<Void> acknowledgeAsync(List<MessageId> messageIdList, Transaction txn) { return delegate.acknowledgeAsync(messageIdList, txn); }
    @Override public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId)  { return delegate.acknowledgeCumulativeAsync(messageId); }
    @Override public CompletableFuture<Void> acknowledgeCumulativeAsync(Message<?> message)   { return delegate.acknowledgeCumulativeAsync(message); }
    @Override public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Transaction txn) { return delegate.acknowledgeCumulativeAsync(messageId, txn); }

    @Override public void negativeAcknowledge(Message<?> message)   { delegate.negativeAcknowledge(message); }
    @Override public void negativeAcknowledge(MessageId messageId)  { delegate.negativeAcknowledge(messageId); }
    @Override public void negativeAcknowledge(Messages<?> messages) { delegate.negativeAcknowledge(messages); }

    // ------------------------------------------------------------------ Consumer — reconsume-later

    @Override public void reconsumeLater(Message<?> m, long d, TimeUnit u) throws PulsarClientException { delegate.reconsumeLater(m, d, u); }
    @Override public void reconsumeLater(Message<?> m, Map<String, String> props, long d, TimeUnit u) throws PulsarClientException { delegate.reconsumeLater(m, props, d, u); }
    @Override public void reconsumeLater(Messages<?> ms, long d, TimeUnit u) throws PulsarClientException { delegate.reconsumeLater(ms, d, u); }
    @Override public void reconsumeLaterCumulative(Message<?> m, long d, TimeUnit u) throws PulsarClientException { delegate.reconsumeLaterCumulative(m, d, u); }

    @Override public CompletableFuture<Void> reconsumeLaterAsync(Message<?> m, long d, TimeUnit u) { return delegate.reconsumeLaterAsync(m, d, u); }
    @Override public CompletableFuture<Void> reconsumeLaterAsync(Message<?> m, Map<String, String> props, long d, TimeUnit u) { return delegate.reconsumeLaterAsync(m, props, d, u); }
    @Override public CompletableFuture<Void> reconsumeLaterAsync(Messages<?> ms, long d, TimeUnit u) { return delegate.reconsumeLaterAsync(ms, d, u); }
    @Override public CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> m, long d, TimeUnit u) { return delegate.reconsumeLaterCumulativeAsync(m, d, u); }
    @Override public CompletableFuture<Void> reconsumeLaterCumulativeAsync(Message<?> m, Map<String, String> props, long d, TimeUnit u) { return delegate.reconsumeLaterCumulativeAsync(m, props, d, u); }

    @Override public void redeliverUnacknowledgedMessages() { delegate.redeliverUnacknowledgedMessages(); }

    // ------------------------------------------------------------------ Consumer — seek / message-id

    @Override public void seek(MessageId messageId) throws PulsarClientException { delegate.seek(messageId); }
    @Override public void seek(long timestamp) throws PulsarClientException      { delegate.seek(timestamp); }
    @Override public void seek(Function<String, Object> function) throws PulsarClientException { delegate.seek(function); }
    @Override public CompletableFuture<Void> seekAsync(MessageId messageId)      { return delegate.seekAsync(messageId); }
    @Override public CompletableFuture<Void> seekAsync(long timestamp)           { return delegate.seekAsync(timestamp); }
    @Override public CompletableFuture<Void> seekAsync(Function<String, Object> function) { return delegate.seekAsync(function); }

    @Override public MessageId getLastMessageId() throws PulsarClientException                { return delegate.getLastMessageId(); }
    @Override public CompletableFuture<MessageId> getLastMessageIdAsync()                     { return delegate.getLastMessageIdAsync(); }
    @Override public List<TopicMessageId> getLastMessageIds() throws PulsarClientException    { return delegate.getLastMessageIds(); }
    @Override public CompletableFuture<List<TopicMessageId>> getLastMessageIdsAsync()         { return delegate.getLastMessageIdsAsync(); }
}
