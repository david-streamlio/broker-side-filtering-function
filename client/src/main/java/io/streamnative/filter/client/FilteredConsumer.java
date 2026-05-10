package io.streamnative.filter.client;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Pulsar {@link Consumer} bound to a single private filtered topic, plus the metadata needed to
 * retract its backing query and clean up the topic on close.
 *
 * <p>The convenience accessors delegate to the underlying consumer. For unusual operations (seek,
 * pause, reconsume-later) reach through {@link #getUnderlyingConsumer()}.
 */
public final class FilteredConsumer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(FilteredConsumer.class);

    private final Session session;
    private final String queryId;
    private final String privateTopic;
    private final String composedSelector;
    private final Consumer<byte[]> underlying;
    private volatile boolean closed;

    FilteredConsumer(Session session,
                     String queryId,
                     String privateTopic,
                     String composedSelector,
                     Consumer<byte[]> underlying) {
        this.session         = session;
        this.queryId         = queryId;
        this.privateTopic    = privateTopic;
        this.composedSelector = composedSelector;
        this.underlying      = underlying;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getPrivateTopic() {
        return privateTopic;
    }

    public String getComposedSelector() {
        return composedSelector;
    }

    public Consumer<byte[]> getUnderlyingConsumer() {
        return underlying;
    }

    public Message<byte[]> receive() throws PulsarClientException {
        return underlying.receive();
    }

    public Message<byte[]> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        return underlying.receive(timeout, unit);
    }

    public CompletableFuture<Message<byte[]>> receiveAsync() {
        return underlying.receiveAsync();
    }

    public void acknowledge(Message<?> message) throws PulsarClientException {
        underlying.acknowledge(message);
    }

    public void acknowledge(MessageId messageId) throws PulsarClientException {
        underlying.acknowledge(messageId);
    }

    public CompletableFuture<Void> acknowledgeAsync(Message<?> message) {
        return underlying.acknowledgeAsync(message);
    }

    public void negativeAcknowledge(Message<?> message) {
        underlying.negativeAcknowledge(message);
    }

    public void negativeAcknowledge(MessageId messageId) {
        underlying.negativeAcknowledge(messageId);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            underlying.close();
        } catch (PulsarClientException e) {
            LOG.warn("Failed to close underlying consumer for queryId={}: {}", queryId, e.getMessage());
        }
        session.retractAndCleanup(queryId, privateTopic);
    }
}
