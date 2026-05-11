package io.streamnative.filter.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.QueryMessage;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Context object for the broker-side selector filter function. Owns the Pulsar client/admin
 * handles, the control-topic producer, and the {@link PrivateTopicPolicy} applied to per-consumer
 * private output topics.
 *
 * <p>Use {@link #builder()} to construct an instance, then {@link #newConsumer()} to build a
 * filtered consumer:
 *
 * <pre>{@code
 * try (Session session = Session.builder()
 *         .serviceUrl("pulsar://broker:6650")
 *         .adminUrl("http://broker:8080")
 *         .controlTopic("persistent://public/control/queries")
 *         .build()) {
 *
 *     Consumer<byte[]> c = session.newConsumer()
 *             .topic("persistent://public/quotes/trades")
 *             .selector("region = 'US'")
 *             .subscribe();
 *
 *     // Standard Pulsar Consumer API:
 *     Message<byte[]> msg = c.receive();
 *     c.acknowledge(msg);
 *
 *     // Dynamic selector ops via FilterControl cast:
 *     ((FilterControl) c).setSelector("region IN ('US','UK')");
 *
 *     c.close();
 * }
 * }</pre>
 *
 * <p>Sessions are thread-safe: multiple {@link #newConsumer()} chains may run concurrently.
 * {@link #close()} closes all resources owned by this session including any Pulsar handles it
 * constructed. Open consumers should be closed before the session.
 */
public final class Session implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PulsarClient pulsarClient;
    private final boolean ownsPulsarClient;
    private final PulsarAdmin pulsarAdmin;
    private final boolean ownsPulsarAdmin;
    private final String controlTopic;
    private final String privateTopicTenant;
    private final String privateTopicNamespace;
    private final PrivateTopicPolicy privateTopicPolicy;
    private final Producer<byte[]> controlProducer;

    Session(PulsarClient pulsarClient, boolean ownsPulsarClient,
            PulsarAdmin pulsarAdmin, boolean ownsPulsarAdmin,
            String controlTopic,
            String privateTopicTenant,
            String privateTopicNamespace,
            PrivateTopicPolicy privateTopicPolicy) throws PulsarClientException {
        this.pulsarClient        = pulsarClient;
        this.ownsPulsarClient    = ownsPulsarClient;
        this.pulsarAdmin         = pulsarAdmin;
        this.ownsPulsarAdmin     = ownsPulsarAdmin;
        this.controlTopic        = controlTopic;
        this.privateTopicTenant  = privateTopicTenant;
        this.privateTopicNamespace = privateTopicNamespace;
        this.privateTopicPolicy  = privateTopicPolicy;
        this.controlProducer     = pulsarClient.newProducer(Schema.BYTES)
                .topic(controlTopic)
                .producerName("selector-filter-session-" + UUID.randomUUID())
                .enableBatching(false)
                .create();

        if (pulsarAdmin == null && privateTopicPolicy.isApplyPerTopicPolicies()) {
            LOG.warn("No PulsarAdmin handle provided. Per-topic TTL/retention/inactive-cleanup "
                    + "policies cannot be applied to private topics; falling back to namespace defaults.");
        }
    }

    public static SessionBuilder builder() {
        return new SessionBuilder();
    }

    /**
     * Start building a filtered consumer. Configure topic(s), selector, and optional subscription
     * settings, then call {@link FilterableConsumerBuilder#subscribe()}.
     */
    public FilterableConsumerBuilder newConsumer() {
        return new FilterableConsumerBuilder(this);
    }

    public String getControlTopic()                   { return controlTopic; }
    public PrivateTopicPolicy getPrivateTopicPolicy() { return privateTopicPolicy; }

    /**
     * Used by tests; in production the function picks up control-topic updates in sub-second time
     * and most callers can skip this.
     */
    public boolean waitForReadiness(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        long sleep = 50L;
        while (System.nanoTime() < deadline) {
            Thread.sleep(sleep);
            sleep = Math.min(sleep * 2, 500L);
        }
        return true;
    }

    @Override
    public void close() {
        try { controlProducer.close(); } catch (Exception e) { LOG.warn("close producer", e); }
        if (ownsPulsarAdmin && pulsarAdmin != null) {
            try { pulsarAdmin.close(); } catch (Exception e) { LOG.warn("close admin", e); }
        }
        if (ownsPulsarClient) {
            try { pulsarClient.close(); } catch (Exception e) { LOG.warn("close client", e); }
        }
    }

    // ---------------------------------------------------------------- Builder/wrapper internals

    /**
     * Called from {@link FilterableConsumerBuilder#subscribe()}. Generates a queryId, creates the
     * private output topic, publishes the registration, and subscribes the underlying consumer.
     */
    Consumer<byte[]> subscribeFiltered(List<String> sourceTopics,
                                       String userSelector,
                                       String subscriptionName,
                                       SubscriptionType subscriptionType,
                                       SubscriptionInitialPosition initialPosition) throws PulsarClientException {
        String queryId      = UUID.randomUUID().toString();
        String privateTopic = String.format("persistent://%s/%s/filtered-%s",
                privateTopicTenant, privateTopicNamespace, queryId);
        String topicConstraint = Selectors.topicIn(sourceTopics);
        String composedSelector = Selectors.combine(topicConstraint, userSelector);
        String resolvedSubName = (subscriptionName == null || subscriptionName.isBlank())
                ? "session-" + UUID.randomUUID()
                : subscriptionName;

        try {
            createAndConfigurePrivateTopic(privateTopic);
            publishQuery(queryId, composedSelector, privateTopic);

            Consumer<byte[]> underlying = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(privateTopic)
                    .subscriptionName(resolvedSubName)
                    .subscriptionType(subscriptionType)
                    .subscriptionInitialPosition(initialPosition)
                    .subscribe();

            String storedUserSelector = (userSelector == null || userSelector.isBlank()) ? null : userSelector;
            LOG.info("Created filtered consumer queryId={} private={} selector=[{}]",
                    queryId, privateTopic, composedSelector);
            return new FilteringPulsarConsumer(this, underlying, queryId, privateTopic,
                    sourceTopics, topicConstraint, storedUserSelector);
        } catch (RuntimeException | PulsarClientException e) {
            safeRetract(queryId);
            safeDeletePrivateTopic(privateTopic);
            throw e instanceof PulsarClientException
                    ? (PulsarClientException) e
                    : PulsarClientException.unwrap(e);
        }
    }

    /**
     * Called from {@link FilteringPulsarConsumer#setSelector(String)}. Republishes the
     * {@link QueryMessage} on the same key, causing the function to hot-reload the compiled
     * selector for {@code queryId}.
     */
    void republishQuery(String queryId, String composedSelector, String outputTopic) throws PulsarClientException {
        publishQuery(queryId, composedSelector, outputTopic);
    }

    /**
     * Called from {@link FilteringPulsarConsumer#close()}. Tombstones the query and (per policy)
     * deletes the private topic.
     */
    void retractAndCleanup(String queryId, String privateTopic) {
        try {
            controlProducer.newMessage().key(queryId).value(null).send();
            LOG.info("Retracted query queryId={}", queryId);
        } catch (PulsarClientException e) {
            LOG.warn("Failed to retract queryId={}: {}", queryId, e.getMessage());
        }
        if (privateTopicPolicy.isDeleteOnClose()) {
            safeDeletePrivateTopic(privateTopic);
        }
    }

    private void createAndConfigurePrivateTopic(String privateTopic) {
        if (pulsarAdmin == null || !privateTopicPolicy.isApplyPerTopicPolicies()) {
            return;
        }
        try {
            pulsarAdmin.topics().createNonPartitionedTopic(privateTopic);
        } catch (PulsarAdminException.ConflictException ignored) {
            // Already exists — fine.
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to create private topic {}: {}. Falling back to auto-create.",
                    privateTopic, e.getMessage());
        }

        try {
            pulsarAdmin.topicPolicies().setMessageTTL(privateTopic, privateTopicPolicy.getMessageTtlSeconds());
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to set messageTTL on {}: {}", privateTopic, e.getMessage());
        }

        try {
            pulsarAdmin.topicPolicies().setRetention(privateTopic, new RetentionPolicies(
                    privateTopicPolicy.getRetentionTimeMinutes(),
                    privateTopicPolicy.getRetentionSizeMB()));
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to set retention on {}: {}", privateTopic, e.getMessage());
        }

        try {
            InactiveTopicPolicies inactive = new InactiveTopicPolicies(
                    InactiveTopicDeleteMode.delete_when_no_subscriptions,
                    privateTopicPolicy.getInactiveDeleteSeconds(),
                    true);
            pulsarAdmin.topicPolicies().setInactiveTopicPolicies(privateTopic, inactive);
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to set inactive-topic policy on {}: {}", privateTopic, e.getMessage());
        }
    }

    private void publishQuery(String queryId, String selector, String outputTopic) throws PulsarClientException {
        QueryMessage qm = new QueryMessage("session-" + queryId, selector, outputTopic);
        byte[] payload;
        try {
            payload = MAPPER.writeValueAsBytes(qm);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize QueryMessage", e);
        }
        controlProducer.newMessage().key(queryId).value(payload).send();
    }

    private void safeRetract(String queryId) {
        try {
            controlProducer.newMessage().key(queryId).value(null).send();
        } catch (Throwable t) {
            LOG.warn("Cleanup retract failed for queryId={}: {}", queryId, t.getMessage());
        }
    }

    private void safeDeletePrivateTopic(String privateTopic) {
        if (pulsarAdmin == null) return;
        try {
            pulsarAdmin.topics().delete(privateTopic, /*force=*/true);
            LOG.info("Deleted private topic {}", privateTopic);
        } catch (PulsarAdminException.NotFoundException ignored) {
            // Already gone.
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to delete private topic {}: {}", privateTopic, e.getMessage());
        }
    }
}
