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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * JMS-Session-style coordinator for the broker-side selector filter function.
 *
 * <p>Use {@link #builder()} to construct an instance. {@link #createConsumer(String, String)} (or
 * the multi-source overload) does the following:
 * <ol>
 *   <li>Generates a unique query id and derives a private output topic name.</li>
 *   <li>Optionally creates the private topic and applies the configured
 *       {@link PrivateTopicPolicy} (TTL, retention, inactive-cleanup).</li>
 *   <li>Publishes a {@link QueryMessage} to the control topic, keyed by the query id.</li>
 *   <li>Subscribes a Pulsar {@link Consumer} on the private topic and returns it wrapped in a
 *       {@link FilteredConsumer}. Closing that consumer retracts the query and (per policy)
 *       deletes the private topic.</li>
 * </ol>
 *
 * <p>The session is thread-safe in the sense that multiple {@code createConsumer} calls can run
 * concurrently; each produces an independent consumer with its own state. {@link #close()} closes
 * all resources owned by this session, including any Pulsar client/admin handles it constructed.
 * Open consumers should be closed before the session.
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
     * Create a filtered consumer attached to a single source topic.
     *
     * @param sourceTopic the input topic the filter function is already consuming
     * @param selector    a JMS selector expression, or {@code null} for no user-side filtering
     */
    public FilteredConsumer createConsumer(String sourceTopic, String selector) throws PulsarClientException {
        return createConsumer(List.of(sourceTopic), selector, "session-" + UUID.randomUUID());
    }

    /**
     * Create a filtered consumer attached to multiple source topics. The session composes the
     * effective selector as {@code __topic__ IN (...) AND (<user selector>)}.
     */
    public FilteredConsumer createConsumer(Collection<String> sourceTopics, String selector) throws PulsarClientException {
        return createConsumer(sourceTopics, selector, "session-" + UUID.randomUUID());
    }

    /**
     * Full-control overload. The {@code subscriptionName} is the Pulsar subscription on the
     * private topic — exclusive type, earliest-position by default.
     */
    public FilteredConsumer createConsumer(Collection<String> sourceTopics,
                                           String selector,
                                           String subscriptionName) throws PulsarClientException {
        if (sourceTopics == null || sourceTopics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }

        String queryId      = UUID.randomUUID().toString();
        String privateTopic = String.format("persistent://%s/%s/filtered-%s",
                privateTopicTenant, privateTopicNamespace, queryId);
        String composedSelector = Selectors.combine(Selectors.topicIn(sourceTopics), selector);

        try {
            // Step 1: create + configure the private topic.
            createAndConfigurePrivateTopic(privateTopic);

            // Step 2: publish the query to the control topic.
            publishQuery(queryId, composedSelector, privateTopic);

            // Step 3: subscribe a consumer to the private topic.
            Consumer<byte[]> consumer = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(privateTopic)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(SubscriptionType.Exclusive)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();

            LOG.info("Created filtered consumer queryId={} private={} selector=[{}]",
                    queryId, privateTopic, composedSelector);
            return new FilteredConsumer(this, queryId, privateTopic, composedSelector, consumer);
        } catch (RuntimeException | PulsarClientException e) {
            // Partial-failure cleanup: retract the query and delete the topic if we created it.
            safeRetract(queryId);
            safeDeletePrivateTopic(privateTopic);
            throw e instanceof PulsarClientException
                    ? (PulsarClientException) e
                    : PulsarClientException.unwrap(e);
        }
    }

    private void createAndConfigurePrivateTopic(String privateTopic) {
        if (pulsarAdmin == null || !privateTopicPolicy.isApplyPerTopicPolicies()) {
            // Rely on auto-create + namespace defaults.
            return;
        }
        try {
            pulsarAdmin.topics().createNonPartitionedTopic(privateTopic);
        } catch (PulsarAdminException.ConflictException ignored) {
            // Topic already exists — fine.
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

    /**
     * Internal: invoked by {@link FilteredConsumer#close()}. Tombstones the query and (per policy)
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

    public String getControlTopic()           { return controlTopic; }
    public PrivateTopicPolicy getPrivateTopicPolicy() { return privateTopicPolicy; }

    /**
     * Wait up to {@code timeout} for the broker-side filter function to observe a freshly-published
     * query. Useful in tests; in production the latency is typically sub-second and callers can
     * skip this.
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
}
