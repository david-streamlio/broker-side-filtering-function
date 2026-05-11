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
 * <p>Two consumer-construction paths use this session:
 * <ul>
 *   <li>The Pulsar-native path: {@link #newConsumer()} returns a builder whose
 *       {@code subscribe()} hands back a standard {@code Consumer<byte[]>} that also implements
 *       {@link FilterControl}.</li>
 *   <li>The Kafka path (in the {@code selector-filter-client-kafka} module): uses the public
 *       registration API — {@link #registerPrivateQuery}, {@link #updatePrivateQuery}, and
 *       {@link #retirePrivateQuery} — to drive its own real {@code KafkaConsumer} pointed at a
 *       KoP-enabled broker.</li>
 * </ul>
 *
 * <p>Sessions are thread-safe: multiple consumer constructions may run concurrently.
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
     * Start building a Pulsar-native filtered consumer. Configure topic(s), selector, and
     * optional subscription settings, then call {@link FilterableConsumerBuilder#subscribe()}.
     */
    public FilterableConsumerBuilder newConsumer() {
        return new FilterableConsumerBuilder(this);
    }

    public String getControlTopic()                   { return controlTopic; }
    public PrivateTopicPolicy getPrivateTopicPolicy() { return privateTopicPolicy; }

    /** Pulsar tenant under which private output topics are created. */
    public String getPrivateTopicTenant()             { return privateTopicTenant; }

    /** Pulsar namespace under which private output topics are created. */
    public String getPrivateTopicNamespace()          { return privateTopicNamespace; }

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

    // =============================================================================
    // Public registration API — used by Pulsar SDK internally AND by the Kafka SDK
    // =============================================================================

    /**
     * Register a filtered query with the broker-side function. Generates a fresh {@code queryId},
     * creates and configures the private output topic, validates the user selector, and publishes
     * the {@link QueryMessage} to the control topic.
     *
     * <p>Callers (per-transport consumer builders) take the returned {@link PrivateQuery} and
     * attach their own consumer to {@link PrivateQuery#privateTopic()}. On failure of that next
     * step they should call {@link #retirePrivateQuery(PrivateQuery)} to clean up.
     *
     * @throws IllegalArgumentException if {@code sourceTopics} is empty or {@code userSelector}
     *                                  is non-blank but malformed.
     * @throws PulsarClientException    on control-topic publish failure.
     */
    public PrivateQuery registerPrivateQuery(List<String> sourceTopics, String userSelector)
            throws PulsarClientException {
        if (sourceTopics == null || sourceTopics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }
        Selectors.validate(userSelector);

        String queryId         = UUID.randomUUID().toString();
        String privateTopic    = String.format("persistent://%s/%s/filtered-%s",
                privateTopicTenant, privateTopicNamespace, queryId);
        String topicConstraint = Selectors.topicIn(sourceTopics);
        String composedSelector = Selectors.combine(topicConstraint, userSelector);

        createAndConfigurePrivateTopic(privateTopic);
        publishQuery(queryId, composedSelector, privateTopic);

        LOG.info("Registered query queryId={} private={} selector=[{}]",
                queryId, privateTopic, composedSelector);
        return new PrivateQuery(queryId, privateTopic, sourceTopics, topicConstraint);
    }

    /**
     * Replace the active user-side selector on an existing query. Validates the new expression,
     * composes it with the original topic constraint, and republishes the {@link QueryMessage} on
     * the same {@code queryId} key — the function hot-reloads its compiled expression in place.
     *
     * @throws IllegalArgumentException if {@code newUserSelector} is non-blank but malformed.
     * @throws PulsarClientException    on control-topic publish failure.
     */
    public void updatePrivateQuery(PrivateQuery pq, String newUserSelector) throws PulsarClientException {
        Selectors.validate(newUserSelector);
        String composed = Selectors.combine(pq.topicConstraint(), newUserSelector);
        publishQuery(pq.queryId(), composed, pq.privateTopic());
        LOG.info("Updated query queryId={} selector=[{}]", pq.queryId(), composed);
    }

    /**
     * Retract a previously-registered query. Publishes a tombstone to the control topic and (per
     * the configured {@link PrivateTopicPolicy}) deletes the private output topic. Best-effort —
     * does not throw; log-only on errors.
     */
    public void retirePrivateQuery(PrivateQuery pq) {
        try {
            controlProducer.newMessage().key(pq.queryId()).value(null).send();
            LOG.info("Retracted query queryId={}", pq.queryId());
        } catch (PulsarClientException e) {
            LOG.warn("Failed to retract queryId={}: {}", pq.queryId(), e.getMessage());
        }
        if (privateTopicPolicy.isDeleteOnClose()) {
            safeDeletePrivateTopic(pq.privateTopic());
        }
    }

    // =============================================================================
    // Pulsar-SDK internal — uses the public registration API + native Pulsar subscribe
    // =============================================================================

    /**
     * Called from {@link FilterableConsumerBuilder#subscribe()}. Registers the query and
     * attaches a native Pulsar consumer to the private topic. Cleans up the registration on
     * Pulsar-subscribe failure.
     */
    Consumer<byte[]> subscribeFiltered(List<String> sourceTopics,
                                       String userSelector,
                                       String subscriptionName,
                                       SubscriptionType subscriptionType,
                                       SubscriptionInitialPosition initialPosition) throws PulsarClientException {
        PrivateQuery pq = registerPrivateQuery(sourceTopics, userSelector);
        String resolvedSubName = (subscriptionName == null || subscriptionName.isBlank())
                ? "session-" + UUID.randomUUID()
                : subscriptionName;
        try {
            Consumer<byte[]> underlying = pulsarClient.newConsumer(Schema.BYTES)
                    .topic(pq.privateTopic())
                    .subscriptionName(resolvedSubName)
                    .subscriptionType(subscriptionType)
                    .subscriptionInitialPosition(initialPosition)
                    .subscribe();
            String storedUserSelector = (userSelector == null || userSelector.isBlank()) ? null : userSelector;
            return new FilteringPulsarConsumer(this, pq, underlying, storedUserSelector);
        } catch (RuntimeException | PulsarClientException e) {
            retirePrivateQuery(pq);
            throw e instanceof PulsarClientException
                    ? (PulsarClientException) e
                    : PulsarClientException.unwrap(e);
        }
    }

    // =============================================================================
    // Internal helpers
    // =============================================================================

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
