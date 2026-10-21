/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.streamnative.filter.client;

import io.streamnative.filter.api.FilterControl;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.api.internal.WindowProgress;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.ValueSchemaInfo;
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
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Context object for the broker-side selector filter SDK. Owns the Pulsar client/admin handles,
 * the orchestrator's input topic producer (the topic the SDK publishes {@link FilterRequest}s to),
 * and the {@link PrivateTopicPolicy} applied to per-consumer private output topics.
 *
 * <p>Two consumer-construction paths use this session:
 * <ul>
 *   <li>The Pulsar-native path: {@link #newConsumer()} / {@link #newConsumer(Schema)} return a
 *       builder whose {@code subscribe()} hands back a standard {@code Consumer<V>}
 *       ({@code byte[]} for the no-schema overload) that also implements {@link FilterControl}.</li>
 *   <li>The Kafka path (in the {@code selector-filter-client-kafka} module): uses the public
 *       registration API — {@link #registerPrivateQuery}, {@link #updatePrivateQuery}, and
 *       {@link #retirePrivateQuery} — to drive its own real {@code KafkaConsumer} pointed at a
 *       KoP-enabled broker.</li>
 * </ul>
 *
 * <p>Wire protocol: SDK publishes {@link FilterRequest} (REGISTER/MODIFY/UNREGISTER) keyed by
 * queryId to the orchestrator's input topic ({@link SessionBuilder#controlTopic configured
 * controlTopic}). Orchestrator translates and routes to per-filter-function control topics.
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
    /** Policy for windowed queries' private topics; see {@link PrivateTopicPolicy#WINDOWED_DEFAULT}. */
    private final PrivateTopicPolicy windowedPrivateTopicPolicy;
    private final Producer<byte[]> controlProducer;
    /** Stable identifier used as the HEARTBEAT message key. Arbitrary; orchestrator does not key on it. */
    private final String sessionId = UUID.randomUUID().toString();
    /**
     * Currently-live queryIds owned by this Session. Updated on register/retire; snapshotted by
     * the heartbeat task to build each HEARTBEAT message body.
     */
    private final Set<String> liveQueryIds = ConcurrentHashMap.newKeySet();
    /** The live queryIds that are windowed, so retirement applies the windowed topic policy. */
    private final Set<String> windowedQueryIds = ConcurrentHashMap.newKeySet();
    /** {@code null} when heartbeats are disabled. */
    private final ScheduledExecutorService heartbeatScheduler;
    private final long heartbeatIntervalSeconds;

    Session(PulsarClient pulsarClient, boolean ownsPulsarClient,
            PulsarAdmin pulsarAdmin, boolean ownsPulsarAdmin,
            String controlTopic,
            String privateTopicTenant,
            String privateTopicNamespace,
            PrivateTopicPolicy privateTopicPolicy,
            long heartbeatIntervalSeconds) throws PulsarClientException {
        this(pulsarClient, ownsPulsarClient, pulsarAdmin, ownsPulsarAdmin, controlTopic, privateTopicTenant,
                privateTopicNamespace, privateTopicPolicy, PrivateTopicPolicy.WINDOWED_DEFAULT,
                heartbeatIntervalSeconds);
    }

    Session(PulsarClient pulsarClient, boolean ownsPulsarClient,
            PulsarAdmin pulsarAdmin, boolean ownsPulsarAdmin,
            String controlTopic,
            String privateTopicTenant,
            String privateTopicNamespace,
            PrivateTopicPolicy privateTopicPolicy,
            PrivateTopicPolicy windowedPrivateTopicPolicy,
            long heartbeatIntervalSeconds) throws PulsarClientException {
        this.windowedPrivateTopicPolicy = windowedPrivateTopicPolicy;
        this.pulsarClient        = pulsarClient;
        this.ownsPulsarClient    = ownsPulsarClient;
        this.pulsarAdmin         = pulsarAdmin;
        this.ownsPulsarAdmin     = ownsPulsarAdmin;
        this.controlTopic        = controlTopic;
        this.privateTopicTenant  = privateTopicTenant;
        this.privateTopicNamespace = privateTopicNamespace;
        this.privateTopicPolicy  = privateTopicPolicy;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        this.controlProducer     = pulsarClient.newProducer(Schema.BYTES)
                .topic(controlTopic)
                .producerName("selector-filter-session-" + sessionId)
                .enableBatching(false)
                .create();

        if (pulsarAdmin == null && privateTopicPolicy.isApplyPerTopicPolicies()) {
            LOG.warn("No PulsarAdmin handle provided. Per-topic TTL/retention/inactive-cleanup "
                    + "policies cannot be applied to private topics; falling back to namespace defaults.");
        }

        if (heartbeatIntervalSeconds > 0L) {
            this.heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "selector-filter-heartbeat-" + sessionId);
                t.setDaemon(true);
                return t;
            });
            this.heartbeatScheduler.scheduleWithFixedDelay(
                    this::sendHeartbeatQuietly,
                    heartbeatIntervalSeconds, heartbeatIntervalSeconds, TimeUnit.SECONDS);
            LOG.info("Session heartbeat scheduled: interval={}s sessionId={}",
                    heartbeatIntervalSeconds, sessionId);
        } else {
            this.heartbeatScheduler = null;
            LOG.info("Session heartbeats disabled (sessionId={})", sessionId);
        }
    }

    public static SessionBuilder builder() {
        return new SessionBuilder();
    }

    /**
     * Start building a Pulsar-native filtered consumer over raw {@code byte[]} values. Configure
     * topic(s), selector, and optional subscription settings, then call
     * {@link FilterableConsumerBuilder#subscribe()}. Wire-identical to the pre-schema behavior: no
     * value schema is forwarded to the orchestrator.
     */
    public FilterableConsumerBuilder<byte[]> newConsumer() {
        return new FilterableConsumerBuilder<>(this, Schema.BYTES, null);
    }

    /**
     * Start building a Pulsar-native filtered consumer whose value is decoded with the supplied
     * {@code valueSchema}. {@link FilterableConsumerBuilder#subscribe()} returns a
     * {@code Consumer<V>}; the message key remains a {@code String} (Pulsar has no key schema — use
     * the Kafka SDK for a typed key). The schema's full {@code SchemaInfo} is forwarded to the
     * orchestrator and on to the filter function so it can (in a follow-up) decode the value for
     * schema-aware filtering.
     *
     * <p>Supported value types: {@code BYTES}, {@code STRING}, {@code JSON(Pojo)},
     * {@code AVRO(Pojo)}, {@code PROTOBUF_NATIVE(Pojo)}, and primitives. {@code Schema.PROTOBUF}
     * (non-native) is rejected because it needs the generated class to decode and carries no
     * descriptor for the function — use {@code Schema.PROTOBUF_NATIVE} instead. {@code AUTO_CONSUME}
     * is unsupported (it has no concrete reader schema).
     *
     * @throws IllegalArgumentException if {@code valueSchema} is {@code Schema.PROTOBUF} (non-native)
     */
    public <V> FilterableConsumerBuilder<V> newConsumer(Schema<V> valueSchema) {
        Objects.requireNonNull(valueSchema, "valueSchema");
        SchemaInfo si = valueSchema.getSchemaInfo();
        if (si != null && si.getType() == SchemaType.PROTOBUF) {
            throw new IllegalArgumentException(
                    "Schema.PROTOBUF is not supported for filtered consumers because the filter "
                    + "function needs the descriptor to decode the value; use Schema.PROTOBUF_NATIVE");
        }
        return new FilterableConsumerBuilder<>(this, valueSchema, toValueSchemaInfo(si));
    }

    /**
     * Map a Pulsar {@link SchemaInfo} into the Pulsar-free {@link ValueSchemaInfo} carried on the
     * wire. Returns {@code null} when there is no schema info (treated as the no-schema path).
     */
    static ValueSchemaInfo toValueSchemaInfo(SchemaInfo si) {
        if (si == null) {
            return null;
        }
        byte[] definition = si.getSchema();
        String encoded = (definition == null || definition.length == 0)
                ? null
                : Base64.getEncoder().encodeToString(definition);
        Map<String, String> props = si.getProperties();
        return new ValueSchemaInfo(
                si.getName(),
                si.getType() == null ? null : si.getType().name(),
                encoded,
                (props == null || props.isEmpty()) ? null : props);
    }

    public String getControlTopic()                   { return controlTopic; }
    public PrivateTopicPolicy getPrivateTopicPolicy() { return privateTopicPolicy; }

    /** Pulsar tenant under which private output topics are created. */
    public String getPrivateTopicTenant()             { return privateTopicTenant; }

    /** Pulsar namespace under which private output topics are created. */
    public String getPrivateTopicNamespace()          { return privateTopicNamespace; }

    /**
     * Used by tests; in production the orchestrator picks up new requests in sub-second time and
     * most callers can skip this.
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
        // Stop the heartbeat task FIRST so it cannot publish after the producer is gone.
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
            try {
                if (!heartbeatScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    heartbeatScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                heartbeatScheduler.shutdownNow();
            }
        }
        try { controlProducer.close(); } catch (Exception e) { LOG.warn("close producer", e); }
        if (ownsPulsarAdmin && pulsarAdmin != null) {
            try { pulsarAdmin.close(); } catch (Exception e) { LOG.warn("close admin", e); }
        }
        if (ownsPulsarClient) {
            try { pulsarClient.close(); } catch (Exception e) { LOG.warn("close client", e); }
        }
    }

    /**
     * Scheduled task body. Snapshots {@link #liveQueryIds} and publishes one HEARTBEAT message if
     * the snapshot is non-empty. Swallows all exceptions — a transient publish failure must not
     * kill the scheduler.
     */
    void sendHeartbeatQuietly() {
        try {
            List<String> snapshot = List.copyOf(liveQueryIds);
            if (snapshot.isEmpty()) return;
            FilterRequest hb = FilterRequest.heartbeat(snapshot);
            publishFilterRequest("heartbeat-" + sessionId, hb);
            LOG.debug("Published HEARTBEAT covering {} queryId(s) sessionId={}",
                    snapshot.size(), sessionId);
        } catch (Throwable t) {
            // DEBUG, not WARN — transient broker unavailability would otherwise spam the log
            // every heartbeatIntervalSeconds. If the broker is unreachable for an extended
            // window, the orchestrator will eventually synthesize UNREGISTERs anyway.
            LOG.debug("Heartbeat publish failed (will retry next tick): {}", t.getMessage());
        }
    }

    // =============================================================================
    // Public registration API — used by Pulsar SDK internally AND by the Kafka SDK
    // =============================================================================

    /**
     * Register a filtered query — Pulsar-flavour convenience overload (no serializer info,
     * {@link ClientType#PULSAR}). See the full overload for details.
     */
    public PrivateQuery registerPrivateQuery(List<String> sourceTopics, String userSelector)
            throws PulsarClientException {
        return registerPrivateQuery(sourceTopics, userSelector, null, null, ClientType.PULSAR);
    }

    /**
     * Register a filtered query with the orchestrator. Generates a fresh {@code queryId}, derives
     * a private output topic name, validates the user selector, optionally creates and configures
     * the private output topic, and publishes a {@link FilterRequest#getCommand() REGISTER}
     * {@link FilterRequest} to the orchestrator's input topic.
     *
     * <p>Callers (per-transport consumer builders) take the returned {@link PrivateQuery} and
     * attach their own consumer to {@link PrivateQuery#privateTopic()}. On failure of that next
     * step they should call {@link #retirePrivateQuery(PrivateQuery)} to clean up.
     *
     * @param sourceTopics      input topics to filter (at least one); each must be a fully-qualified Pulsar topic
     * @param userSelector      JMS-selector expression; validated client-side
     * @param keyDeserializer   FQN of consumer-side key deserializer, or {@code null} if not asserted
     * @param valueDeserializer FQN of consumer-side value deserializer, or {@code null} if not asserted
     * @param clientType        SDK family producing this request
     * @throws IllegalArgumentException if {@code sourceTopics} is empty or {@code userSelector}
     *                                  is non-blank but malformed.
     * @throws PulsarClientException    on control-topic publish failure.
     */
    public PrivateQuery registerPrivateQuery(List<String> sourceTopics, String userSelector,
                                             String keyDeserializer, String valueDeserializer,
                                             ClientType clientType) throws PulsarClientException {
        return registerPrivateQuery(sourceTopics, userSelector, keyDeserializer, valueDeserializer,
                null, clientType);
    }

    /**
     * Full registration overload that also forwards the consumer's value {@link ValueSchemaInfo}
     * (used by the Pulsar SDK's {@link #subscribeFiltered} so the filter function receives the value
     * schema). See {@link #registerPrivateQuery(List, String, String, String, ClientType)} for the
     * common contract; {@code valueSchema} may be {@code null}.
     */
    public PrivateQuery registerPrivateQuery(List<String> sourceTopics, String userSelector,
                                             String keyDeserializer, String valueDeserializer,
                                             ValueSchemaInfo valueSchema,
                                             ClientType clientType) throws PulsarClientException {
        if (sourceTopics == null || sourceTopics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }
        Selectors.validate(userSelector);

        String queryId      = UUID.randomUUID().toString();
        String privateTopic = String.format("persistent://%s/%s/filtered-%s",
                privateTopicTenant, privateTopicNamespace, queryId);

        createAndConfigurePrivateTopic(privateTopic, privateTopicPolicy);

        FilterRequest req = FilterRequest.register(sourceTopics, privateTopic, userSelector,
                keyDeserializer, valueDeserializer, valueSchema, clientType);
        publishFilterRequest(queryId, req);

        // Track this queryId in our heartbeat set so subsequent heartbeats keep the orchestrator
        // informed that it is still live.
        liveQueryIds.add(queryId);

        LOG.info("Registered query queryId={} private={} selector=[{}] clientType={}",
                queryId, privateTopic, userSelector, clientType);
        return new PrivateQuery(queryId, privateTopic, sourceTopics);
    }

    /**
     * Replace the active user-side selector on an existing query. Validates the new expression
     * and publishes a {@link FilterRequest#getCommand() MODIFY} on the same {@code queryId} key —
     * the orchestrator then republishes the underlying {@code QueryMessage} on each per-function
     * control topic where this queryId is registered, causing a hot reload.
     *
     * @throws IllegalArgumentException if {@code newUserSelector} is malformed or blank.
     * @throws PulsarClientException    on control-topic publish failure.
     */
    public void updatePrivateQuery(PrivateQuery pq, String newUserSelector) throws PulsarClientException {
        Selectors.validate(newUserSelector);
        FilterRequest req = FilterRequest.modify(newUserSelector);
        req.validate();  // MODIFY requires non-blank selector — fail at SDK, not at orchestrator
        publishFilterRequest(pq.queryId(), req);
        LOG.info("Updated query queryId={} selector=[{}]", pq.queryId(), newUserSelector);
    }

    /**
     * Retract a previously-registered query. Publishes an UNREGISTER {@link FilterRequest} keyed
     * by queryId (orchestrator fans out tombstones to every per-function control topic) and (per
     * the configured {@link PrivateTopicPolicy}) deletes the private output topic. Best-effort —
     * does not throw; log-only on errors.
     */
    public void retirePrivateQuery(PrivateQuery pq) {
        // Stop heartbeating this queryId BEFORE publishing UNREGISTER, so a heartbeat that races
        // with the UNREGISTER cannot re-touch a queryId we are about to retract.
        liveQueryIds.remove(pq.queryId());
        PrivateTopicPolicy policy = windowedQueryIds.remove(pq.queryId())
                ? windowedPrivateTopicPolicy : privateTopicPolicy;
        try {
            publishFilterRequest(pq.queryId(), FilterRequest.unregister());
            LOG.info("Retracted query queryId={}", pq.queryId());
        } catch (PulsarClientException e) {
            LOG.warn("Failed to retract queryId={}: {}", pq.queryId(), e.getMessage());
        }
        if (policy.isDeleteOnClose()) {
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
    <V> Consumer<V> subscribeFiltered(List<String> sourceTopics,
                                      String userSelector,
                                      String subscriptionName,
                                      SubscriptionType subscriptionType,
                                      SubscriptionInitialPosition initialPosition,
                                      Schema<V> valueSchema,
                                      ValueSchemaInfo valueSchemaInfo) throws PulsarClientException {
        return subscribeFiltered(sourceTopics, userSelector, subscriptionName, subscriptionType, initialPosition,
                valueSchema, valueSchemaInfo, null);
    }

    /**
     * As above; with {@code liveOnlyProgress} non-null ({@code TimeWindow.liveOnly()}), the consumer
     * also implements {@link WindowControl}, caught up from the start. It runs on the same shared live
     * filter as any other live query.
     */
    <V> Consumer<V> subscribeFiltered(List<String> sourceTopics,
                                      String userSelector,
                                      String subscriptionName,
                                      SubscriptionType subscriptionType,
                                      SubscriptionInitialPosition initialPosition,
                                      Schema<V> valueSchema,
                                      ValueSchemaInfo valueSchemaInfo,
                                      WindowProgress liveOnlyProgress) throws PulsarClientException {
        // Forward the value schema name as the valueDeserializer (parity with the Kafka SDK's
        // per-input-topic mismatch detection) and the full SchemaInfo for the function. No key
        // schema — Pulsar keys are always Strings.
        String valueDeserializer = valueSchemaInfo == null ? null : valueSchemaInfo.getName();
        PrivateQuery pq = registerPrivateQuery(sourceTopics, userSelector,
                null, valueDeserializer, valueSchemaInfo, ClientType.PULSAR);
        String resolvedSubName = (subscriptionName == null || subscriptionName.isBlank())
                ? "session-" + UUID.randomUUID()
                : subscriptionName;
        try {
            Consumer<V> underlying = pulsarClient.newConsumer(valueSchema)
                    .topic(pq.privateTopic())
                    .subscriptionName(resolvedSubName)
                    .subscriptionType(subscriptionType)
                    .subscriptionInitialPosition(initialPosition)
                    .subscribe();
            String storedUserSelector = (userSelector == null || userSelector.isBlank()) ? null : userSelector;
            return liveOnlyProgress == null
                    ? new FilteringPulsarConsumer<>(this, pq, underlying, storedUserSelector)
                    : new WindowedPulsarConsumer<>(this, pq, underlying, storedUserSelector, liveOnlyProgress);
        } catch (RuntimeException | PulsarClientException e) {
            retirePrivateQuery(pq);
            throw e instanceof PulsarClientException
                    ? (PulsarClientException) e
                    : PulsarClientException.unwrap(e);
        }
    }

    /**
     * Register a windowed query: the server reads {@code sourceTopic}'s history from {@code fromMs}
     * (epoch ms on the log's clock: broker-entry time when the broker appends it; {@code 0} means from
     * the beginning) into a fresh private topic configured with the windowed topic policy.
     * Bounded ({@code toMs} set, possibly in the future): the matches end with a completion record.
     * Open-ended ({@code toMs} {@code null}): the historical matches end with a caught-up record and
     * live matches follow. See {@link TimeWindow#resolve} for how windows map to these bounds.
     *
     * @throws IllegalArgumentException if the selector is malformed or the window is empty
     * @throws PulsarClientException    on control-topic publish failure
     */
    public PrivateQuery registerWindowedQuery(String sourceTopic, String userSelector,
                                              String keyDeserializer, String valueDeserializer,
                                              ValueSchemaInfo valueSchema, ClientType clientType,
                                              long fromMs, Long toMs) throws PulsarClientException {
        Objects.requireNonNull(sourceTopic, "sourceTopic");
        if (fromMs < 0 || (toMs != null && toMs <= fromMs)) {
            throw new IllegalArgumentException("window requires 0 <= from < to, got [" + fromMs + ", " + toMs + "]");
        }
        Selectors.validate(userSelector);

        String queryId      = UUID.randomUUID().toString();
        String privateTopic = String.format("persistent://%s/%s/filtered-%s",
                privateTopicTenant, privateTopicNamespace, queryId);
        createAndConfigurePrivateTopic(privateTopic, windowedPrivateTopicPolicy);

        FilterRequest req = FilterRequest.registerWindowed(sourceTopic, privateTopic, userSelector,
                keyDeserializer, valueDeserializer, valueSchema, clientType, fromMs, toMs);
        publishFilterRequest(queryId, req);
        liveQueryIds.add(queryId);
        windowedQueryIds.add(queryId);

        LOG.info("Registered windowed query queryId={} private={} source={} window=[{}, {}] selector=[{}] clientType={}",
                queryId, privateTopic, sourceTopic, fromMs, toMs == null ? "open" : toMs, userSelector, clientType);
        return new PrivateQuery(queryId, privateTopic, List.of(sourceTopic));
    }

    /**
     * Windowed counterpart of {@link #subscribeFiltered}: registers the window, then subscribes from
     * the start of the private topic (the windowed topic policy retains results until they're read).
     */
    <V> Consumer<V> subscribeWindowed(String sourceTopic,
                                      String userSelector,
                                      String subscriptionName,
                                      SubscriptionType subscriptionType,
                                      Schema<V> valueSchema,
                                      ValueSchemaInfo valueSchemaInfo,
                                      long fromMs, Long toMs) throws PulsarClientException {
        String valueDeserializer = valueSchemaInfo == null ? null : valueSchemaInfo.getName();
        PrivateQuery pq = registerWindowedQuery(sourceTopic, userSelector, null, valueDeserializer,
                valueSchemaInfo, ClientType.PULSAR, fromMs, toMs);
        String resolvedSubName = (subscriptionName == null || subscriptionName.isBlank())
                ? "session-" + UUID.randomUUID()
                : subscriptionName;
        try {
            Consumer<V> underlying = pulsarClient.newConsumer(valueSchema)
                    .topic(pq.privateTopic())
                    .subscriptionName(resolvedSubName)
                    .subscriptionType(subscriptionType)
                    .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                    .subscribe();
            String storedUserSelector = (userSelector == null || userSelector.isBlank()) ? null : userSelector;
            return new WindowedPulsarConsumer<>(this, pq, underlying, storedUserSelector,
                    WindowProgress.forWindow(toMs == null));
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

    private void createAndConfigurePrivateTopic(String privateTopic, PrivateTopicPolicy privateTopicPolicy) {
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

    private void publishFilterRequest(String queryId, FilterRequest request) throws PulsarClientException {
        byte[] payload;
        try {
            payload = MAPPER.writeValueAsBytes(request);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize FilterRequest", e);
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
