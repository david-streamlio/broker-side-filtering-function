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

package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.LogPosition;
import io.streamnative.filter.common.WindowSpec;
import java.time.Clock;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link WindowPreparer} built on the function runtime's {@link PulsarClient}.
 *
 * <p>Why the client and not the admin API: the orchestrator NAR bundles its own admin client, but the
 * Functions runtime puts Jackson on every function's parent classpath. Admin calls that serialize a
 * NAR-only bean ({@code createSubscription} sends a {@code ResetCursorData}) then mix the two
 * classloaders and fail with OpenTelemetry {@code NoClassDefFoundError} / {@code LinkageError}. The
 * runtime client is what the orchestrator already uses for its producers and table views. The admin
 * client is kept for three calls whose responses are simple shapes: partitioned-topic metadata, the
 * namespace's topic list and the topic's retention policy.
 *
 * <p>{@code Consumer.seek(timestamp)} resets the subscription on the broker exactly as
 * {@code resetCursor(timestamp)} does: by the log's clock (broker-entry timestamp when the broker
 * appends one, else publish time; spike S1). The end position is captured after the seek, so it is
 * never behind it. Message IDs are read via {@link LogPosition} from {@code toByteArray()}, so nothing
 * depends on which classloader created them.
 */
final class ClientWindowPreparer implements WindowPreparer {

    private static final Logger LOG = LoggerFactory.getLogger(ClientWindowPreparer.class);
    private static final int READ_TIMEOUT_SECONDS = 10;

    private final PulsarClient client;
    private final PulsarAdmin admin;
    private final Clock clock;

    ClientWindowPreparer(PulsarClient client, PulsarAdmin admin) {
        this(client, admin, Clock.systemUTC());
    }

    ClientWindowPreparer(PulsarClient client, PulsarAdmin admin, Clock clock) {
        this.client = client;
        this.admin = admin;
        this.clock = clock;
    }

    @Override
    public WindowPlan prepare(String sourceTopic, String subscription, long fromMs, Long toMs) throws Exception {
        String log = singleLog(sourceTopic);
        // Paused: positioning the subscription must not pull messages off it.
        Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                .topic(sourceTopic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .startPaused(true)
                .subscribe();
        try {
            consumer.seek(fromMs);
            List<TopicMessageId> lastIds = consumer.getLastMessageIds();
            TopicMessageId lastId = lastIds.isEmpty() ? null : lastIds.get(0);
            LogPosition end = lastId == null ? null : LogPosition.fromBytes(lastId.toByteArray());
            boolean hasMessages = end != null && end.isMessage();

            Long earliestTime = hasMessages ? logTimeAt(log, MessageId.earliest, false) : null;
            Long lastTime = hasMessages ? logTimeAt(log, lastId, true) : null;
            boolean truncated = truncatedByRetention(fromMs, earliestTime, retentionMinutes(sourceTopic),
                    clock.millis());
            // The log's clock only moves forward (broker-entry time), so if the last message is
            // before the window start, nothing in the window exists.
            boolean empty = !hasMessages || lastTime == null || lastTime < fromMs;

            WindowSpec spec = new WindowSpec(fromMs, toMs,
                    hasMessages ? end.ledgerId() : null, hasMessages ? end.entryId() : null, truncated);
            if (empty && toMs != null) {
                consumer.unsubscribe();   // a bounded empty window: no filter will use it
            } else {
                consumer.close();         // the subscription stays, positioned at fromMs
            }
            LOG.info("Prepared window {} on {} (subscription {}): empty={}", spec, log, subscription, empty);
            return new WindowPlan(spec, empty);
        } catch (Exception e) {
            consumer.closeAsync();
            throw e;
        }
    }

    /**
     * Whether the server can't vouch that the whole window was retained, so its result may be missing
     * messages that retention deleted. True for a window that starts in the past ({@code 0 < fromMs < nowMs})
     * when any of these hold:
     * <ul>
     *   <li>it starts before the oldest retained message. Retention and a topic that is newer than the
     *       window look the same from here, so a window reaching back before a topic's first message counts;</li>
     *   <li>nothing is retained ({@code earliestMs} {@code null}). Deleted and never written look the same,
     *       and a complete, empty result would claim nothing happened;</li>
     *   <li>it starts before the retention horizon, {@code nowMs} minus a time-based retention, even while
     *       older messages are still there.</li>
     * </ul>
     * {@code fromMs = 0} asks for everything retained, which is never a truncation. A retention of
     * {@code -1} (infinite), {@code 0} (none: messages last until acknowledged) or {@code null} (unknown)
     * gives no horizon.
     */
    static boolean truncatedByRetention(long fromMs, Long earliestMs, Integer retentionMinutes, long nowMs) {
        if (fromMs <= 0 || fromMs >= nowMs) {
            return false;
        }
        if (earliestMs == null || earliestMs > fromMs) {
            return true;
        }
        return retentionMinutes != null && retentionMinutes > 0
                && fromMs < nowMs - TimeUnit.MINUTES.toMillis(retentionMinutes);
    }

    /**
     * The retention time that applies to {@code topic} (its own policy, else its namespace's, else the
     * broker default) in minutes, or {@code null} if it can't be read. Then only retained messages decide
     * truncation.
     */
    private Integer retentionMinutes(String topic) {
        try {
            RetentionPolicies retention = admin.topicPolicies().getRetention(topic, true);
            return retention == null ? null : retention.getRetentionTimeInMinutes();
        } catch (PulsarAdminException | RuntimeException | LinkageError e) {
            // LinkageError: the classloader mix described above, should this response ever trigger it.
            LOG.warn("Couldn't read the retention of {}; checking truncation against retained messages only: {}",
                    topic, e.toString());
            return null;
        }
    }

    /**
     * The single ledger-backed log behind {@code topic}: the topic itself when non-partitioned, its only
     * partition when it has one (Kafka clients over KoP create single-partition topics).
     *
     * @throws IllegalArgumentException if the topic doesn't exist (a subscribe would auto-create it) or
     *         has more than one partition
     */
    String singleLog(String topic) throws PulsarAdminException {
        int partitions = admin.topics().getPartitionedTopicMetadata(topic).partitions;
        if (partitions > 1) {
            throw new IllegalArgumentException("windowed queries need a non-partitioned or single-partition topic; "
                    + topic + " has " + partitions + " partitions");
        }
        String log = partitions == 1 ? topic + "-partition-0" : topic;
        String namespace = TopicName.get(topic).getNamespace();
        if (!admin.topics().getList(namespace).contains(log)) {
            throw new IllegalArgumentException("source topic " + topic + " not found");
        }
        return log;
    }

    /** Log-clock time of the message at {@code start}, or {@code null} if none is readable. */
    private Long logTimeAt(String log, MessageId start, boolean inclusive) throws Exception {
        // One message is all this needs; the default 1,000-message prefetch makes the broker read (for the
        // earliest message, from cold storage) far more than that.
        ReaderBuilder<byte[]> builder = client.newReader(Schema.BYTES).topic(log).startMessageId(start)
                .receiverQueueSize(1);
        if (inclusive) {
            builder = builder.startMessageIdInclusive();
        }
        try (Reader<byte[]> reader = builder.create()) {
            Message<byte[]> m = reader.readNext(READ_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            return m == null ? null : m.getBrokerPublishTime().orElse(m.getPublishTime());
        }
    }
}
