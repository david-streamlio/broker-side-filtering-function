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

package io.streamnative.filter.client.kafka;

import io.streamnative.filter.client.FilterControl;
import io.streamnative.filter.client.PrivateQuery;
import io.streamnative.filter.client.Session;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Wraps a real Kafka {@link Consumer} subscribed (via KoP) to the private filtered output topic,
 * and adds {@link FilterControl} so callers can swap the active selector after construction.
 *
 * <p>All read/commit/seek/etc. methods delegate to the underlying consumer. The
 * <b>subscription-management</b> methods —
 * {@link #subscribe(Collection)}, {@link #assign(Collection)}, {@link #unsubscribe()} and their
 * variants — throw {@link UnsupportedOperationException}. The filtered subscription is fixed at
 * builder time; callers who want a different subscription should build a new filtered consumer.
 *
 * <p>{@link #close()} closes the underlying Kafka consumer and retires the registered query on
 * the Pulsar control topic.
 */
final class FilteringKafkaConsumer<K, V> implements Consumer<K, V>, FilterControl {

    private static final Logger LOG = LoggerFactory.getLogger(FilteringKafkaConsumer.class);

    private static final String FROZEN_SUBSCRIPTION_MSG =
            "Filtered Kafka consumer's subscription is fixed at build time; "
                    + "create a new filtered consumer for a different subscription.";

    private final Session session;
    private final PrivateQuery query;
    private final Consumer<K, V> delegate;
    private volatile String userSelector;
    private volatile boolean closed;

    FilteringKafkaConsumer(Session session, PrivateQuery query, Consumer<K, V> delegate, String initialUserSelector) {
        this.session      = session;
        this.query        = query;
        this.delegate     = delegate;
        this.userSelector = initialUserSelector;
    }

    // ------------------------------------------------------------------ FilterControl

    @Override
    public synchronized void setSelector(String selector) throws PulsarClientException {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        session.updatePrivateQuery(query, selector);
        this.userSelector = (selector == null || selector.isBlank()) ? null : selector;
    }

    @Override public String       currentSelector() { return userSelector; }
    @Override public String       queryId()         { return query.queryId(); }
    @Override public List<String> sourceTopics()    { return query.sourceTopics(); }
    @Override public String       privateTopic()    { return query.privateTopic(); }

    // ------------------------------------------------------------------ Frozen subscription

    @Override public void subscribe(Collection<String> topics) {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG);
    }

    @Override public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG);
    }

    @Override public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG);
    }

    @Override public void subscribe(Pattern pattern) {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG);
    }

    @Override public void assign(Collection<TopicPartition> partitions) {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG);
    }

    @Override public void unsubscribe() {
        throw new UnsupportedOperationException(FROZEN_SUBSCRIPTION_MSG
                + " Call close() to terminate this consumer and retire its query.");
    }

    // ------------------------------------------------------------------ Lifecycle

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            delegate.close();
        } catch (RuntimeException e) {
            LOG.warn("Failed to close underlying Kafka consumer for queryId={}: {}",
                    query.queryId(), e.getMessage());
            try {
                session.retirePrivateQuery(query);
            } catch (Exception ignored) {}
            throw e;
        }
        session.retirePrivateQuery(query);
    }

    @Override
    public synchronized void close(Duration timeout) {
        if (closed) return;
        closed = true;
        try {
            delegate.close(timeout);
        } catch (RuntimeException e) {
            LOG.warn("Failed to close (timeout={}) underlying Kafka consumer for queryId={}: {}",
                    timeout, query.queryId(), e.getMessage());
            try {
                session.retirePrivateQuery(query);
            } catch (Exception ignored) {}
            throw e;
        }
        session.retirePrivateQuery(query);
    }

    @Override public void wakeup() { delegate.wakeup(); }

    // ------------------------------------------------------------------ Read

    @Override public ConsumerRecords<K, V> poll(long timeoutMs)        { return delegate.poll(timeoutMs); }
    @Override public ConsumerRecords<K, V> poll(Duration timeout)      { return delegate.poll(timeout); }

    // ------------------------------------------------------------------ Subscription state (read-only)

    @Override public Set<TopicPartition> assignment()                    { return delegate.assignment(); }
    @Override public Set<String> subscription()                          { return delegate.subscription(); }
    @Override public Set<TopicPartition> paused()                        { return delegate.paused(); }
    @Override public ConsumerGroupMetadata groupMetadata()               { return delegate.groupMetadata(); }

    // ------------------------------------------------------------------ Commit

    @Override public void commitSync()                                   { delegate.commitSync(); }
    @Override public void commitSync(Duration timeout)                   { delegate.commitSync(timeout); }
    @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) { delegate.commitSync(offsets); }
    @Override public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        delegate.commitSync(offsets, timeout);
    }
    @Override public void commitAsync()                                  { delegate.commitAsync(); }
    @Override public void commitAsync(OffsetCommitCallback callback)     { delegate.commitAsync(callback); }
    @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        delegate.commitAsync(offsets, callback);
    }

    // ------------------------------------------------------------------ Seek / position

    @Override public void seek(TopicPartition partition, long offset)                              { delegate.seek(partition, offset); }
    @Override public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)      { delegate.seek(partition, offsetAndMetadata); }
    @Override public void seekToBeginning(Collection<TopicPartition> partitions)                   { delegate.seekToBeginning(partitions); }
    @Override public void seekToEnd(Collection<TopicPartition> partitions)                         { delegate.seekToEnd(partitions); }
    @Override public long position(TopicPartition partition)                                       { return delegate.position(partition); }
    @Override public long position(TopicPartition partition, Duration timeout)                     { return delegate.position(partition, timeout); }

    // ------------------------------------------------------------------ Committed offsets

    @Override public OffsetAndMetadata committed(TopicPartition partition)                         { return delegate.committed(partition); }
    @Override public OffsetAndMetadata committed(TopicPartition partition, Duration timeout)       { return delegate.committed(partition, timeout); }
    @Override public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> ps)      { return delegate.committed(ps); }
    @Override public Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> ps, Duration timeout) {
        return delegate.committed(ps, timeout);
    }

    // ------------------------------------------------------------------ Pause / resume

    @Override public void pause(Collection<TopicPartition> partitions)                             { delegate.pause(partitions); }
    @Override public void resume(Collection<TopicPartition> partitions)                            { delegate.resume(partitions); }

    // ------------------------------------------------------------------ Metadata

    @Override public Map<MetricName, ? extends Metric> metrics()                                   { return delegate.metrics(); }
    @Override public List<PartitionInfo> partitionsFor(String topic)                               { return delegate.partitionsFor(topic); }
    @Override public List<PartitionInfo> partitionsFor(String topic, Duration timeout)             { return delegate.partitionsFor(topic, timeout); }
    @Override public Map<String, List<PartitionInfo>> listTopics()                                 { return delegate.listTopics(); }
    @Override public Map<String, List<PartitionInfo>> listTopics(Duration timeout)                 { return delegate.listTopics(timeout); }

    @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> ts) {
        return delegate.offsetsForTimes(ts);
    }
    @Override public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> ts, Duration timeout) {
        return delegate.offsetsForTimes(ts, timeout);
    }
    @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> ps)     { return delegate.beginningOffsets(ps); }
    @Override public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> ps, Duration timeout) {
        return delegate.beginningOffsets(ps, timeout);
    }
    @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> ps)           { return delegate.endOffsets(ps); }
    @Override public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> ps, Duration timeout) {
        return delegate.endOffsets(ps, timeout);
    }
    @Override public OptionalLong currentLag(TopicPartition partition)                             { return delegate.currentLag(partition); }

    @Override public void enforceRebalance()                                                       { delegate.enforceRebalance(); }
    @Override public void enforceRebalance(String reason)                                          { delegate.enforceRebalance(reason); }
}
