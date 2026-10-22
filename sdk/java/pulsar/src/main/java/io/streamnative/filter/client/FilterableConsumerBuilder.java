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
import io.streamnative.filter.common.ValueSchemaInfo;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Fluent builder for a filtered Pulsar consumer. Returned by {@link Session#newConsumer()} and
 * {@link Session#newConsumer(Schema)}.
 *
 * <p>The builder mirrors the most common subset of {@code org.apache.pulsar.client.api.ConsumerBuilder}
 * configuration. The terminal {@link #subscribe()} call returns a standard
 * {@link org.apache.pulsar.client.api.Consumer Consumer&lt;V&gt;} that ALSO implements
 * {@link FilterControl} — callers cast when they want dynamic-selector ops. For the no-schema
 * {@link Session#newConsumer()} path {@code V} is {@code byte[]}.
 *
 * <p>Required configuration: at least one source topic. The {@code selector} may be omitted
 * (or {@code null}/blank) — in that case the consumer only filters on the topic constraint.
 *
 * @param <V> the value type, fixed by the {@code Schema<V>} this builder was created with.
 */
public final class FilterableConsumerBuilder<V> {

    private final Session session;
    private final Schema<V> valueSchema;
    private final ValueSchemaInfo valueSchemaInfo;
    private final List<String> sourceTopics = new ArrayList<>();
    private String selector;
    private String subscriptionName;
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;
    private SubscriptionInitialPosition initialPosition = SubscriptionInitialPosition.Earliest;
    private TimeWindow window;

    /**
     * @param valueSchema     the Pulsar schema the underlying consumer decodes with
     * @param valueSchemaInfo the wire description forwarded to the orchestrator/filter function,
     *                        or {@code null} for the no-schema ({@code byte[]}) path
     */
    FilterableConsumerBuilder(Session session, Schema<V> valueSchema, ValueSchemaInfo valueSchemaInfo) {
        this.session         = Objects.requireNonNull(session, "session");
        this.valueSchema     = Objects.requireNonNull(valueSchema, "valueSchema");
        this.valueSchemaInfo = valueSchemaInfo;
    }

    /**
     * Set a single source topic. Replaces any previously-configured topics.
     */
    public FilterableConsumerBuilder<V> topic(String topic) {
        Objects.requireNonNull(topic, "topic");
        sourceTopics.clear();
        sourceTopics.add(topic);
        return this;
    }

    /**
     * Set multiple source topics. The composed selector will use {@code __topic__ IN (...)}.
     * Replaces any previously-configured topics.
     */
    public FilterableConsumerBuilder<V> topics(Collection<String> topics) {
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
    public FilterableConsumerBuilder<V> selector(String selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Subscription name used on the private output topic. Defaults to a unique
     * {@code session-<uuid>} value.
     */
    public FilterableConsumerBuilder<V> subscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
        return this;
    }

    /** Subscription type for the underlying consumer. Defaults to {@link SubscriptionType#Exclusive}. */
    public FilterableConsumerBuilder<V> subscriptionType(SubscriptionType type) {
        this.subscriptionType = Objects.requireNonNull(type, "subscriptionType");
        return this;
    }

    /**
     * Initial position for the underlying subscription. Defaults to
     * {@link SubscriptionInitialPosition#Earliest}.
     */
    public FilterableConsumerBuilder<V> subscriptionInitialPosition(SubscriptionInitialPosition pos) {
        this.initialPosition = Objects.requireNonNull(pos, "subscriptionInitialPosition");
        return this;
    }

    /**
     * Read part of the source topic's log: history, live events, or both (see {@link TimeWindow}). The
     * consumer returned by {@link #subscribe()} also implements {@link WindowControl}, which reports
     * when the window's history has been delivered and, for a bounded window, when it is complete.
     *
     * <p>Windows that read history need exactly one source topic, run on a dedicated server-side filter,
     * and always start at the beginning of their results ({@link #subscriptionInitialPosition} doesn't
     * apply); their selector is fixed. {@link TimeWindow#liveOnly()} runs as an ordinary live query.
     */
    public FilterableConsumerBuilder<V> window(TimeWindow window) {
        this.window = Objects.requireNonNull(window, "window");
        return this;
    }

    /**
     * Subscribe a real Pulsar consumer on the private output topic. The returned object is a
     * {@link Consumer Consumer&lt;V&gt;} that also implements {@link FilterControl} (and
     * {@link WindowControl} for a windowed query).
     *
     * @throws IllegalArgumentException if {@code selector} is non-blank but fails to parse
     *                                  against the JMS-selector grammar; the cause is the
     *                                  underlying {@code SelectorParseException}.
     * @throws PulsarClientException    if the underlying Pulsar subscribe or control-topic
     *                                  publish fails.
     */
    public Consumer<V> subscribe() throws PulsarClientException {
        if (sourceTopics.isEmpty()) {
            throw new IllegalStateException("At least one source topic is required");
        }
        if (window != null && window.isLiveOnly()) {
            return session.subscribeFiltered(List.copyOf(sourceTopics), selector, subscriptionName, subscriptionType,
                    initialPosition, valueSchema, valueSchemaInfo, WindowProgress.liveOnly());
        }
        if (window != null) {
            if (sourceTopics.size() != 1) {
                throw new IllegalStateException("A window that reads history needs exactly one source topic");
            }
            TimeWindow.Bounds bounds = window.resolve(System.currentTimeMillis());
            return session.subscribeWindowed(sourceTopics.get(0), selector, subscriptionName, subscriptionType,
                    valueSchema, valueSchemaInfo, bounds.fromMs(), bounds.toMs());
        }
        return session.subscribeFiltered(
                List.copyOf(sourceTopics),
                selector,
                subscriptionName,
                subscriptionType,
                initialPosition,
                valueSchema,
                valueSchemaInfo);
    }
}
