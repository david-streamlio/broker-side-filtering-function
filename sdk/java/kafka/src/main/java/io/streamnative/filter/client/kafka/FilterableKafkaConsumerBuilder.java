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

import io.streamnative.filter.client.PrivateQuery;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.ClientType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.PulsarClientException;

import io.streamnative.filter.api.FilterException;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.internal.WindowProgress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;

/**
 * Fluent builder for a filtered Kafka consumer. Returned by
 * {@link FilterableKafkaConsumers#newConsumer(Session, Properties)}.
 *
 * <p>The terminal {@link #subscribe()} call returns a standard
 * {@link org.apache.kafka.clients.consumer.Consumer Consumer&lt;K, V&gt;} that ALSO implements
 * {@link io.streamnative.filter.api.FilterControl} — callers cast when they want dynamic
 * selector ops.
 *
 * <p>Required configuration: at least one source Pulsar topic. The {@code selector} may be
 * omitted (or {@code null}/blank) — in that case the consumer only filters on the topic
 * constraint.
 *
 * @param <K> key type — must match the configured {@code KEY_DESERIALIZER_CLASS_CONFIG}
 * @param <V> value type — must match the configured {@code VALUE_DESERIALIZER_CLASS_CONFIG}
 */
public final class FilterableKafkaConsumerBuilder<K, V> {

    private final Session session;
    private final Properties kafkaProps;
    private final List<String> sourceTopics = new ArrayList<>();
    private String selector;
    private TimeWindow window;

    // Hooks for unit testing: swap in a fake KafkaConsumer factory.
    private Function<Properties, Consumer<K, V>> kafkaConsumerFactory = props -> new KafkaConsumer<>(props);

    FilterableKafkaConsumerBuilder(Session session, Properties kafkaProps) {
        this.session    = Objects.requireNonNull(session, "session");
        this.kafkaProps = Objects.requireNonNull(kafkaProps, "kafkaProps");
    }

    /**
     * Set a single source Pulsar topic. Replaces any previously-configured topics.
     */
    public FilterableKafkaConsumerBuilder<K, V> topic(String topic) {
        Objects.requireNonNull(topic, "topic");
        sourceTopics.clear();
        sourceTopics.add(topic);
        return this;
    }

    /**
     * Set multiple source Pulsar topics. The composed selector uses {@code __topic__ IN (...)}.
     * Replaces any previously-configured topics.
     */
    public FilterableKafkaConsumerBuilder<K, V> topics(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }
        sourceTopics.clear();
        sourceTopics.addAll(topics);
        return this;
    }

    /**
     * Set the user-side JMS selector. May be {@code null} or blank to filter only on the topic
     * constraint.
     */
    public FilterableKafkaConsumerBuilder<K, V> selector(String selector) {
        this.selector = selector;
        return this;
    }

    /**
     * Test hook: substitute the {@link KafkaConsumer} construction step with a custom factory.
     * Production code should not call this — the default constructs a real {@code KafkaConsumer}
     * from the configured {@code kafkaProps}.
     */
    /**
     * Read part of the source topic's log: history, live events, or both (see {@link TimeWindow}). The
     * consumer returned by {@link #subscribe()} also implements
     * {@link io.streamnative.filter.api.WindowControl}, which reports when the window's history has
     * been delivered and, for a bounded window, when it is complete.
     *
     * <p>Windows that read history need exactly one source topic, run on a dedicated server-side filter,
     * and always read their results from the beginning ({@code auto.offset.reset=earliest}); their
     * selector is fixed. {@link TimeWindow#liveOnly()} runs as an ordinary live query.
     */
    public FilterableKafkaConsumerBuilder<K, V> window(TimeWindow window) {
        this.window = Objects.requireNonNull(window, "window");
        return this;
    }

    FilterableKafkaConsumerBuilder<K, V> withKafkaConsumerFactory(Function<Properties, Consumer<K, V>> factory) {
        this.kafkaConsumerFactory = Objects.requireNonNull(factory, "kafkaConsumerFactory");
        return this;
    }

    /**
     * Register the query, attach a real {@link KafkaConsumer} subscribed to the KoP-mapped name
     * of the private output Pulsar topic, and return it wrapped so it also implements
     * {@link io.streamnative.filter.api.FilterControl}.
     *
     * @throws IllegalStateException    if no source topic was configured, or a window that reads
     *                                  history has more than one.
     * @throws IllegalArgumentException if {@code selector} is non-blank but fails to parse.
     * @throws FilterException          if the query couldn't be registered with the server.
     */
    public Consumer<K, V> subscribe() {
        if (sourceTopics.isEmpty()) {
            throw new IllegalStateException("At least one source topic is required");
        }

        // Pass through the Kafka deserializer class FQNs (when configured) so the orchestrator
        // can detect schema mismatches across registrations on the same input topic.
        String keyDeserializer   = kafkaProps.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        String valueDeserializer = kafkaProps.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        boolean windowed = window != null && !window.isLiveOnly();
        if (windowed && sourceTopics.size() != 1) {
            throw new IllegalStateException("A window that reads history needs exactly one source topic");
        }
        PrivateQuery pq;
        try {
            if (windowed) {
                TimeWindow.Bounds bounds = window.resolve(System.currentTimeMillis());
                pq = session.registerWindowedQuery(sourceTopics.get(0), selector, keyDeserializer, valueDeserializer,
                        null, ClientType.KAFKA, bounds.fromMs(), bounds.toMs());
            } else {
                pq = session.registerPrivateQuery(List.copyOf(sourceTopics), selector,
                        keyDeserializer, valueDeserializer, ClientType.KAFKA);
            }
        } catch (PulsarClientException e) {
            // The control plane runs on Pulsar; Kafka callers get a client-neutral exception.
            throw new FilterException("Failed to register the filtered query", e);
        }
        try {
            String kafkaTopic = kafkaTopicNameFor(pq.privateTopic());
            Properties props = kafkaProps;
            if (windowed) {
                // A window's results are read once, from the start of its private topic.
                props = new Properties();
                props.putAll(kafkaProps);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            }
            Consumer<K, V> underlying = kafkaConsumerFactory.apply(props);
            try {
                underlying.subscribe(List.of(kafkaTopic));
            } catch (RuntimeException e) {
                try { underlying.close(); } catch (Exception ignored) {}
                throw e;
            }
            String stored = (selector == null || selector.isBlank()) ? null : selector;
            if (window == null) {
                return new FilteringKafkaConsumer<>(session, pq, underlying, stored);
            }
            WindowProgress progress = window.isLiveOnly()
                    ? WindowProgress.liveOnly()
                    : WindowProgress.forWindow(window.isOpenEnded());
            return new WindowedKafkaConsumer<>(session, pq, underlying, stored, progress);
        } catch (RuntimeException e) {
            session.retirePrivateQuery(pq);
            throw e;
        }
    }

    /**
     * Resolve the Kafka topic name for a Pulsar private topic FQN. With KoP's default
     * {@code kafkaTenant}/{@code kafkaNamespace} mapping, the Kafka name is the last segment of
     * the Pulsar FQN — e.g. {@code persistent://public/default/filtered-<uuid>} →
     * {@code filtered-<uuid>}.
     *
     * <p>Package-private and exposed to tests; future versions may add a builder hook for
     * non-standard KoP mappings.
     */
    static String kafkaTopicNameFor(String pulsarTopicFqn) {
        int slash = pulsarTopicFqn.lastIndexOf('/');
        return slash < 0 ? pulsarTopicFqn : pulsarTopicFqn.substring(slash + 1);
    }
}
