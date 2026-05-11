package io.streamnative.filter.client.kafka;

import io.streamnative.filter.client.PrivateQuery;
import io.streamnative.filter.client.Session;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.pulsar.client.api.PulsarClientException;

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
 * {@link io.streamnative.filter.client.FilterControl} — callers cast when they want dynamic
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
    FilterableKafkaConsumerBuilder<K, V> withKafkaConsumerFactory(Function<Properties, Consumer<K, V>> factory) {
        this.kafkaConsumerFactory = Objects.requireNonNull(factory, "kafkaConsumerFactory");
        return this;
    }

    /**
     * Register the query, attach a real {@link KafkaConsumer} subscribed to the KoP-mapped name
     * of the private output Pulsar topic, and return it wrapped so it also implements
     * {@link io.streamnative.filter.client.FilterControl}.
     *
     * @throws IllegalArgumentException if no source topic was configured, or {@code selector} is
     *                                  non-blank but fails to parse.
     * @throws PulsarClientException    if control-topic publish fails.
     */
    public Consumer<K, V> subscribe() throws PulsarClientException {
        if (sourceTopics.isEmpty()) {
            throw new IllegalStateException("At least one source topic is required");
        }

        PrivateQuery pq = session.registerPrivateQuery(List.copyOf(sourceTopics), selector);
        try {
            String kafkaTopic = kafkaTopicNameFor(pq.privateTopic());
            Consumer<K, V> underlying = kafkaConsumerFactory.apply(kafkaProps);
            try {
                underlying.subscribe(List.of(kafkaTopic));
            } catch (RuntimeException e) {
                try { underlying.close(); } catch (Exception ignored) {}
                throw e;
            }
            String stored = (selector == null || selector.isBlank()) ? null : selector;
            return new FilteringKafkaConsumer<>(session, pq, underlying, stored);
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
