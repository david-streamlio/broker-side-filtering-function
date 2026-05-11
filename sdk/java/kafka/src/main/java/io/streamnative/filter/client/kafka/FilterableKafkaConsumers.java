package io.streamnative.filter.client.kafka;

import io.streamnative.filter.client.Session;

import java.util.Properties;

/**
 * Entry point for the Kafka-flavoured selector-filter SDK. Mirrors
 * {@code Session.newConsumer()} on the Pulsar side: hand the builder a {@link Session} and the
 * Kafka client configuration, then chain
 * {@link FilterableKafkaConsumerBuilder#topic(String) topic} /
 * {@link FilterableKafkaConsumerBuilder#selector(String) selector} /
 * {@link FilterableKafkaConsumerBuilder#subscribe() subscribe()}.
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.put(BOOTSTRAP_SERVERS_CONFIG, "broker:9094"); // KoP listener
 * props.put(GROUP_ID_CONFIG, "my-group");
 * props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
 * props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
 *
 * Consumer<String, byte[]> c = FilterableKafkaConsumers.newConsumer(session, props)
 *         .topic("persistent://public/in/trades")
 *         .selector("region = 'US'")
 *         .subscribe();
 *
 * ((FilterControl) c).setSelector("region IN ('US','UK')");
 * }</pre>
 *
 * <p><b>Deployment requirement:</b> the broker must be KoP-enabled with
 * {@code kafkaEntryFormat=pulsar_non_batched}; the {@link Session#getPrivateTopicTenant()
 * privateTopicTenant} / {@link Session#getPrivateTopicNamespace() privateTopicNamespace} pair
 * must match KoP's {@code kafkaTenant}/{@code kafkaNamespace} configuration so the private
 * output Pulsar topic is reachable by Kafka clients.
 */
public final class FilterableKafkaConsumers {

    private FilterableKafkaConsumers() {}

    /**
     * Create a builder for a filtered Kafka consumer. The {@code kafkaProps} are passed verbatim
     * to {@code new KafkaConsumer<>(props)} at {@code subscribe()} time; configure {@code
     * bootstrap.servers} (KoP listener), {@code group.id}, and key/value deserializers.
     */
    public static <K, V> FilterableKafkaConsumerBuilder<K, V> newConsumer(Session session, Properties kafkaProps) {
        return new FilterableKafkaConsumerBuilder<>(session, kafkaProps);
    }
}
