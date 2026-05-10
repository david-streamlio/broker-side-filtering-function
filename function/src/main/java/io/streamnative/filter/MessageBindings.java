package io.streamnative.filter;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Selector-evaluation bindings backed by a Pulsar {@link Record}.
 *
 * <p>Resolution is <strong>lazy</strong>: the {@code __topic__} suffix-stripping pass and the
 * call to {@code record.getProperties()} only run when a selector first references those bindings.
 * A selector that only mentions {@code __key__} never pays the cost of touching topic or
 * properties for its evaluation, and a no-match drop never pays for resolving properties at all.
 *
 * <p>Records observed which bindings were actually touched, via {@link #topicResolved()},
 * {@link #propertiesResolved()}. Useful for tests and for measuring fast-path coverage.
 *
 * <p><strong>Source-format independence.</strong> The {@code __key__} binding is resolved through
 * {@link Message#getKeyBytes()}, which transparently decodes a base64-encoded key produced by the
 * Kafka-on-Pulsar (KoP) bridge when {@code kafkaEntryFormat=pulsar_non_batched} is configured.
 * Selector authors compose against the <em>logical</em> key in both cases: a Kafka producer that
 * sends {@code b"BTC"} and a native Pulsar producer that sends {@code "BTC"} both surface as
 * {@code __key__ = 'BTC'} to the selector.
 */
public final class MessageBindings {

    public static final String KEY_BINDING = "__key__";
    public static final String TOPIC_BINDING = "__topic__";

    private final Supplier<String> keySupplier;
    private final Supplier<String> topicSupplier;
    private final Supplier<Map<String, String>> propertiesSupplier;

    private String key;
    private boolean keyResolved;
    private String topic;
    private boolean topicResolved;
    private Map<String, String> properties;
    private boolean propertiesResolved;

    public MessageBindings(String key, String topic, Map<String, String> properties) {
        this.keySupplier        = () -> key;
        this.topicSupplier      = () -> topic;
        this.propertiesSupplier = () -> properties == null ? Map.of() : properties;
    }

    private MessageBindings(Supplier<String> keySupplier,
                            Supplier<String> topicSupplier,
                            Supplier<Map<String, String>> propertiesSupplier) {
        this.keySupplier        = keySupplier;
        this.topicSupplier      = topicSupplier;
        this.propertiesSupplier = propertiesSupplier;
    }

    public static MessageBindings from(Record<?> record) {
        return new MessageBindings(
                () -> resolveLogicalKey(record),
                () -> TopicNames.normalize(record.getTopicName().orElse(null)),
                () -> {
                    Map<String, String> p = record.getProperties();
                    return p == null ? Map.of() : p;
                });
    }

    /**
     * Resolves the message's <em>logical</em> key, accounting for KoP base64-encoded keys.
     *
     * <p>When {@link Record#getMessage()} is present (the normal case for inputs to a Pulsar
     * Function), we read the key via {@link Message#getKeyBytes()}: for KoP-bridged messages this
     * returns the decoded Kafka key bytes; for native Pulsar messages it returns the UTF-8 bytes
     * of {@link Message#getKey()}. We then convert to a String (UTF-8). Falls back to
     * {@link Record#getKey()} when the message is unavailable (typically only in unit tests).
     */
    private static String resolveLogicalKey(Record<?> record) {
        Optional<? extends Message<?>> msg = record.getMessage();
        if (msg.isPresent() && msg.get().hasKey()) {
            byte[] bytes = msg.get().getKeyBytes();
            if (bytes != null) {
                return new String(bytes, StandardCharsets.UTF_8);
            }
        }
        return record.getKey().orElse(null);
    }

    public String getKey() {
        if (!keyResolved) {
            key = keySupplier.get();
            keyResolved = true;
        }
        return key;
    }

    public String getTopic() {
        if (!topicResolved) {
            topic = topicSupplier.get();
            topicResolved = true;
        }
        return topic;
    }

    public Map<String, String> getProperties() {
        if (!propertiesResolved) {
            properties = propertiesSupplier.get();
            propertiesResolved = true;
        }
        return properties;
    }

    public Object lookup(String name) {
        if (KEY_BINDING.equals(name)) {
            return getKey();
        }
        if (TOPIC_BINDING.equals(name)) {
            return getTopic();
        }
        return getProperties().get(name);
    }

    /** Test/observability hook: true iff the topic binding has been resolved. */
    public boolean topicResolved() {
        return topicResolved;
    }

    /** Test/observability hook: true iff the properties binding has been resolved. */
    public boolean propertiesResolved() {
        return propertiesResolved;
    }

    /** Test/observability hook: true iff the key binding has been resolved. */
    public boolean keyResolved() {
        return keyResolved;
    }
}
