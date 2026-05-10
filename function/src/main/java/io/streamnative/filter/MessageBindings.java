package io.streamnative.filter;

import org.apache.pulsar.functions.api.Record;

import java.util.Map;
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
                () -> record.getKey().orElse(null),
                () -> TopicNames.normalize(record.getTopicName().orElse(null)),
                () -> {
                    Map<String, String> p = record.getProperties();
                    return p == null ? Map.of() : p;
                });
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
