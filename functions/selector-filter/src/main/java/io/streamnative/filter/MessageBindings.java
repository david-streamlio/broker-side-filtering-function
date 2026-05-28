package io.streamnative.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 *
 * <p><strong>Transparent base64 property decoding.</strong> When KoP attaches the
 * {@link #KOP_INTERNAL_HEADER_FORMAT} disambiguation property naming one or more user properties
 * as {@code "base64"}-encoded, the {@link #lookup(String)} path decodes those values once and
 * surfaces the UTF-8 form (when the decoded bytes are valid UTF-8). The decoded value is cached
 * for the lifetime of the bindings so repeated lookups within a single record evaluation reuse
 * the same {@code String} instance. Properties whose base64-decoded bytes are not valid UTF-8 are
 * passed through unchanged — selectors must compare against the base64 string in that case.
 */
public final class MessageBindings {

    private static final Logger LOG = LoggerFactory.getLogger(MessageBindings.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static final String KEY_BINDING = "__key__";
    public static final String TOPIC_BINDING = "__topic__";

    /**
     * KoP-injected disambiguation property whose JSON value names user properties whose raw value
     * is base64-encoded (vs raw UTF-8). Hidden from selectors, but consulted by
     * {@link #lookup(String)} to drive transparent decoding of the named properties.
     */
    public static final String KOP_INTERNAL_HEADER_FORMAT = "__ksn_internal_header_format";

    private final Supplier<String> keySupplier;
    private final Supplier<String> topicSupplier;
    private final Supplier<Map<String, String>> propertiesSupplier;
    private final Supplier<byte[]> payloadSupplier;

    private String key;
    private boolean keyResolved;
    private String topic;
    private boolean topicResolved;
    private Map<String, String> properties;
    private boolean propertiesResolved;

    // Lazily-parsed JSON payload — used when a selector references a field not found in properties.
    private JsonNode payloadJson;
    private boolean payloadResolved;

    // Lazily-built base64-decode state. The format-parse runs at most once per record; the cache
    // is populated on demand and holds one entry per base64-flagged property that has been looked
    // up so far (decoded or raw fallback).
    private boolean formatParsed;
    private Set<String> base64FlaggedNames;
    private Map<String, String> decodedCache;

    /** Test constructor — no payload; only key/topic/properties are bound. */
    public MessageBindings(String key, String topic, Map<String, String> properties) {
        this.keySupplier        = () -> key;
        this.topicSupplier      = () -> topic;
        this.propertiesSupplier = () -> properties == null ? Map.of() : properties;
        this.payloadSupplier    = null;
    }

    /** Test constructor — includes raw payload bytes for JSON-field selectors. */
    public MessageBindings(String key, String topic, Map<String, String> properties, byte[] payload) {
        this.keySupplier        = () -> key;
        this.topicSupplier      = () -> topic;
        this.propertiesSupplier = () -> properties == null ? Map.of() : properties;
        this.payloadSupplier    = () -> payload;
    }

    private MessageBindings(Supplier<String> keySupplier,
                            Supplier<String> topicSupplier,
                            Supplier<Map<String, String>> propertiesSupplier,
                            Supplier<byte[]> payloadSupplier) {
        this.keySupplier        = keySupplier;
        this.topicSupplier      = topicSupplier;
        this.propertiesSupplier = propertiesSupplier;
        this.payloadSupplier    = payloadSupplier;
    }

    /**
     * Creates bindings for a live Pulsar record. The payload is passed explicitly so the
     * function can resolve JSON fields (e.g. {@code side = 'buy'}, {@code price > 50000})
     * when properties are empty — as is the case for KoP-bridged Coinbase messages.
     */
    public static MessageBindings from(Record<?> record, byte[] payload) {
        return new MessageBindings(
                () -> resolveLogicalKey(record),
                () -> TopicNames.normalize(record.getTopicName().orElse(null)),
                () -> {
                    Map<String, String> p = record.getProperties();
                    return p == null ? Map.of() : p;
                },
                () -> payload);
    }

    /** Backward-compatible overload — no payload; JSON-field selectors will not match. */
    public static MessageBindings from(Record<?> record) {
        return from(record, null);
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
        if (KOP_INTERNAL_HEADER_FORMAT.equals(name)) {
            // Hide KoP's internal disambiguation property from selectors; it must not be a
            // filterable signal even if a selector author tries to reference it by name.
            return null;
        }
        Map<String, String> props = getProperties();
        String raw = props.get(name);
        if (raw != null) {
            if (isBase64Flagged(name, props)) {
                return decodedOrRaw(name, raw);
            }
            return raw;
        }
        // Properties did not contain the name — fall through to JSON payload fields.
        // This allows selectors like `side = 'buy'` or `price > 50000` to work against
        // messages (e.g. KoP-bridged Coinbase tickers) whose payload is JSON but whose
        // Pulsar properties map is empty.
        return getJsonField(name);
    }

    /**
     * Lazily parses the payload as a JSON object and returns the named field's value.
     * Returns {@link Number} for numeric nodes (enabling JMS numeric comparisons like
     * {@code price > 50000}), {@link Boolean} for boolean nodes, {@link String} otherwise.
     * Returns {@code null} when the payload is absent, not a JSON object, or does not
     * contain the field.
     *
     * <p>Many financial APIs (including Coinbase) encode prices and volumes as quoted
     * decimals: {@code "price":"77351.21"}. This method coerces such strings to
     * {@link Double} so numeric comparisons work correctly.
     */
    private Object getJsonField(String name) {
        if (!payloadResolved) {
            payloadResolved = true;
            if (payloadSupplier != null) {
                byte[] bytes = payloadSupplier.get();
                if (bytes != null && bytes.length > 0) {
                    try {
                        JsonNode root = MAPPER.readTree(bytes);
                        payloadJson = (root != null && root.isObject()) ? root : null;
                    } catch (Exception e) {
                        LOG.debug("Payload is not valid JSON — JSON-field selectors will not match: {}", e.toString());
                    }
                }
            }
        }
        if (payloadJson == null) {
            return null;
        }
        JsonNode node = payloadJson.get(name);
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        String text = node.asText();
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException ignored) {
            return text;
        }
    }

    /**
     * Lazily inspects {@link #KOP_INTERNAL_HEADER_FORMAT}, parses its JSON value once per record,
     * and returns whether {@code name} is one of the properties KoP marked as base64-encoded.
     */
    private boolean isBase64Flagged(String name, Map<String, String> props) {
        if (!formatParsed) {
            String fmt = props.get(KOP_INTERNAL_HEADER_FORMAT);
            base64FlaggedNames = fmt == null ? Collections.emptySet() : parseFormatSpec(fmt);
            formatParsed = true;
        }
        return base64FlaggedNames.contains(name);
    }

    /**
     * Returns the cached decoded form of {@code raw} (decoding on first request), falling back to
     * {@code raw} when the base64-decode succeeds but the bytes aren't valid UTF-8, or when the
     * input isn't legal base64.
     */
    private String decodedOrRaw(String name, String raw) {
        if (decodedCache == null) {
            decodedCache = new HashMap<>();
        } else {
            String cached = decodedCache.get(name);
            if (cached != null) {
                return cached;
            }
        }
        String resolved = tryBase64DecodeToUtf8(raw);
        if (resolved == null) {
            resolved = raw;
        }
        decodedCache.put(name, resolved);
        return resolved;
    }

    /**
     * Returns the UTF-8 form of {@code base64} when it decodes cleanly to a valid UTF-8 byte
     * sequence; {@code null} when either the input is not valid base64 or the bytes are not
     * valid UTF-8. The caller falls back to the raw string in the {@code null} case.
     */
    private static String tryBase64DecodeToUtf8(String base64) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            return null;
        }
        if (!isValidUtf8(decoded)) {
            return null;
        }
        return new String(decoded, StandardCharsets.UTF_8);
    }

    private static boolean isValidUtf8(byte[] bytes) {
        try {
            StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return true;
        } catch (CharacterCodingException e) {
            return false;
        }
    }

    /**
     * Parses KoP's {@code __ksn_internal_header_format} JSON. The expected shape is a JSON object
     * whose values name an encoding (e.g. {@code "base64"} or {@code "string"}). This method
     * collects property names whose value is the string {@code "base64"} (case-insensitive). Any
     * other JSON shape, or a parse failure, yields an empty set — base64 auto-decode then never
     * fires for that record, which is the safe default.
     */
    private static Set<String> parseFormatSpec(String json) {
        try {
            JsonNode root = MAPPER.readTree(json);
            if (root == null || !root.isObject()) {
                return Collections.emptySet();
            }
            Set<String> flagged = new HashSet<>();
            Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> e = fields.next();
                JsonNode value = e.getValue();
                if (value != null && value.isTextual()
                        && "base64".equalsIgnoreCase(value.asText())) {
                    flagged.add(e.getKey());
                }
            }
            return flagged.isEmpty() ? Collections.emptySet() : flagged;
        } catch (Exception e) {
            LOG.debug("Failed to parse {} JSON: {}", KOP_INTERNAL_HEADER_FORMAT, e.toString());
            return Collections.emptySet();
        }
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

    /** Test/observability hook: true iff the JSON payload has been parsed (or attempted). */
    public boolean payloadResolved() {
        return payloadResolved;
    }
}
