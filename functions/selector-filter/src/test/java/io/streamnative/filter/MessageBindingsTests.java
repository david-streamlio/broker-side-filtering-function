package io.streamnative.filter;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MessageBindingsTests {

    @Test
    void resolvesKey() {
        MessageBindings b = new MessageBindings("AAPL", "persistent://t/n/trades", Map.of());
        assertThat(b.lookup(MessageBindings.KEY_BINDING)).isEqualTo("AAPL");
    }

    @Test
    void resolvesTopic() {
        MessageBindings b = new MessageBindings(null, "persistent://t/n/trades", Map.of());
        assertThat(b.lookup(MessageBindings.TOPIC_BINDING)).isEqualTo("persistent://t/n/trades");
    }

    @Test
    void resolvesUserProperty() {
        MessageBindings b = new MessageBindings(null, null, Map.of("region", "US"));
        assertThat(b.lookup("region")).isEqualTo("US");
    }

    @Test
    void unknownPropertyReturnsNull() {
        MessageBindings b = new MessageBindings(null, null, Map.of("region", "US"));
        assertThat(b.lookup("country")).isNull();
    }

    @Test
    void unknownPseudoHeaderReturnsNull() {
        // We do not surface JMS pseudo-headers as bindings. If a property happens to have a
        // JMS-prefixed name, it is looked up like any other user property (no special handling).
        MessageBindings withProp = new MessageBindings("k", "t", Map.of("JMSMessageID", "1"));
        assertThat(withProp.lookup("JMSMessageID")).isEqualTo("1");

        MessageBindings withoutProp = new MessageBindings("k", "t", Map.of());
        assertThat(withoutProp.lookup("JMSMessageID")).isNull();
        assertThat(withoutProp.lookup("JMSPriority")).isNull();
    }

    @Test
    void nullPropertiesMapTolerated() {
        MessageBindings b = new MessageBindings("k", "t", null);
        assertThat(b.getProperties()).isEmpty();
        assertThat(b.lookup("anything")).isNull();
    }

    @Test
    void fromRecordStripsPartitionSuffix() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.of("AAPL"));
        Mockito.when(record.getTopicName()).thenReturn(Optional.of("persistent://t/n/trades-partition-2"));
        Mockito.when(record.getProperties()).thenReturn(Map.of("region", "US"));

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isEqualTo("AAPL");
        assertThat(b.getTopic()).isEqualTo("persistent://t/n/trades");
        assertThat(b.getProperties()).containsEntry("region", "US");
    }

    @Test
    void fromRecordWithMissingKeyAndTopic() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.empty());
        Mockito.when(record.getTopicName()).thenReturn(Optional.empty());
        Mockito.when(record.getProperties()).thenReturn(null);

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isNull();
        assertThat(b.getTopic()).isNull();
        assertThat(b.getProperties()).isEmpty();
    }

    @Test
    void fromRecordIsLazyForKeyOnlyAccess() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.of("AAPL"));

        MessageBindings b = MessageBindings.from(record);
        Object resolvedKey = b.lookup(MessageBindings.KEY_BINDING);

        assertThat(resolvedKey).isEqualTo("AAPL");
        assertThat(b.keyResolved()).isTrue();
        assertThat(b.topicResolved()).isFalse();
        assertThat(b.propertiesResolved()).isFalse();
        // getTopicName() / getProperties() were never invoked on the record.
        Mockito.verify(record, Mockito.never()).getTopicName();
        Mockito.verify(record, Mockito.never()).getProperties();
    }

    @Test
    void fromRecordResolvesTopicLazily() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getTopicName()).thenReturn(Optional.of("persistent://t/n/x-partition-3"));

        MessageBindings b = MessageBindings.from(record);
        Object topic = b.lookup(MessageBindings.TOPIC_BINDING);

        assertThat(topic).isEqualTo("persistent://t/n/x");
        assertThat(b.topicResolved()).isTrue();
        assertThat(b.propertiesResolved()).isFalse();
        Mockito.verify(record, Mockito.never()).getProperties();
    }

    @Test
    void fromRecordResolvesPropertiesOnDemandOnly() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getProperties()).thenReturn(Map.of("region", "US"));

        MessageBindings b = MessageBindings.from(record);
        Object value = b.lookup("region");

        assertThat(value).isEqualTo("US");
        assertThat(b.propertiesResolved()).isTrue();
        assertThat(b.topicResolved()).isFalse();
        Mockito.verify(record, Mockito.never()).getTopicName();
    }

    // ===== KoP / base64-encoded key handling =====

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void fromRecordReadsKeyViaMessageGetKeyBytes() {
        // Native Pulsar path: key is "AAPL", getKeyBytes returns its UTF-8 bytes.
        Record record = Mockito.mock(Record.class);
        Message message = Mockito.mock(Message.class);
        Mockito.when(record.getMessage()).thenReturn(Optional.of(message));
        Mockito.when(message.hasKey()).thenReturn(true);
        Mockito.when(message.getKeyBytes()).thenReturn("AAPL".getBytes(StandardCharsets.UTF_8));

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isEqualTo("AAPL");
        // We never had to fall back to record.getKey().
        Mockito.verify(record, Mockito.never()).getKey();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void fromRecordDecodesKoPBase64Key() {
        // KoP pulsar_non_batched: the producer sent b"BTC"; the Kafka bridge stored the Pulsar
        // key as base64 ("QlRD"). Message.getKeyBytes() transparently decodes back to "BTC".
        byte[] decoded = "BTC".getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(decoded);
        assertThat(encoded).isEqualTo("QlRD");

        Record record = Mockito.mock(Record.class);
        Message message = Mockito.mock(Message.class);
        Mockito.when(record.getMessage()).thenReturn(Optional.of(message));
        Mockito.when(message.hasKey()).thenReturn(true);
        Mockito.when(message.hasBase64EncodedKey()).thenReturn(true);
        // The Pulsar client's Message.getKeyBytes() returns the decoded bytes; we model that
        // behavior directly here.
        Mockito.when(message.getKeyBytes()).thenReturn(decoded);

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isEqualTo("BTC");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void fromRecordFallsBackToRecordGetKeyWhenMessageAbsent() {
        // No Message present (e.g. a synthetic test record). Resolution falls back to
        // Record.getKey() which is what the previous behavior used directly.
        Record record = Mockito.mock(Record.class);
        Mockito.when(record.getMessage()).thenReturn(Optional.empty());
        Mockito.when(record.getKey()).thenReturn(Optional.of("LEGACY"));

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isEqualTo("LEGACY");
    }

    // ===== KoP property handling =====

    @Test
    void kopInternalHeaderFormatIsHiddenFromLookup() {
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header-key\":\"base64\"}",
                "region", "US"));

        // The internal property must NOT surface to the selector engine, even though it's
        // present in the underlying map (we don't strip it from getProperties() because the
        // forward path may want to keep it for downstream KoP-aware tools).
        assertThat(b.lookup(MessageBindings.KOP_INTERNAL_HEADER_FORMAT)).isNull();

        // Other properties resolve normally.
        assertThat(b.lookup("region")).isEqualTo("US");

        // Preserved for forwarding.
        assertThat(b.getProperties()).containsKey(MessageBindings.KOP_INTERNAL_HEADER_FORMAT);
    }

    @Test
    void kopBase64EncodedBinaryHeaderSurfacesAsIs() {
        // KoP's encoding rule: binary header bytes [0x88] → property value "iA==" (base64).
        // We pass through unchanged; the selector author compares against the base64 string.
        MessageBindings b = new MessageBindings("k", "t", Map.of("binary-header", "iA=="));
        assertThat(b.lookup("binary-header")).isEqualTo("iA==");
    }

    @Test
    void kopUtf8HeaderSurfacesAsUtf8String() {
        // KoP's encoding rule: valid-UTF-8 header bytes → property value is the UTF-8 string.
        MessageBindings b = new MessageBindings("k", "t", Map.of("header-key", "header-value-2"));
        assertThat(b.lookup("header-key")).isEqualTo("header-value-2");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void fromRecordFallsBackWhenMessageHasNoKey() {
        Record record = Mockito.mock(Record.class);
        Message message = Mockito.mock(Message.class);
        Mockito.when(record.getMessage()).thenReturn(Optional.of(message));
        Mockito.when(message.hasKey()).thenReturn(false);
        Mockito.when(record.getKey()).thenReturn(Optional.empty());

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isNull();
    }

    // ===== KoP transparent base64 property decoding =====

    @Test
    void flaggedBase64PropertyWithValidUtf8DecodeSurfacesDecodedForm() {
        // The Kafka producer sent header bytes [0x6b,0x65,0x79] (= "key" UTF-8). KoP stored
        // the Pulsar property as "a2V5" (base64) for disambiguation. With the disambig property
        // flagging this header as base64-encoded, the function transparently surfaces "key".
        String base64 = Base64.getEncoder().encodeToString("key".getBytes(StandardCharsets.UTF_8));
        assertThat(base64).isEqualTo("a2V5");

        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header-key\":\"base64\"}",
                "header-key", base64));

        assertThat(b.lookup("header-key")).isEqualTo("key");
    }

    @Test
    void flaggedBase64PropertyWithBinaryDecodeFallsBackToRaw() {
        // A truly binary header. Bytes [0x88] are not valid UTF-8; base64 is "iA==".
        // Decoding succeeds, UTF-8 validation fails → return the raw base64 string so the
        // selector can compare against it.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"bin-header\":\"base64\"}",
                "bin-header", "iA=="));

        assertThat(b.lookup("bin-header")).isEqualTo("iA==");
    }

    @Test
    void unflaggedPropertyIsNotDecodedEvenIfItLooksLikeBase64() {
        // A property whose value happens to be a valid base64 string but is NOT flagged as
        // base64 by the disambig. The function leaves it alone — selector compares against
        // the literal string.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"other\":\"base64\"}",
                "header-key", "a2V5"));

        assertThat(b.lookup("header-key")).isEqualTo("a2V5");
    }

    @Test
    void decodedPropertyValueIsCachedAcrossLookups() {
        // Repeated lookups within the same MessageBindings must return the same String
        // reference, proving the decoded value is cached rather than re-allocated each call.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"h\":\"base64\"}",
                "h", "a2V5"));

        Object first  = b.lookup("h");
        Object second = b.lookup("h");
        Object third  = b.lookup("h");

        assertThat(first).isEqualTo("key");
        assertThat(second).isSameAs(first);
        assertThat(third).isSameAs(first);
    }

    @Test
    void rawFallbackIsAlsoCached() {
        // Binary-decode case: the raw base64 string is the resolved value. Multiple lookups
        // must return the same reference.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"bin\":\"base64\"}",
                "bin", "iA=="));

        Object first  = b.lookup("bin");
        Object second = b.lookup("bin");
        assertThat(first).isEqualTo("iA==");
        assertThat(second).isSameAs(first);
    }

    @Test
    void malformedDisambigJsonDisablesAutoDecode() {
        // If KoP's disambig property is corrupted, fall back to raw pass-through for safety.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{not-json",
                "header-key", "a2V5"));

        assertThat(b.lookup("header-key")).isEqualTo("a2V5");
    }

    @Test
    void unexpectedDisambigSchemaDisablesAutoDecode() {
        // An array (rather than the expected object) shape: no decoding triggered.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "[\"header-key\"]",
                "header-key", "a2V5"));

        assertThat(b.lookup("header-key")).isEqualTo("a2V5");
    }

    @Test
    void disambigWithNonBase64EncodingValueIsIgnored() {
        // Disambig that names a property with format "string" should not trigger base64 decode.
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT, "{\"header-key\":\"string\"}",
                "header-key", "a2V5"));

        assertThat(b.lookup("header-key")).isEqualTo("a2V5");
    }

    @Test
    void noDisambigPropertyMeansNoDecodeAttempted() {
        // Without __ksn_internal_header_format, a base64-looking value is passed through.
        MessageBindings b = new MessageBindings("k", "t", Map.of("header-key", "a2V5"));
        assertThat(b.lookup("header-key")).isEqualTo("a2V5");
    }

    @Test
    void multipleFlaggedPropertiesAreHandledIndependently() {
        MessageBindings b = new MessageBindings("k", "t", Map.of(
                MessageBindings.KOP_INTERNAL_HEADER_FORMAT,
                "{\"text-h\":\"base64\",\"bin-h\":\"base64\"}",
                "text-h", "a2V5",     // base64 of "key" — valid UTF-8 → decoded surfaces
                "bin-h", "iA=="));    // base64 of [0x88] — invalid UTF-8 → raw passes

        assertThat(b.lookup("text-h")).isEqualTo("key");
        assertThat(b.lookup("bin-h")).isEqualTo("iA==");
    }
}
