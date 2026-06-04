package io.streamnative.filter;

import io.streamnative.filter.common.ValueSchemaInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Builds a {@link ValueDecoder} from a query's {@link ValueSchemaInfo}, invoked once per query when
 * the {@link CompiledQuery} is loaded (never on the per-message hot path).
 *
 * <p>Decoder coverage:
 * <ul>
 *   <li><strong>AVRO</strong> → {@link AvroValueDecoder}.</li>
 *   <li><strong>PROTOBUF_NATIVE</strong> → {@link ProtobufNativeValueDecoder}.</li>
 *   <li><strong>JSON</strong> → {@code null}; JSON values are already resolved by
 *       {@link MessageBindings}' best-effort JSON path, so no dedicated decoder is needed.</li>
 *   <li><strong>STRING / BYTES / other / absent</strong> → {@code null}; no structured value
 *       decode. Selectors still match on key, topic, and user properties.</li>
 * </ul>
 *
 * <p>A schema that names a decoder but can't be built (e.g. a malformed Avro {@code .avsc}) yields
 * {@code null} with a warning rather than failing the query — value-field selectors then simply
 * won't match, while key/topic/property selectors keep working.
 */
final class ValueDecoderFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ValueDecoderFactory.class);

    private ValueDecoderFactory() {
    }

    /** Returns a decoder for {@code schema}, or {@code null} when no structured decode applies. */
    static ValueDecoder from(ValueSchemaInfo schema) {
        if (schema == null || schema.getType() == null) {
            return null;
        }
        switch (schema.getType()) {
            case "AVRO":
                return buildAvro(schema);
            case "PROTOBUF_NATIVE":
                return buildProtobufNative(schema);
            default:
                // JSON handled by the best-effort JSON path; STRING/BYTES/others unsupported.
                return null;
        }
    }

    private static ValueDecoder buildAvro(ValueSchemaInfo schema) {
        String definition = schema.getDefinition();
        if (definition == null || definition.isBlank()) {
            LOG.warn("AVRO value schema '{}' carries no definition; value-field decoding disabled",
                    schema.getName());
            return null;
        }
        try {
            String avsc = new String(Base64.getDecoder().decode(definition), StandardCharsets.UTF_8);
            return new AvroValueDecoder(avsc);
        } catch (RuntimeException e) {
            LOG.warn("Failed to build Avro decoder for value schema '{}': {}",
                    schema.getName(), e.toString());
            return null;
        }
    }

    private static ValueDecoder buildProtobufNative(ValueSchemaInfo schema) {
        String definition = schema.getDefinition();
        if (definition == null || definition.isBlank()) {
            LOG.warn("PROTOBUF_NATIVE value schema '{}' carries no definition; value-field decoding disabled",
                    schema.getName());
            return null;
        }
        try {
            byte[] schemaData = Base64.getDecoder().decode(definition);
            return new ProtobufNativeValueDecoder(schemaData);
        } catch (RuntimeException e) {
            LOG.warn("Failed to build Protobuf-native decoder for value schema '{}': {}",
                    schema.getName(), e.toString());
            return null;
        }
    }
}
