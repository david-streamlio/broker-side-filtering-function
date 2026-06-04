package io.streamnative.filter;

import io.streamnative.filter.common.ValueSchemaInfo;

/**
 * A loaded query: the compiled selector plus its fan-out output topic.
 *
 * <p>{@code valueSchema} is the consumer's value schema, forwarded by the orchestrator in the
 * {@code QueryMessage} ({@code null} for the byte[] / Kafka paths). {@code valueDecoder} is the
 * decoder built from it once at load time (see {@link ValueDecoderFactory}); it lets the selector
 * match on top-level value fields (e.g. an Avro {@code price > 50000}). {@code null} when no
 * structured value decode applies — selectors then match on key, topic, properties, and best-effort
 * JSON, exactly as before.
 */
public record CompiledQuery(String queryId,
                            String name,
                            String outputTopic,
                            SelectorEvaluator evaluator,
                            ValueSchemaInfo valueSchema,
                            ValueDecoder valueDecoder) {
}
