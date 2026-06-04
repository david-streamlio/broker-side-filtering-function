package io.streamnative.filter;

import io.streamnative.filter.common.ValueSchemaInfo;

/**
 * A loaded query: the compiled selector plus its fan-out output topic.
 *
 * <p>{@code valueSchema} is the consumer's value schema, forwarded by the orchestrator in the
 * {@code QueryMessage}. It is <strong>stored but not yet used</strong>: a follow-up wires it into a
 * value decoder so selectors can match on schema-aware value fields (see the decoder seam documented
 * in {@code MessageBindings}). {@code null} for the byte[] / Kafka paths.
 */
public record CompiledQuery(String queryId,
                            String name,
                            String outputTopic,
                            SelectorEvaluator evaluator,
                            ValueSchemaInfo valueSchema) {
}
