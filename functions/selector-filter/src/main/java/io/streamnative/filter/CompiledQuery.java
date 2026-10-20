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
 *
 * <p>{@code window} is set for a windowed query (its {@code QueryMessage} carried a window) and
 * {@code null} for a live query.
 */
public record CompiledQuery(String queryId,
                            String name,
                            String outputTopic,
                            SelectorEvaluator evaluator,
                            ValueSchemaInfo valueSchema,
                            ValueDecoder valueDecoder,
                            WindowState window) {

    /** A live (non-windowed) query. */
    public CompiledQuery(String queryId, String name, String outputTopic, SelectorEvaluator evaluator,
                         ValueSchemaInfo valueSchema, ValueDecoder valueDecoder) {
        this(queryId, name, outputTopic, evaluator, valueSchema, valueDecoder, null);
    }
}
