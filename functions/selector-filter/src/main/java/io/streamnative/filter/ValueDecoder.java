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

import java.util.Map;

/**
 * Decodes a message value (raw payload bytes) into its top-level fields so the selector can match
 * on them — e.g. {@code price > 50000} or {@code side = 'buy'} against a structured value.
 *
 * <p>Built once per query from the consumer's {@code ValueSchemaInfo} (see {@link ValueDecoderFactory})
 * and held on the {@link CompiledQuery}; {@link MessageBindings} invokes it lazily and caches the
 * result per message, so a payload is decoded at most once even across multiple matching queries.
 *
 * <p>Implementations:
 * <ul>
 *   <li>are <strong>thread-safe</strong> (the same decoder instance is shared across the function's
 *       concurrent {@code process()} invocations) and must not mutate the input array;</li>
 *   <li>return values coerced to {@link String} / {@link Number} / {@link Boolean} so JMS-selector
 *       comparison semantics apply; non-scalar fields (records, arrays, maps, bytes) resolve to
 *       {@code null} (only top-level scalar fields are addressable in this version);</li>
 *   <li>return {@code null} from {@link #decode(byte[])} when the payload cannot be decoded — the
 *       caller then falls back to best-effort JSON parsing.</li>
 * </ul>
 */
interface ValueDecoder {

    /**
     * Decode {@code payload} into a top-level field map (field name → scalar value), or {@code null}
     * if the payload is empty or cannot be decoded with this decoder's schema.
     */
    Map<String, Object> decode(byte[] payload);
}
