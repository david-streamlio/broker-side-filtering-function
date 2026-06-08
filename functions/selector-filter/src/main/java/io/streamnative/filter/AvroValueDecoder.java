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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericEnumSymbol;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Decodes Pulsar {@code Schema.AVRO} message values. The Avro {@code .avsc} (carried in the query's
 * {@code ValueSchemaInfo.definition}) is the reader schema; since the filter forwards the producer's
 * bytes verbatim, the reader schema equals the writer schema and the binary payload decodes directly.
 *
 * <p>Only top-level scalar fields are surfaced (the JMS selector grammar can't address nested paths).
 * The unshaded {@code org.apache.avro} classes used here are bundled in the filter NAR; they decode
 * the same binary format Pulsar's shaded Avro encoder produces on the wire.
 */
final class AvroValueDecoder implements ValueDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(AvroValueDecoder.class);

    private final Schema schema;
    // GenericDatumReader is thread-safe for read() when each call supplies its own decoder and a
    // null reuse object, which is exactly how decode() invokes it.
    private final GenericDatumReader<GenericRecord> reader;

    /**
     * @param avsc the Avro schema JSON text (a record schema)
     * @throws IllegalArgumentException if the schema is not a record or fails to parse
     */
    AvroValueDecoder(String avsc) {
        Schema parsed;
        try {
            parsed = new Schema.Parser().parse(avsc);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Avro schema: " + e.getMessage(), e);
        }
        if (parsed.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException(
                    "Avro value schema must be a record; got " + parsed.getType());
        }
        this.schema = parsed;
        this.reader = new GenericDatumReader<>(parsed);
    }

    @Override
    public Map<String, Object> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return recordToMap(reader.read(null, decoder));
        } catch (Exception e) {
            // Not the expected Avro layout (corrupt/foreign bytes) — let the caller fall back.
            LOG.debug("Avro decode failed for schema {}: {}", schema.getFullName(), e.toString());
            return null;
        }
    }

    /** Convert a record to a field map, recursing into nested records (for dotted-path lookups). */
    private static Map<String, Object> recordToMap(GenericRecord record) {
        Map<String, Object> fields = new HashMap<>();
        for (Schema.Field f : record.getSchema().getFields()) {
            fields.put(f.name(), coerce(record.get(f.pos())));
        }
        return fields;
    }

    /**
     * Coerce an Avro field value: scalars to {@link String} / {@link Number} / {@link Boolean}, a
     * nested record to a nested {@code Map} (so {@code a.b} dotted paths resolve). Arrays, maps,
     * fixed, bytes, and {@code null} resolve to {@code null}.
     */
    private static Object coerce(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof CharSequence) {           // Avro Utf8 → String
            return value.toString();
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof GenericEnumSymbol<?>) {
            return value.toString();
        }
        if (value instanceof GenericRecord nested) {
            return recordToMap(nested);
        }
        return null;
    }
}
