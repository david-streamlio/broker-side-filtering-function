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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class ValueDecoderFactoryTests {

    private static ValueSchemaInfo info(String name, String type, String definition) {
        return new ValueSchemaInfo(name, type, definition, null);
    }

    @Test
    void nullWhenNoSchema() {
        assertThat(ValueDecoderFactory.from(null)).isNull();
    }

    @Test
    void nullForJsonSchema() {
        // JSON values are served by MessageBindings' best-effort JSON path, not a dedicated decoder.
        assertThat(ValueDecoderFactory.from(info("X", "JSON", "abc"))).isNull();
    }

    @Test
    void nullForStringAndBytes() {
        assertThat(ValueDecoderFactory.from(info("STRING", "STRING", null))).isNull();
        assertThat(ValueDecoderFactory.from(info("BYTES", "BYTES", null))).isNull();
    }

    @Test
    void buildsAvroDecoderForValidSchema() {
        Schema<AvroValueDecoderTests.Trade> s = Schema.AVRO(AvroValueDecoderTests.Trade.class);
        String def = Base64.getEncoder().encodeToString(s.getSchemaInfo().getSchema());
        assertThat(ValueDecoderFactory.from(info("Trade", "AVRO", def)))
                .isInstanceOf(AvroValueDecoder.class);
    }

    @Test
    void nullForAvroWithMissingDefinition() {
        assertThat(ValueDecoderFactory.from(info("X", "AVRO", null))).isNull();
        assertThat(ValueDecoderFactory.from(info("X", "AVRO", "  "))).isNull();
    }

    @Test
    void nullForAvroWithMalformedDefinition() {
        String bad = Base64.getEncoder().encodeToString("{ not avsc".getBytes());
        assertThat(ValueDecoderFactory.from(info("X", "AVRO", bad))).isNull();
    }

    @Test
    void buildsProtobufNativeDecoderForValidSchema() throws Exception {
        byte[] schemaData = ProtobufNativeSchemaUtils.serialize(
                ProtobufNativeValueDecoderTests.tradeDescriptor());
        String def = Base64.getEncoder().encodeToString(schemaData);
        assertThat(ValueDecoderFactory.from(info("Trade", "PROTOBUF_NATIVE", def)))
                .isInstanceOf(ProtobufNativeValueDecoder.class);
    }

    @Test
    void nullForProtobufNativeWithMissingOrMalformedDefinition() {
        assertThat(ValueDecoderFactory.from(info("X", "PROTOBUF_NATIVE", null))).isNull();
        String bad = Base64.getEncoder().encodeToString("{ not json".getBytes());
        assertThat(ValueDecoderFactory.from(info("X", "PROTOBUF_NATIVE", bad))).isNull();
    }
}
