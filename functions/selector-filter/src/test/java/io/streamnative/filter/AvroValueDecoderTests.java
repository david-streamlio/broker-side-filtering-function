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

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates {@link AvroValueDecoder} against bytes produced by Pulsar's own {@code Schema.AVRO}
 * encoder — i.e. the exact wire format the filter receives. The encoder uses Pulsar's shaded Avro
 * while the decoder uses the unshaded bundled Avro; this test confirms they interoperate.
 */
class AvroValueDecoderTests {

    /** POJO mapped by Pulsar's reflective Avro schema. Public fields keep the .avsc simple. */
    public static class Trade {
        public String sym;
        public double price;
        public int qty;
        public boolean active;
    }

    public static class Address {
        public String city;
        public int zip;
    }

    public static class Order {
        public String sym;
        public Address address;
    }

    private static AvroValueDecoder decoderFor(Schema<?> schema) {
        String avsc = new String(schema.getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
        return new AvroValueDecoder(avsc);
    }

    @Test
    void decodesPulsarAvroEncodedRecordToTopLevelScalars() {
        Schema<Trade> schema = Schema.AVRO(Trade.class);
        Trade t = new Trade();
        t.sym = "AAPL";
        t.price = 195.5;
        t.qty = 10;
        t.active = true;

        Map<String, Object> fields = decoderFor(schema).decode(schema.encode(t));

        assertThat(fields).isNotNull();
        assertThat(fields.get("sym")).isEqualTo("AAPL");
        assertThat(((Number) fields.get("price")).doubleValue()).isEqualTo(195.5);
        assertThat(((Number) fields.get("qty")).intValue()).isEqualTo(10);
        assertThat(fields.get("active")).isEqualTo(true);
    }

    @Test
    void decodesNestedRecordToNestedMap() {
        Schema<Order> schema = Schema.AVRO(Order.class);
        Order o = new Order();
        o.sym = "AAPL";
        Address a = new Address();
        a.city = "NYC";
        a.zip = 10001;
        o.address = a;

        Map<String, Object> fields = decoderFor(schema).decode(schema.encode(o));

        assertThat(fields.get("sym")).isEqualTo("AAPL");
        assertThat(fields.get("address")).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> addr = (Map<String, Object>) fields.get("address");
        assertThat(addr.get("city")).isEqualTo("NYC");
        assertThat(((Number) addr.get("zip")).intValue()).isEqualTo(10001);
    }

    @Test
    void returnsNullForEmptyOrUndecodablePayload() {
        AvroValueDecoder decoder = decoderFor(Schema.AVRO(Trade.class));
        assertThat(decoder.decode(null)).isNull();
        assertThat(decoder.decode(new byte[0])).isNull();
        assertThat(decoder.decode("not avro at all".getBytes(StandardCharsets.UTF_8))).isNull();
    }

    @Test
    void rejectsNonRecordSchema() {
        assertThatThrownBy(() -> new AvroValueDecoder("\"string\""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("record");
    }

    @Test
    void rejectsMalformedSchema() {
        assertThatThrownBy(() -> new AvroValueDecoder("{ not valid avsc"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
