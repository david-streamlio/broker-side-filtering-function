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

package io.streamnative.filter.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class QueryMessageTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void roundTripsWithValueSchema() throws Exception {
        ValueSchemaInfo schema = new ValueSchemaInfo(
                "com.acme.Trade", "AVRO", "eyJ0eXBlIjoicmVjb3JkIn0=", Map.of("k", "v"));
        QueryMessage qm = new QueryMessage(
                "session-1", "price > 50000", "persistent://public/filtered/out", schema);

        QueryMessage parsed = MAPPER.readValue(MAPPER.writeValueAsBytes(qm), QueryMessage.class);

        assertThat(parsed).isEqualTo(qm);
        assertThat(parsed.getValueSchema()).isEqualTo(schema);
        assertThat(parsed.getSelector()).isEqualTo("price > 50000");
    }

    @Test
    void roundTripsWithoutValueSchemaAndOmitsItFromWire() throws Exception {
        QueryMessage qm = new QueryMessage(
                "session-1", "region = 'US'", "persistent://public/filtered/out", null);

        String json = MAPPER.writeValueAsString(qm);
        assertThat(json).doesNotContain("valueSchema");

        QueryMessage parsed = MAPPER.readValue(json, QueryMessage.class);
        assertThat(parsed).isEqualTo(qm);
        assertThat(parsed.getValueSchema()).isNull();
    }

    @Test
    void toleratesUnknownFieldsForForwardCompat() throws Exception {
        String json = """
                {"name":"s","selector":"x = 1","outputTopic":"persistent://t/n/o","future":true}
                """;
        QueryMessage parsed = MAPPER.readValue(json, QueryMessage.class);
        assertThat(parsed.getSelector()).isEqualTo("x = 1");
        assertThat(parsed.getValueSchema()).isNull();
    }

    @Test
    void roundTripsWithWindow() throws Exception {
        WindowSpec window = new WindowSpec(1_000L, 2_000L, 17L, 42L, true);
        QueryMessage qm = new QueryMessage(
                "session-1", "__key__ = '8675309'", "persistent://public/filtered/out", null, window);

        QueryMessage parsed = MAPPER.readValue(MAPPER.writeValueAsBytes(qm), QueryMessage.class);

        assertThat(parsed).isEqualTo(qm);
        assertThat(parsed.getWindow()).isEqualTo(window);
        assertThat(parsed.getWindow().hasEndPosition()).isTrue();
        assertThat(parsed.getWindow().isTruncatedByRetention()).isTrue();
    }

    @Test
    void liveQueryHasNoWindowOnTheWire() throws Exception {
        QueryMessage qm = new QueryMessage("s", "x = 1", "persistent://t/n/o", null);
        assertThat(MAPPER.writeValueAsString(qm)).doesNotContain("window");
        assertThat(MAPPER.readValue(MAPPER.writeValueAsBytes(qm), QueryMessage.class).getWindow()).isNull();
    }

    @Test
    void windowWithoutEndPosition() throws Exception {
        String json = """
                {"name":"s","selector":"x = 1","outputTopic":"persistent://t/n/o",
                 "window":{"fromMs":10,"toMs":20}}
                """;
        WindowSpec window = MAPPER.readValue(json, QueryMessage.class).getWindow();
        assertThat(window.getFromMs()).isEqualTo(10L);
        assertThat(window.getToMs()).isEqualTo(20L);
        assertThat(window.hasEndPosition()).isFalse();
        assertThat(window.isTruncatedByRetention()).isFalse();
    }
}
