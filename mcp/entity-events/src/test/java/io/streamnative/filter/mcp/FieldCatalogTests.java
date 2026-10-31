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

package io.streamnative.filter.mcp;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FieldCatalogTests {

    private static final String TOPIC = "persistent://public/default/orders";

    private static FieldCatalog.Sample sample(String key, Map<String, String> properties, String json) {
        return new FieldCatalog.Sample(key, properties, json == null ? null : json.getBytes(StandardCharsets.UTF_8));
    }

    private static FieldCatalog.Field field(FieldCatalog.Fields fields, String name) {
        return fields.fields().stream().filter(f -> f.name().equals(name)).findFirst()
                .orElseThrow(() -> new AssertionError("no field " + name + " in " + fields.names()));
    }

    @Test
    void headersAreListedWithExamplesAndTheFiltersOwnPropertiesAreLeftOut() {
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
                sample("1", Map.of("event_type", "created", "__src_msgid__", "1:2:3"), null),
                sample("2", Map.of("event_type", "shipped", "region", "US"), null),
                sample("3", Map.of("event_type", "created"), null)));

        assertThat(fields.names()).containsExactly("__key__", "__publish_time__", "event_type", "region");
        FieldCatalog.Field eventType = field(fields, "event_type");
        assertThat(eventType.source()).isEqualTo("header");
        assertThat(eventType.examples()).containsExactly("created", "shipped");
        assertThat(eventType.describe()).containsEntry("cost", "read from message metadata, without decoding the value");
        assertThat(fields.sampled()).isEqualTo(3);
        assertThat(fields.bodyFieldsFrom()).isEqualTo("none");
    }

    @Test
    void jsonValueFieldsUseDottedPathsAndLeaveOutPaddingAndArrays() {
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
                sample("1", Map.of(), "{\"status\":\"PAID\",\"amount\":12.5,\"gift\":true,"
                        + "\"customer\":{\"id\":\"c-9\",\"tier\":\"GOLD\"},\"items\":[1,2],\"_pad\":\"xxxx\"}")));

        assertThat(fields.names()).contains("status", "amount", "gift", "customer.id", "customer.tier")
                .doesNotContain("items", "_pad", "customer");
        assertThat(field(fields, "amount").type()).isEqualTo("number");
        assertThat(field(fields, "gift").type()).isEqualTo("boolean");
        FieldCatalog.Field tier = field(fields, "customer.tier");
        assertThat(tier.source()).isEqualTo("body");
        assertThat(tier.examples()).containsExactly("GOLD");
        assertThat(tier.describe()).containsEntry("cost", "decodes the message value once per event");
        assertThat(fields.bodyFieldsFrom()).isEqualTo("sampled JSON values");
    }

    @Test
    void namesTheSelectorGrammarCantExpressAreNotOffered() {
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
                sample("1", Map.of("x-trace", "abc", "between", "1", "region", "US"), "{\"ship to\":\"x\",\"city\":\"Oslo\"}")));

        assertThat(fields.names()).contains("region", "city").doesNotContain("x-trace", "between", "ship to");
        assertThat(FieldCatalog.nameable("address.city")).isTrue();
        assertThat(FieldCatalog.nameable("AND")).isFalse();
        assertThat(FieldCatalog.nameable("1abc")).isFalse();
    }

    @Test
    void valuesInKafkaWireFormatOfferNoBodyFieldsBecauseTheFilterCantReadThem() {
        byte[] json = "{\"status\":\"PAID\"}".getBytes(StandardCharsets.UTF_8);
        byte[] framed = new byte[json.length + 5];
        framed[0] = 0;
        framed[4] = 7; // schema id 7
        System.arraycopy(json, 0, framed, 5, json.length);
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
                new FieldCatalog.Sample("1", Map.of("event_type", "paid"), framed)));

        assertThat(fields.names()).containsExactly("__key__", "__publish_time__", "event_type");
        assertThat(fields.bodyFieldsFrom()).contains("wire format").contains("key and headers");
    }

    @Test
    void theSamplesAreKeptWithTheFields() {
        List<FieldCatalog.Sample> samples = List.of(sample("1", Map.of("region", "US"), "{\"status\":\"PAID\"}"));
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), samples);

        assertThat(fields.samples()).isEqualTo(samples);
        assertThat(fields.field("region")).map(FieldCatalog.Field::source).contains("header");
        assertThat(new FieldCatalog.Fields(TOPIC, List.of(), 0, "none").samples()).isEmpty();
    }

    @Test
    void aHeaderShadowsAValueFieldOfTheSameName() {
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
                sample("1", Map.of("status", "from-header"), "{\"status\":\"from-body\"}")));

        assertThat(fields.fields().stream().filter(f -> f.name().equals("status")).toList())
                .singleElement()
                .satisfies(f -> {
                    assertThat(f.source()).isEqualTo("header");
                    assertThat(f.examples()).containsExactly("from-header");
                });
    }

    @Test
    void aRegisteredAvroRecordSchemaIsUsedOverSampledValues() {
        String avro = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                + "{\"name\":\"status\",\"type\":\"string\"},"
                + "{\"name\":\"amount\",\"type\":[\"null\",\"double\"]},"
                + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
                + "{\"name\":\"customer\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":["
                + "{\"name\":\"id\",\"type\":\"string\"}]}}]}";
        FieldCatalog.Fields fields = FieldCatalog.build(TOPIC,
                Optional.of(new FieldCatalog.RegisteredSchema("AVRO", avro)),
                List.of(sample("1", Map.of(), "{\"status\":\"PAID\",\"sampled_only\":\"x\"}")));

        assertThat(fields.names()).contains("status", "amount", "customer.id").doesNotContain("tags", "sampled_only");
        assertThat(field(fields, "amount").type()).isEqualTo("number");
        assertThat(fields.bodyFieldsFrom()).isEqualTo("registered AVRO schema");
    }

    @Test
    void fieldsAreCachedWithinTheTtlAndReadAgainAfterIt() {
        AtomicInteger reads = new AtomicInteger();
        FieldCatalog.Inspector inspector = new FieldCatalog.Inspector() {
            @Override
            public List<FieldCatalog.Sample> recent(String topic, int n) {
                reads.incrementAndGet();
                assertThat(n).isEqualTo(10);
                return List.of(sample("1", Map.of("event_type", "created"), null));
            }

            @Override
            public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
                return Optional.empty();
            }
        };
        MutableClock clock = new MutableClock();
        FieldCatalog catalog = new FieldCatalog(inspector, 10, Duration.ofMinutes(5), clock);

        assertThat(catalog.names(TOPIC)).contains("event_type");
        clock.advance(Duration.ofMinutes(4));
        catalog.fields(TOPIC);
        assertThat(reads).hasValue(1);
        clock.advance(Duration.ofMinutes(2));
        catalog.fields(TOPIC);
        assertThat(reads).hasValue(2);
    }

    @Test
    void anInspectorFailureIsAnIllegalStateWithTheTopic() {
        FieldCatalog.Inspector broken = new FieldCatalog.Inspector() {
            @Override
            public List<FieldCatalog.Sample> recent(String topic, int n) throws Exception {
                throw new java.io.IOException("connection refused");
            }

            @Override
            public Optional<FieldCatalog.RegisteredSchema> schema(String topic) {
                return Optional.empty();
            }
        };
        FieldCatalog catalog = new FieldCatalog(broken, 10, Duration.ofMinutes(5), Clock.systemUTC());

        assertThatThrownBy(() -> catalog.fields(TOPIC))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(TOPIC)
                .hasMessageContaining("connection refused");
    }

    /** A clock the test moves forward. */
    static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-10-31T12:00:00Z");

        void advance(Duration d) {
            now = now.plus(d);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
