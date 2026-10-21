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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterRequestTests {

    /**
     * Standard mapper for tests; matches the orchestrator-side configuration we'll use in
     * production (tolerate unknown fields for forward-compat, see R10 in the plan).
     */
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    // ---------------------------------------------------------------- serde

    @Test
    void registerSerdeRoundTripsSingleInputTopic() throws Exception {
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/filtered/filtered-1234",
                "region = 'US'",
                "org.apache.kafka.common.serialization.StringDeserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer",
                ClientType.KAFKA);

        byte[] payload = MAPPER.writeValueAsBytes(req);
        FilterRequest parsed = MAPPER.readValue(payload, FilterRequest.class);

        assertThat(parsed).isEqualTo(req);
        assertThat(parsed.getCommand()).isEqualTo(Command.REGISTER);
        assertThat(parsed.getInputTopics()).containsExactly("persistent://public/in/trades");
        assertThat(parsed.getTopicsPattern()).isNull();
        assertThat(parsed.getClientType()).isEqualTo(ClientType.KAFKA);
    }

    @Test
    void registerSerdeRoundTripsTopicsPattern() throws Exception {
        FilterRequest req = FilterRequest.registerPattern(
                "persistent://public/trades/.*",
                "persistent://public/filtered/filtered-abcd",
                "__key__ IN ('AAPL','MSFT')",
                null, null, ClientType.PULSAR);

        byte[] payload = MAPPER.writeValueAsBytes(req);
        FilterRequest parsed = MAPPER.readValue(payload, FilterRequest.class);

        assertThat(parsed.getInputTopics()).isNull();
        assertThat(parsed.getTopicsPattern()).isEqualTo("persistent://public/trades/.*");
        assertThat(parsed.getKeyDeserializer()).isNull();
        assertThat(parsed.getValueDeserializer()).isNull();
        assertThat(parsed.getClientType()).isEqualTo(ClientType.PULSAR);
    }

    @Test
    void modifySerdeRoundTripsMinimalFields() throws Exception {
        FilterRequest req = FilterRequest.modify("region IN ('US','UK')");
        byte[] payload = MAPPER.writeValueAsBytes(req);
        FilterRequest parsed = MAPPER.readValue(payload, FilterRequest.class);

        assertThat(parsed.getCommand()).isEqualTo(Command.MODIFY);
        assertThat(parsed.getSelector()).isEqualTo("region IN ('US','UK')");
        assertThat(parsed.getInputTopics()).isNull();
        assertThat(parsed.getOutputTopic()).isNull();
        assertThat(parsed.getClientType()).isNull();
    }

    @Test
    void unregisterSerdeRoundTripsCommandOnly() throws Exception {
        FilterRequest req = FilterRequest.unregister();
        byte[] payload = MAPPER.writeValueAsBytes(req);

        // Serialized form should be minimal — NON_NULL inclusion strips everything but command.
        String json = new String(payload);
        assertThat(json).isEqualTo("{\"command\":\"UNREGISTER\"}");

        FilterRequest parsed = MAPPER.readValue(payload, FilterRequest.class);
        assertThat(parsed.getCommand()).isEqualTo(Command.UNREGISTER);
    }

    @Test
    void serdeOmitsNullFieldsFromOutput() throws Exception {
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/x",
                "persistent://public/filtered/filtered-1",
                "k = 'v'",
                null, null, ClientType.PULSAR);

        String json = MAPPER.writeValueAsString(req);
        assertThat(json).doesNotContain("topicsPattern")
                        .doesNotContain("keyDeserializer")
                        .doesNotContain("valueDeserializer")
                        .contains("\"command\":\"REGISTER\"")
                        .contains("\"clientType\":\"PULSAR\"");
    }

    @Test
    void unknownFieldsAreTolerated() throws Exception {
        // Forward-compat — orchestrator must accept FilterRequests with newer-version fields.
        String json = """
                {
                  "command": "UNREGISTER",
                  "someFutureField": "ignored",
                  "anotherNewField": 42
                }
                """;
        FilterRequest parsed = MAPPER.readValue(json, FilterRequest.class);
        assertThat(parsed.getCommand()).isEqualTo(Command.UNREGISTER);
    }

    @Test
    void registerSerdeRoundTripsValueSchema() throws Exception {
        ValueSchemaInfo schema = new ValueSchemaInfo(
                "com.acme.Trade", "AVRO",
                "eyJ0eXBlIjoicmVjb3JkIn0=",  // base64 of a stand-in .avsc
                java.util.Map.of("__alwaysAllowNull", "true"));
        FilterRequest req = FilterRequest.register(
                List.of("persistent://public/in/trades"),
                "persistent://public/filtered/filtered-9",
                "price > 50000",
                null, "com.acme.Trade", schema, ClientType.PULSAR);

        byte[] payload = MAPPER.writeValueAsBytes(req);
        FilterRequest parsed = MAPPER.readValue(payload, FilterRequest.class);

        assertThat(parsed).isEqualTo(req);
        assertThat(parsed.getValueSchema()).isEqualTo(schema);
        assertThat(parsed.getValueSchema().getType()).isEqualTo("AVRO");
        assertThat(parsed.getValueSchema().getDefinition()).isEqualTo("eyJ0eXBlIjoicmVjb3JkIn0=");
        assertThat(parsed.getValueDeserializer()).isEqualTo("com.acme.Trade");
    }

    @Test
    void registerWithoutValueSchemaOmitsItFromWire() throws Exception {
        // Back-compat: a register with no value schema must be wire-identical to the pre-schema
        // format — the valueSchema key must not appear at all.
        FilterRequest req = FilterRequest.register(
                "persistent://public/in/x",
                "persistent://public/filtered/filtered-1",
                "k = 'v'",
                null, null, ClientType.PULSAR);

        assertThat(req.getValueSchema()).isNull();
        assertThat(MAPPER.writeValueAsString(req)).doesNotContain("valueSchema");
    }

    // ---------------------------------------------------------------- validation (REGISTER)

    @Test
    void registerWithoutInputsOrPatternThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, null, null,
                "persistent://t/n/out", "x = 1", null, null, null, ClientType.PULSAR, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputTopics")
                .hasMessageContaining("topicsPattern");
    }

    @Test
    void registerWithBothInputsAndPatternThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER,
                List.of("persistent://t/n/a"), "persistent://t/n/.*",
                "persistent://t/n/out", "x = 1", null, null, null, ClientType.PULSAR, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void registerWithEmptyInputsListThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, List.of(), null,
                "persistent://t/n/out", "x = 1", null, null, null, ClientType.PULSAR, null);
        assertThatThrownBy(req::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registerWithBlankInputTopicThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, List.of("   "), null,
                "persistent://t/n/out", "x = 1", null, null, null, ClientType.PULSAR, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void registerWithoutOutputTopicThrows() {
        FilterRequest req = FilterRequest.register(
                "persistent://t/n/in", null, "x = 1", null, null, ClientType.PULSAR);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("outputTopic");
    }

    @Test
    void registerWithNullSelectorIsAllowed() {
        // null selector = "no user filter; match every record on the input topic". The orchestrator
        // is responsible for translating to a JMS-selector identity downstream.
        FilterRequest req = FilterRequest.register(
                "persistent://t/n/in", "persistent://t/n/out", null,
                null, null, ClientType.PULSAR);
        req.validate(); // no throw
    }

    @Test
    void registerWithBlankSelectorIsAllowed() {
        FilterRequest req = FilterRequest.register(
                "persistent://t/n/in", "persistent://t/n/out", "   ",
                null, null, ClientType.PULSAR);
        req.validate(); // no throw
    }

    @Test
    void registerWithoutClientTypeThrows() {
        FilterRequest req = FilterRequest.register(
                "persistent://t/n/in", "persistent://t/n/out", "x = 1",
                null, null, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clientType");
    }

    @Test
    void registerHappyPathPasses() {
        FilterRequest req = FilterRequest.register(
                "persistent://t/n/in", "persistent://t/n/out", "x = 1",
                null, null, ClientType.PULSAR);
        req.validate(); // no throw
    }

    @Test
    void registerPatternHappyPathPasses() {
        FilterRequest req = FilterRequest.registerPattern(
                "persistent://t/n/.*", "persistent://t/n/out", "x = 1",
                "K", "V", ClientType.KAFKA);
        req.validate(); // no throw
    }

    // ---------------------------------------------------------------- validation (MODIFY)

    @Test
    void modifyWithoutSelectorThrows() {
        FilterRequest req = new FilterRequest(Command.MODIFY, null, null, null,
                null, null, null, null, null, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selector");
    }

    @Test
    void modifyWithBlankSelectorThrows() {
        FilterRequest req = FilterRequest.modify("   ");
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("selector");
    }

    @Test
    void modifyHappyPathPasses() {
        FilterRequest.modify("x = 2").validate();
    }

    @Test
    void modifyIgnoresExtraneousFieldsAtValidate() {
        // Per the schema, MODIFY tolerates extra fields silently. The orchestrator may choose to
        // log or strip them; validation does not throw.
        FilterRequest req = new FilterRequest(Command.MODIFY,
                List.of("persistent://t/n/in"),  // extraneous on MODIFY
                null,
                "persistent://t/n/out",          // extraneous on MODIFY
                "x = 1",
                "K", "V", null, ClientType.KAFKA, null);
        req.validate(); // no throw
    }

    // ---------------------------------------------------------------- validation (UNREGISTER)

    @Test
    void unregisterHappyPathPasses() {
        FilterRequest.unregister().validate();
    }

    @Test
    void unregisterIgnoresExtraneousFields() {
        FilterRequest req = new FilterRequest(Command.UNREGISTER,
                List.of("persistent://t/n/x"), null,
                "persistent://t/n/out", "x = 1", "K", "V", null, ClientType.PULSAR, null);
        req.validate(); // no throw — UNREGISTER tolerates everything
    }

    // ---------------------------------------------------------------- validation (command absent)

    @Test
    void nullCommandThrows() {
        FilterRequest req = new FilterRequest(null, null, null, null, null, null, null, null, null, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("command");
    }

    // ---------------------------------------------------------------- validation (HEARTBEAT)

    @Test
    void heartbeatHappyPathPasses() {
        FilterRequest.heartbeat(List.of("uuid-1", "uuid-2")).validate();
    }

    @Test
    void heartbeatWithNullQueryIdsThrows() {
        FilterRequest req = new FilterRequest(Command.HEARTBEAT, null, null, null, null,
                null, null, null, null, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryIds");
    }

    @Test
    void heartbeatWithEmptyQueryIdsThrows() {
        FilterRequest req = FilterRequest.heartbeat(List.of());
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("queryIds");
    }

    @Test
    void heartbeatWithBlankQueryIdThrows() {
        FilterRequest req = FilterRequest.heartbeat(List.of("uuid-1", "   "));
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("blank");
    }

    @Test
    void heartbeatSerdeRoundTrip() throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        FilterRequest req = FilterRequest.heartbeat(List.of("a", "b", "c"));
        byte[] bytes = mapper.writeValueAsBytes(req);
        FilterRequest parsed = mapper.readValue(bytes, FilterRequest.class);
        assertThat(parsed.getCommand()).isEqualTo(Command.HEARTBEAT);
        assertThat(parsed.getQueryIds()).containsExactly("a", "b", "c");
        assertThat(parsed).isEqualTo(req);
    }

    @Test
    void heartbeatSerdeOmitsAbsentFields() throws Exception {
        com.fasterxml.jackson.databind.ObjectMapper mapper =
                new com.fasterxml.jackson.databind.ObjectMapper();
        String json = mapper.writeValueAsString(FilterRequest.heartbeat(List.of("a")));
        // HEARTBEAT does not carry input/output/selector fields; the @JsonInclude(NON_NULL) on
        // FilterRequest must drop them.
        assertThat(json).doesNotContain("inputTopics")
                        .doesNotContain("topicsPattern")
                        .doesNotContain("outputTopic")
                        .doesNotContain("selector")
                        .doesNotContain("keyDeserializer")
                        .doesNotContain("valueDeserializer")
                        .doesNotContain("clientType");
    }

    // ---------------------------------------------------------------- windowed REGISTER

    private static FilterRequest windowed(long from, long to) {
        return FilterRequest.registerWindowed("persistent://public/default/orders",
                "persistent://public/filtered/filtered-w1", "__key__ = '8675309'",
                null, null, null, ClientType.PULSAR, from, to);
    }

    @Test
    void windowedRegisterRoundTrips() throws Exception {
        FilterRequest req = windowed(1_000L, 2_000L);
        req.validate();

        FilterRequest parsed = MAPPER.readValue(MAPPER.writeValueAsBytes(req), FilterRequest.class);

        assertThat(parsed).isEqualTo(req);
        assertThat(parsed.isWindowed()).isTrue();
        assertThat(parsed.getWindowFromMs()).isEqualTo(1_000L);
        assertThat(parsed.getWindowToMs()).isEqualTo(2_000L);
    }

    @Test
    void liveRegisterHasNoWindowOnTheWire() throws Exception {
        FilterRequest req = FilterRequest.register("persistent://public/in/trades",
                "persistent://public/filtered/x", "region = 'US'", null, null, ClientType.PULSAR);
        String json = MAPPER.writeValueAsString(req);
        assertThat(json).doesNotContain("window");
        assertThat(MAPPER.readValue(json, FilterRequest.class).isWindowed()).isFalse();
    }

    @Test
    void windowedRegisterRejectsAnEmptyOrInvertedWindow() {
        assertThatThrownBy(() -> windowed(2_000L, 2_000L).validate())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("windowFromMs < windowToMs");
        assertThatThrownBy(() -> windowed(-1L, 2_000L).validate())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void windowedRegisterRequiresBothBounds() {
        FilterRequest req = new FilterRequest(Command.REGISTER, List.of("persistent://public/default/orders"), null,
                "persistent://public/filtered/x", null, null, null, null, ClientType.PULSAR, null, 1_000L, null);
        assertThatThrownBy(req::validate).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both windowFromMs and windowToMs");
    }

    @Test
    void windowedRegisterRequiresExactlyOneInputTopic() {
        FilterRequest twoTopics = new FilterRequest(Command.REGISTER,
                List.of("persistent://public/default/a", "persistent://public/default/b"), null,
                "persistent://public/filtered/x", null, null, null, null, ClientType.PULSAR, null, 1L, 2L);
        assertThatThrownBy(twoTopics::validate).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one input topic");
        FilterRequest pattern = new FilterRequest(Command.REGISTER, null, "persistent://public/default/.*",
                "persistent://public/filtered/x", null, null, null, null, ClientType.PULSAR, null, 1L, 2L);
        assertThatThrownBy(pattern::validate).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one input topic");
    }
}
