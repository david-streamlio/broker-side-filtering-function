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

    // ---------------------------------------------------------------- validation (REGISTER)

    @Test
    void registerWithoutInputsOrPatternThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, null, null,
                "persistent://t/n/out", "x = 1", null, null, ClientType.PULSAR);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputTopics")
                .hasMessageContaining("topicsPattern");
    }

    @Test
    void registerWithBothInputsAndPatternThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER,
                List.of("persistent://t/n/a"), "persistent://t/n/.*",
                "persistent://t/n/out", "x = 1", null, null, ClientType.PULSAR);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void registerWithEmptyInputsListThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, List.of(), null,
                "persistent://t/n/out", "x = 1", null, null, ClientType.PULSAR);
        assertThatThrownBy(req::validate).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void registerWithBlankInputTopicThrows() {
        FilterRequest req = new FilterRequest(Command.REGISTER, List.of("   "), null,
                "persistent://t/n/out", "x = 1", null, null, ClientType.PULSAR);
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
                null, null, null, null);
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
                "K", "V", ClientType.KAFKA);
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
                "persistent://t/n/out", "x = 1", "K", "V", ClientType.PULSAR);
        req.validate(); // no throw — UNREGISTER tolerates everything
    }

    // ---------------------------------------------------------------- validation (command absent)

    @Test
    void nullCommandThrows() {
        FilterRequest req = new FilterRequest(null, null, null, null, null, null, null, null);
        assertThatThrownBy(req::validate)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("command");
    }
}
