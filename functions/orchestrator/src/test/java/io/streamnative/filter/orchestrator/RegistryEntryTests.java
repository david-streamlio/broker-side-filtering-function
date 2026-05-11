package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RegistryEntryTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static FilterRequest reqWithSerializers(String keyDeser, String valueDeser) {
        return FilterRequest.register(
                "persistent://public/in/trades",
                "persistent://public/out/x",
                "region = 'US'",
                keyDeser, valueDeser, ClientType.KAFKA);
    }

    private static RegistryEntry entryWithSerializers(String keyDeser, String valueDeser) {
        return new RegistryEntry(
                "persistent://public/orchestrator/ctrl-trades-abc",
                "selector-filter-trades-abc",
                keyDeser, valueDeser, ClientType.KAFKA,
                "2026-05-11T00:00:00Z");
    }

    // ---------------------------------------------------------------- Compatibility matrix

    @Test
    void nullNullIsCompatible() {
        RegistryEntry entry = entryWithSerializers(null, null);
        FilterRequest req   = reqWithSerializers(null, null);
        assertThat(entry.check(req).isCompatible()).isTrue();
    }

    @Test
    void nullThenXTriggersUpgrade() {
        RegistryEntry entry = entryWithSerializers(null, null);
        FilterRequest req   = reqWithSerializers("StringDeser", "ByteDeser");
        assertThat(entry.check(req).needsUpgrade()).isTrue();
        RegistryEntry merged = entry.mergedWith(req);
        assertThat(merged.getKeyDeserializer()).isEqualTo("StringDeser");
        assertThat(merged.getValueDeserializer()).isEqualTo("ByteDeser");
    }

    @Test
    void xThenNullIsCompatibleNoChange() {
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser");
        FilterRequest req   = reqWithSerializers(null, null);
        assertThat(entry.check(req).isCompatible()).isTrue();
    }

    @Test
    void xThenXIsCompatible() {
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser");
        FilterRequest req   = reqWithSerializers("StringDeser", "ByteDeser");
        assertThat(entry.check(req).isCompatible()).isTrue();
    }

    @Test
    void xThenYIsRejected() {
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser");
        FilterRequest req   = reqWithSerializers("IntegerDeser", "ByteDeser");
        RegistryEntry.CompatibilityResult result = entry.check(req);
        assertThat(result.isRejected()).isTrue();
        assertThat(result.reason()).contains("keyDeserializer mismatch");
    }

    @Test
    void valueDeserializerMismatchIsRejected() {
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser");
        FilterRequest req   = reqWithSerializers("StringDeser", "ProtobufDeser");
        RegistryEntry.CompatibilityResult result = entry.check(req);
        assertThat(result.isRejected()).isTrue();
        assertThat(result.reason()).contains("valueDeserializer mismatch");
    }

    @Test
    void partialUpgradeMergesOnlyNullFields() {
        RegistryEntry entry = entryWithSerializers(null, "ByteDeser");
        FilterRequest req   = reqWithSerializers("StringDeser", "ByteDeser");
        assertThat(entry.check(req).needsUpgrade()).isTrue();

        RegistryEntry merged = entry.mergedWith(req);
        assertThat(merged.getKeyDeserializer()).isEqualTo("StringDeser");
        assertThat(merged.getValueDeserializer()).isEqualTo("ByteDeser");
        assertThat(merged.getControlTopic()).isEqualTo(entry.getControlTopic()); // unchanged
        assertThat(merged.getFilterFunctionName()).isEqualTo(entry.getFilterFunctionName()); // unchanged
        assertThat(merged.getCreatedAt()).isEqualTo(entry.getCreatedAt()); // unchanged
    }

    // ---------------------------------------------------------------- Serde

    @Test
    void serdeRoundTrip() throws Exception {
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser");
        byte[] payload = MAPPER.writeValueAsBytes(entry);
        RegistryEntry parsed = MAPPER.readValue(payload, RegistryEntry.class);
        assertThat(parsed).isEqualTo(entry);
    }

    @Test
    void serdeOmitsNullFields() throws Exception {
        RegistryEntry entry = entryWithSerializers(null, null);
        String json = MAPPER.writeValueAsString(entry);
        assertThat(json).doesNotContain("keyDeserializer")
                        .doesNotContain("valueDeserializer");
    }

    @Test
    void unknownFieldsTolerated() throws Exception {
        String json = """
                {
                  "controlTopic": "persistent://t/n/ctrl",
                  "filterFunctionName": "fn-1",
                  "createdAt": "2026-05-11T00:00:00Z",
                  "someFutureField": "ignored"
                }
                """;
        RegistryEntry parsed = MAPPER.readValue(json, RegistryEntry.class);
        assertThat(parsed.getFilterFunctionName()).isEqualTo("fn-1");
    }

    // ---------------------------------------------------------------- newEntry factory

    @Test
    void newEntryRecordsRequestFields() {
        FilterRequest req = reqWithSerializers("StringDeser", "ByteDeser");
        RegistryEntry entry = RegistryEntry.newEntry(
                "persistent://public/orchestrator/ctrl-trades-abc",
                "selector-filter-trades-abc",
                req);
        assertThat(entry.getControlTopic()).isEqualTo("persistent://public/orchestrator/ctrl-trades-abc");
        assertThat(entry.getFilterFunctionName()).isEqualTo("selector-filter-trades-abc");
        assertThat(entry.getKeyDeserializer()).isEqualTo("StringDeser");
        assertThat(entry.getValueDeserializer()).isEqualTo("ByteDeser");
        assertThat(entry.getClientType()).isEqualTo(ClientType.KAFKA);
        assertThat(entry.getCreatedAt()).isNotBlank();
    }
}
