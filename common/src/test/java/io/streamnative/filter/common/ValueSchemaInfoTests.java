package io.streamnative.filter.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ValueSchemaInfoTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void roundTripsAllFields() throws Exception {
        ValueSchemaInfo si = new ValueSchemaInfo(
                "com.acme.Trade", "JSON", "eyJ0eXBlIjoib2JqZWN0In0=",
                Map.of("__alwaysAllowNull", "true", "__jsr310ConversionEnabled", "false"));

        ValueSchemaInfo parsed = MAPPER.readValue(MAPPER.writeValueAsBytes(si), ValueSchemaInfo.class);

        assertThat(parsed).isEqualTo(si);
        assertThat(parsed.getName()).isEqualTo("com.acme.Trade");
        assertThat(parsed.getType()).isEqualTo("JSON");
        assertThat(parsed.getDefinition()).isEqualTo("eyJ0eXBlIjoib2JqZWN0In0=");
        assertThat(parsed.getProperties()).containsEntry("__alwaysAllowNull", "true");
    }

    @Test
    void omitsNullFieldsAndToleratesUnknown() throws Exception {
        ValueSchemaInfo si = new ValueSchemaInfo("STRING", "STRING", null, null);
        String json = MAPPER.writeValueAsString(si);
        assertThat(json).doesNotContain("definition").doesNotContain("properties");

        // Forward-compat: a newer producer may add fields.
        ValueSchemaInfo parsed = MAPPER.readValue(
                "{\"name\":\"STRING\",\"type\":\"STRING\",\"futureField\":1}", ValueSchemaInfo.class);
        assertThat(parsed.getName()).isEqualTo("STRING");
        assertThat(parsed.getType()).isEqualTo("STRING");
        assertThat(parsed.getDefinition()).isNull();
    }

    @Test
    void toStringDoesNotDumpDefinition() {
        ValueSchemaInfo si = new ValueSchemaInfo("X", "AVRO", "AAAABBBBCCCC", null);
        assertThat(si.toString()).contains("definitionLen=12").doesNotContain("AAAABBBBCCCC");
    }
}
