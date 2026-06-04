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
}
