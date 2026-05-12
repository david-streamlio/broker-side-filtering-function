package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class QueryEntryTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Test
    void legacyConstructorOmitsRegistryKeys() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"));
        assertThat(q.getOutputTopic()).isEqualTo("persistent://public/out/x");
        assertThat(q.getControlTopics()).containsExactly("persistent://public/orchestrator/ctrl-a");
        assertThat(q.getRegistryKeys()).isNull();
    }

    @Test
    void newConstructorStoresParallelRegistryKeys() {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a",
                        "persistent://public/orchestrator/ctrl-b"),
                List.of("persistent://public/in/a",
                        "persistent://public/in/b"));
        assertThat(q.getControlTopics()).hasSize(2);
        assertThat(q.getRegistryKeys()).containsExactly(
                "persistent://public/in/a", "persistent://public/in/b");
    }

    @Test
    void serdeRoundTripWithRegistryKeys() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                List.of("persistent://public/in/a"));
        byte[] payload = MAPPER.writeValueAsBytes(q);
        QueryEntry parsed = MAPPER.readValue(payload, QueryEntry.class);
        assertThat(parsed).isEqualTo(q);
        assertThat(parsed.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }

    @Test
    void serdeOmitsNullRegistryKeys() throws Exception {
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"));
        String json = MAPPER.writeValueAsString(q);
        assertThat(json).doesNotContain("registryKeys");
    }

    @Test
    void legacyJsonWithoutRegistryKeysDeserializesNull() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"]
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getRegistryKeys()).isNull();
        assertThat(parsed.getOutputTopic()).isEqualTo("persistent://public/out/x");
    }

    @Test
    void unknownFieldsTolerated() throws Exception {
        String json = """
                {
                  "outputTopic": "persistent://public/out/x",
                  "controlTopics": ["persistent://public/orchestrator/ctrl-a"],
                  "registryKeys": ["persistent://public/in/a"],
                  "someFutureField": 42
                }
                """;
        QueryEntry parsed = MAPPER.readValue(json, QueryEntry.class);
        assertThat(parsed.getOutputTopic()).isEqualTo("persistent://public/out/x");
        assertThat(parsed.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }

    @Test
    void registryKeysListIsImmutable() {
        java.util.ArrayList<String> mutableKeys = new java.util.ArrayList<>();
        mutableKeys.add("persistent://public/in/a");
        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/orchestrator/ctrl-a"),
                mutableKeys);
        // Caller mutating after construction must not affect the stored list.
        mutableKeys.add("persistent://public/in/b");
        assertThat(q.getRegistryKeys()).containsExactly("persistent://public/in/a");
    }
}
