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

package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import org.junit.jupiter.api.Test;

import java.time.Instant;

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
        // newEntry leaves activeQueryIds null — the first withQueryId(...) initializes it.
        assertThat(entry.getActiveQueryIds()).isNull();
        assertThat(entry.getIdleSince()).isNull();
    }

    // ---------------------------------------------------------------- activeQueryIds / GC

    @Test
    void withQueryIdInitializesAndTracks() {
        RegistryEntry entry = entryWithSerializers(null, null);
        RegistryEntry afterFirst = entry.withQueryId("q-1");
        assertThat(afterFirst.getActiveQueryIds()).containsExactly("q-1");
        assertThat(afterFirst.getIdleSince()).isNull();

        RegistryEntry afterSecond = afterFirst.withQueryId("q-2");
        assertThat(afterSecond.getActiveQueryIds()).containsExactlyInAnyOrder("q-1", "q-2");
    }

    @Test
    void withQueryIdClearsIdleSince() {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry idle = entryWithSerializers(null, null)
                .withQueryId("q-1")
                .withoutQueryId("q-1", t0);
        assertThat(idle.getActiveQueryIds()).isEmpty();
        assertThat(idle.getIdleSince()).isEqualTo(t0.toString());

        RegistryEntry revived = idle.withQueryId("q-2");
        assertThat(revived.getActiveQueryIds()).containsExactly("q-2");
        assertThat(revived.getIdleSince()).isNull();
    }

    @Test
    void withoutQueryIdSetsIdleSinceWhenEmpty() {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry entry = entryWithSerializers(null, null).withQueryId("q-1");
        RegistryEntry afterRemove = entry.withoutQueryId("q-1", t0);
        assertThat(afterRemove.getActiveQueryIds()).isEmpty();
        assertThat(afterRemove.getIdleSince()).isEqualTo(t0.toString());
    }

    @Test
    void withoutQueryIdLeavesIdleSinceNullWhenStillActive() {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry entry = entryWithSerializers(null, null)
                .withQueryId("q-1")
                .withQueryId("q-2");
        RegistryEntry afterRemoveOne = entry.withoutQueryId("q-1", t0);
        assertThat(afterRemoveOne.getActiveQueryIds()).containsExactly("q-2");
        assertThat(afterRemoveOne.getIdleSince()).isNull();
    }

    @Test
    void withoutQueryIdOnLegacyEntryIsNoOp() {
        // Legacy entry (activeQueryIds = null) is intentionally a no-op so old v1 entries
        // don't become GC-eligible based on incomplete bookkeeping.
        RegistryEntry legacy = entryWithSerializers(null, null);
        assertThat(legacy.getActiveQueryIds()).isNull();
        RegistryEntry after = legacy.withoutQueryId("q-1", Instant.now());
        assertThat(after).isSameAs(legacy);
    }

    @Test
    void withoutQueryIdUnknownQueryIdIsIdempotent() {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry entry = entryWithSerializers(null, null).withQueryId("q-1");
        RegistryEntry after = entry.withoutQueryId("q-not-present", t0);
        // q-1 still present, idleSince still null.
        assertThat(after.getActiveQueryIds()).containsExactly("q-1");
        assertThat(after.getIdleSince()).isNull();
    }

    @Test
    void mergedWithPreservesActiveQueryIdsAndIdleSince() {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry idle = entryWithSerializers(null, null)
                .withQueryId("q-1")
                .withoutQueryId("q-1", t0);
        // Schema-upgrade (null serializers → asserted) must not stomp GC bookkeeping.
        FilterRequest req = reqWithSerializers("StringDeser", "ByteDeser");
        RegistryEntry merged = idle.mergedWith(req);
        assertThat(merged.getKeyDeserializer()).isEqualTo("StringDeser");
        assertThat(merged.getActiveQueryIds()).isEmpty();
        assertThat(merged.getIdleSince()).isEqualTo(t0.toString());
    }

    @Test
    void serdeRoundTripWithActiveQueryIds() throws Exception {
        Instant t0 = Instant.parse("2026-05-12T19:00:00Z");
        RegistryEntry entry = entryWithSerializers("StringDeser", "ByteDeser")
                .withQueryId("q-1")
                .withoutQueryId("q-1", t0);
        byte[] payload = MAPPER.writeValueAsBytes(entry);
        RegistryEntry parsed = MAPPER.readValue(payload, RegistryEntry.class);
        assertThat(parsed.getActiveQueryIds()).isEmpty();
        assertThat(parsed.getIdleSince()).isEqualTo(t0.toString());
        assertThat(parsed).isEqualTo(entry);
    }

    @Test
    void legacyJsonWithoutGcFieldsDeserializesWithNulls() throws Exception {
        // A v1-era serialized entry — no activeQueryIds, no idleSince.
        String json = """
                {
                  "controlTopic": "persistent://t/n/ctrl",
                  "filterFunctionName": "fn-1",
                  "createdAt": "2026-05-11T00:00:00Z"
                }
                """;
        RegistryEntry parsed = MAPPER.readValue(json, RegistryEntry.class);
        assertThat(parsed.getActiveQueryIds()).isNull();
        assertThat(parsed.getIdleSince()).isNull();
    }

    // ---------------------------------------------------------------- isPattern (reconcile re-spawn)

    @Test
    void newEntryRecordsInputTopicAsNotPattern() {
        RegistryEntry entry = RegistryEntry.newEntry(
                "persistent://public/orchestrator/ctrl-trades", "selector-filter-trades",
                reqWithSerializers("StringDeser", "ByteDeser"));
        assertThat(entry.isPattern()).isFalse();
    }

    @Test
    void newEntryRecordsTopicsPatternAsPattern() {
        FilterRequest patternReq = FilterRequest.registerPattern(
                "persistent://public/trades/.*", "persistent://public/out/x", "region = 'US'",
                null, null, ClientType.PULSAR);
        RegistryEntry entry = RegistryEntry.newEntry(
                "persistent://public/orchestrator/ctrl-pat", "selector-filter-pat", patternReq);
        assertThat(entry.isPattern()).isTrue();
    }

    @Test
    void isPatternSurvivesQueryIdCopyMethods() {
        FilterRequest patternReq = FilterRequest.registerPattern(
                "persistent://public/trades/.*", "persistent://public/out/x", "region = 'US'",
                null, null, ClientType.PULSAR);
        RegistryEntry entry = RegistryEntry.newEntry("persistent://t/n/ctrl", "fn", patternReq)
                .withQueryId("q-1");
        assertThat(entry.isPattern()).isTrue();
        RegistryEntry idled = entry.withoutQueryId("q-1", Instant.parse("2026-05-12T00:00:00Z"));
        assertThat(idled.isPattern()).isTrue();
    }

    @Test
    void serdeRoundTripPreservesIsPattern() throws Exception {
        FilterRequest patternReq = FilterRequest.registerPattern(
                "persistent://public/trades/.*", "persistent://public/out/x", "region = 'US'",
                null, null, ClientType.PULSAR);
        RegistryEntry entry = RegistryEntry.newEntry("persistent://t/n/ctrl", "fn", patternReq);
        RegistryEntry parsed = MAPPER.readValue(MAPPER.writeValueAsBytes(entry), RegistryEntry.class);
        assertThat(parsed.isPattern()).isTrue();
        assertThat(parsed).isEqualTo(entry);
    }

    @Test
    void legacyJsonWithoutIsPatternDefaultsFalse() throws Exception {
        String json = """
                {
                  "controlTopic": "persistent://t/n/ctrl",
                  "filterFunctionName": "fn-1",
                  "createdAt": "2026-05-11T00:00:00Z"
                }
                """;
        RegistryEntry parsed = MAPPER.readValue(json, RegistryEntry.class);
        assertThat(parsed.isPattern()).isFalse();
    }
}
