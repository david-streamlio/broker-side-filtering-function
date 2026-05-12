package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link IdleFilterSweeper#sweep()}. Drives the sweeper with an injected
 * {@link Clock#fixed} so TTL math is deterministic; backs the {@link RegistryStore} with an
 * in-memory map that mirrors the TableView's behavior; mocks the {@link PulsarAdmin} calls used
 * for GC.
 */
class IdleFilterSweeperTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String FN_TENANT    = "public";
    private static final String FN_NAMESPACE = "filtered";

    private TableView<byte[]> view;
    private Producer<byte[]> producer;
    private TypedMessageBuilder<byte[]> msg;
    private PulsarAdmin admin;
    private Functions functions;
    private Topics topics;
    private RegistryStore registry;
    private OrchestratorConfig config;

    /** Backing storage for the mocked TableView. Tests pre-seed and read this. */
    private final Map<String, byte[]> storage = new HashMap<>();

    private String lastKey;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
        view     = (TableView<byte[]>) mock(TableView.class);
        producer = (Producer<byte[]>) mock(Producer.class);
        msg      = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        admin    = mock(PulsarAdmin.class);
        functions = mock(Functions.class);
        topics    = mock(Topics.class);

        when(admin.functions()).thenReturn(functions);
        when(admin.topics()).thenReturn(topics);

        when(view.get(anyString())).thenAnswer(inv -> storage.get(inv.getArgument(0, String.class)));
        when(view.entrySet()).thenAnswer(inv -> new HashSet<>(storage.entrySet()));

        when(producer.newMessage()).thenReturn(msg);
        doAnswer(inv -> { lastKey = inv.getArgument(0, String.class); return msg; })
                .when(msg).key(anyString());
        doAnswer(inv -> { storage.put(lastKey, inv.getArgument(0)); return msg; })
                .when(msg).value(any(byte[].class));
        doAnswer(inv -> { storage.remove(lastKey); return msg; })
                .when(msg).value(isNull());

        registry = new RegistryStore(view, producer);
        // GC enabled, 60-second TTL, 30s sweep — only TTL matters for sweep() unit tests.
        config = OrchestratorConfig.forTestingWithGc(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                true, 60L, 30L);
    }

    // ---------------------------------------------------------------- helpers

    private void seedT(String key, RegistryEntry entry) throws Exception {
        storage.put(RegistryStore.TOPIC_KEY_PREFIX + key, MAPPER.writeValueAsBytes(entry));
    }

    private void seedQ(String queryId, byte[] bytes) {
        storage.put(RegistryStore.QUERY_KEY_PREFIX + queryId, bytes);
    }

    private RegistryEntry baseEntry(String functionSuffix) {
        return new RegistryEntry(
                "persistent://public/filtered/ctrl-" + functionSuffix,
                "selector-filter-" + functionSuffix,
                null, null, ClientType.PULSAR,
                "2026-05-10T00:00:00Z");
    }

    private IdleFilterSweeper sweeperAt(Instant now) {
        return new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC));
    }

    private IdleFilterSweeper sweeperAt(Instant now, java.util.function.Consumer<String> evict) {
        return new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC), evict);
    }

    private static PulsarAdminException.NotFoundException notFound() {
        return new PulsarAdminException.NotFoundException(
                new RuntimeException("not found"), "not found", 404);
    }

    private static PulsarAdminException adminError(int status, String message) {
        return new PulsarAdminException(new RuntimeException(message), message, status);
    }

    // ---------------------------------------------------------------- GC eligibility

    @Test
    void deletesEntryIdleLongerThanTtl() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);   // 2 min > 60s TTL
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1")
                .withoutQueryId("q-1", idleSince);
        seedT("persistent://public/in/trades", idle);

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(1);
        assertThat(stats.skippedRecentRegister).isEqualTo(0);
        verify(functions).deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-trades");
        verify(topics).delete(eq("persistent://public/filtered/ctrl-trades"), eq(true));
        // The t: entry was retracted via a null tombstone.
        assertThat(storage).doesNotContainKey(RegistryStore.TOPIC_KEY_PREFIX + "persistent://public/in/trades");
    }

    @Test
    void skipsEntryWithinTtlGracePeriod() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(30);    // < 60s TTL
        RegistryEntry recentlyIdle = baseEntry("trades")
                .withQueryId("q-1")
                .withoutQueryId("q-1", idleSince);
        seedT("persistent://public/in/trades", recentlyIdle);

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        verify(functions, never()).deleteFunction(any(), any(), any());
        verify(topics, never()).delete(anyString(), anyBoolean());
    }

    @Test
    void skipsEntryWithActiveQueryIds() throws Exception {
        Instant now = Instant.parse("2026-05-12T20:00:00Z");
        RegistryEntry inUse = baseEntry("trades").withQueryId("q-1");
        seedT("persistent://public/in/trades", inUse);

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        verify(functions, never()).deleteFunction(any(), any(), any());
    }

    @Test
    void skipsPreGcEntryWithNullActiveQueryIds() throws Exception {
        // Legacy entry — v1-era state. Sweeper MUST NOT classify these as idle even if they look
        // "abandoned" (null Set < empty Set is intentional).
        Instant now = Instant.parse("2026-05-12T20:00:00Z");
        seedT("persistent://public/in/legacy", baseEntry("legacy"));

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        verify(functions, never()).deleteFunction(any(), any(), any());
    }

    @Test
    void mixedFleetGcsOnlyEligibleEntries() throws Exception {
        Instant t0  = Instant.parse("2026-05-12T20:00:00Z");
        Instant now = t0.plusSeconds(120);

        // Eligible — idle > TTL.
        seedT("in/a", baseEntry("a").withQueryId("q-a").withoutQueryId("q-a", t0));
        // Not eligible — in use.
        seedT("in/b", baseEntry("b").withQueryId("q-b"));
        // Not eligible — pre-GC (null activeQueryIds).
        seedT("in/c", baseEntry("c"));
        // Not eligible — within TTL grace.
        seedT("in/d", baseEntry("d").withQueryId("q-d").withoutQueryId("q-d", now.minusSeconds(10)));

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(4);
        assertThat(stats.deleted).isEqualTo(1);
        verify(functions).deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-a");
        verify(functions, never()).deleteFunction(eq(FN_TENANT), eq(FN_NAMESPACE), eq("selector-filter-b"));
        verify(functions, never()).deleteFunction(eq(FN_TENANT), eq(FN_NAMESPACE), eq("selector-filter-c"));
        verify(functions, never()).deleteFunction(eq(FN_TENANT), eq(FN_NAMESPACE), eq("selector-filter-d"));
    }

    // ---------------------------------------------------------------- Recency check

    @Test
    void skipsCandidateThatBecameActiveBetweenSnapshotAndRecheck() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        String key = "persistent://public/in/trades";

        RegistryEntry idleSnap = baseEntry("trades")
                .withQueryId("q-old").withoutQueryId("q-old", idleSince);
        seedT(key, idleSnap);

        // Use a TableView mock where the per-key get() returns a DIFFERENT (revived) entry
        // than what entrySet() saw — simulates a concurrent REGISTER arriving between iteration
        // and recency re-read.
        RegistryEntry revived = idleSnap.withQueryId("q-new");
        byte[] revivedBytes = MAPPER.writeValueAsBytes(revived);
        when(view.get(RegistryStore.TOPIC_KEY_PREFIX + key)).thenReturn(revivedBytes);

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        assertThat(stats.skippedRecentRegister).isEqualTo(1);
        verify(functions, never()).deleteFunction(any(), any(), any());
    }

    // ---------------------------------------------------------------- Error tolerance

    @Test
    void notFoundOnDeleteFunctionCountsAsSuccess() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(notFound()).when(functions)
                .deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-trades");

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.deleted).isEqualTo(1); // counted as success
        assertThat(stats.errors).isEqualTo(0);
        verify(topics).delete(eq("persistent://public/filtered/ctrl-trades"), eq(true));
        // Registry entry was retracted.
        assertThat(storage).doesNotContainKey(RegistryStore.TOPIC_KEY_PREFIX + "in/trades");
    }

    @Test
    void notFoundOnTopicDeleteCountsAsSuccess() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(notFound()).when(topics).delete(anyString(), anyBoolean());

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.deleted).isEqualTo(1);
        assertThat(stats.errors).isEqualTo(0);
        // Registry entry was still retracted.
        assertThat(storage).doesNotContainKey(RegistryStore.TOPIC_KEY_PREFIX + "in/trades");
    }

    @Test
    void permanentErrorOnDeleteFunctionLeavesEntryForRetry() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(adminError(403, "Forbidden")).when(functions)
                .deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-trades");

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.deleted).isEqualTo(0);
        assertThat(stats.errors).isEqualTo(1);
        // Topic delete must NOT have run — function delete is the first gate.
        verify(topics, never()).delete(anyString(), anyBoolean());
        // Registry entry survives for the next sweep tick to retry.
        assertThat(storage).containsKey(RegistryStore.TOPIC_KEY_PREFIX + "in/trades");
    }

    @Test
    void permanentErrorOnTopicDeleteLeavesEntryForRetry() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(adminError(503, "Service Unavailable")).when(topics)
                .delete(anyString(), anyBoolean());

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.deleted).isEqualTo(0);
        assertThat(stats.errors).isEqualTo(1);
        // Function was successfully deleted, but registry entry survives so the next tick can
        // attempt topic delete again (deleteFunction is then a no-op NotFound).
        assertThat(storage).containsKey(RegistryStore.TOPIC_KEY_PREFIX + "in/trades");
    }

    @Test
    void singleErrorDoesNotPoisonOtherCandidatesInSameTick() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        seedT("in/bad",  baseEntry("bad") .withQueryId("q-1").withoutQueryId("q-1", idleSince));
        seedT("in/good", baseEntry("good").withQueryId("q-2").withoutQueryId("q-2", idleSince));

        doThrow(adminError(403, "Forbidden")).when(functions)
                .deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-bad");

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();

        assertThat(stats.evaluated).isEqualTo(2);
        assertThat(stats.deleted).isEqualTo(1);
        assertThat(stats.errors).isEqualTo(1);
        verify(functions).deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-good");
        // bad entry survives, good entry retracted.
        assertThat(storage).containsKey(RegistryStore.TOPIC_KEY_PREFIX + "in/bad");
        assertThat(storage).doesNotContainKey(RegistryStore.TOPIC_KEY_PREFIX + "in/good");
    }

    // ---------------------------------------------------------------- defensive

    @Test
    void unparseableIdleSinceIsSkipped() throws Exception {
        // Construct an entry directly with malformed idleSince — represents a corrupted state.
        RegistryEntry corrupt = new RegistryEntry(
                "persistent://public/filtered/ctrl-bad", "selector-filter-bad",
                null, null, ClientType.PULSAR,
                "2026-05-10T00:00:00Z",
                Set.of(),
                "not-an-instant");
        seedT("in/bad", corrupt);

        IdleFilterSweeper.SweepStats stats = sweeperAt(Instant.parse("2026-05-12T20:00:00Z")).sweep();
        // Evaluated but the unparseable timestamp causes isGcEligible to return false defensively.
        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        verify(functions, never()).deleteFunction(any(), any(), any());
    }

    @Test
    void emptyActiveSetWithNullIdleSinceIsSkipped() throws Exception {
        // Defensive: should never happen in normal flow (withoutQueryId always stamps idleSince
        // when emptying) but the sweeper must not crash if it does.
        RegistryEntry weird = new RegistryEntry(
                "persistent://public/filtered/ctrl-weird", "selector-filter-weird",
                null, null, ClientType.PULSAR,
                "2026-05-10T00:00:00Z",
                Set.of(),
                null);
        seedT("in/weird", weird);

        IdleFilterSweeper.SweepStats stats = sweeperAt(Instant.parse("2026-05-12T20:00:00Z")).sweep();
        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(0);
        verify(functions, never()).deleteFunction(any(), any(), any());
    }

    // ---------------------------------------------------------------- beforeTopicDelete callback

    @Test
    void beforeTopicDeleteCallbackFiresBetweenFunctionAndTopicDeletes() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        // Record the order of side effects across deleteFunction → callback → deleteTopic.
        java.util.List<String> events = new java.util.ArrayList<>();
        doAnswer(inv -> { events.add("deleteFunction:" + inv.getArgument(2)); return null; })
                .when(functions).deleteFunction(anyString(), anyString(), anyString());
        doAnswer(inv -> { events.add("deleteTopic:" + inv.getArgument(0)); return null; })
                .when(topics).delete(anyString(), anyBoolean());

        IdleFilterSweeper.SweepStats stats = sweeperAt(now,
                ct -> events.add("evict:" + ct)).sweep();

        assertThat(stats.deleted).isEqualTo(1);
        assertThat(events).containsExactly(
                "deleteFunction:selector-filter-trades",
                "evict:persistent://public/filtered/ctrl-trades",
                "deleteTopic:persistent://public/filtered/ctrl-trades");
    }

    @Test
    void evictCallbackExceptionDoesNotBlockTopicDelete() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        IdleFilterSweeper.SweepStats stats = sweeperAt(now, ct -> {
            throw new RuntimeException("simulated producer close failure");
        }).sweep();

        // Even though the callback threw, the topic delete still proceeded — the t: entry
        // retracted, GC counted as deleted.
        assertThat(stats.deleted).isEqualTo(1);
        verify(topics).delete(eq("persistent://public/filtered/ctrl-trades"), eq(true));
        assertThat(storage).doesNotContainKey(RegistryStore.TOPIC_KEY_PREFIX + "in/trades");
    }

    @Test
    void evictCallbackNotInvokedWhenFunctionDeleteFails() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(adminError(403, "Forbidden")).when(functions)
                .deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-trades");

        boolean[] called = {false};
        IdleFilterSweeper.SweepStats stats = sweeperAt(now, ct -> called[0] = true).sweep();

        assertThat(stats.errors).isEqualTo(1);
        assertThat(called[0]).as("evict callback must not fire if function delete failed").isFalse();
    }

    // ---------------------------------------------------------------- defensive

    @Test
    void qPrefixEntriesAreIgnoredByTPass() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        seedT("in/a", baseEntry("a").withQueryId("q-1").withoutQueryId("q-1", idleSince));
        // A coexisting valid q: entry must not affect the t: GC pass. (Pre-heartbeat support, a
        // junk-bytes q: entry was used here to test prefix filtering; now we also have a real q:
        // pass over QueryEntries, so the test uses a well-formed entry. The intent — q: entries
        // do not break t: iteration — survives.)
        seedQ("some-query-id", MAPPER.writeValueAsBytes(new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-x"),
                List.of("in/a"))));

        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();
        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.deleted).isEqualTo(1);
    }

    // ---------------------------------------------------------------- Stale-heartbeat detection

    @Test
    void freshHeartbeatNeverSynthesizesUnregister() throws Exception {
        Instant lastSeen = Instant.parse("2026-05-12T22:00:00Z");
        Instant now      = lastSeen.plusSeconds(30);  // 30s < 60s default maxStale (we set 60s below)

        // GC + heartbeat both enabled with 60s maxStale.
        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry q = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-trades"),
                List.of("in/trades"),
                lastSeen.toString());
        seedQ("q-1", MAPPER.writeValueAsBytes(q));

        java.util.List<String> synthesized = new java.util.ArrayList<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC), ct -> {}, (qid, qe) -> synthesized.add(qid));

        IdleFilterSweeper.SweepStats stats = sweeper.sweep();

        assertThat(stats.heartbeatsEvaluated).isEqualTo(1);
        assertThat(stats.heartbeatsSynthesized).isEqualTo(0);
        assertThat(synthesized).isEmpty();
    }

    @Test
    void staleHeartbeatSynthesizesUnregister() throws Exception {
        Instant lastSeen = Instant.parse("2026-05-12T22:00:00Z");
        Instant now      = lastSeen.plusSeconds(120);  // > 60s maxStale

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry stale = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-trades"),
                List.of("in/trades"),
                lastSeen.toString());
        seedQ("q-stale", MAPPER.writeValueAsBytes(stale));

        java.util.List<String> synthesized = new java.util.ArrayList<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC), ct -> {}, (qid, qe) -> synthesized.add(qid));

        IdleFilterSweeper.SweepStats stats = sweeper.sweep();

        assertThat(stats.heartbeatsEvaluated).isEqualTo(1);
        assertThat(stats.heartbeatsSynthesized).isEqualTo(1);
        assertThat(synthesized).containsExactly("q-stale");
    }

    @Test
    void preHeartbeatEntryWithNullLastSeenIsNeverStale() throws Exception {
        Instant now = Instant.parse("2026-05-12T22:10:00Z");

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        // Legacy q: entry — null lastSeen because it was written before heartbeat support.
        QueryEntry legacy = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-trades"),
                List.of("in/trades"));
        seedQ("q-legacy", MAPPER.writeValueAsBytes(legacy));

        java.util.List<String> synthesized = new java.util.ArrayList<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC), ct -> {}, (qid, qe) -> synthesized.add(qid));

        IdleFilterSweeper.SweepStats stats = sweeper.sweep();

        // The legacy entry was evaluated but null lastSeen means "not stale" per the design.
        assertThat(stats.heartbeatsEvaluated).isEqualTo(1);
        assertThat(stats.heartbeatsSynthesized).isEqualTo(0);
        assertThat(synthesized).isEmpty();
    }

    @Test
    void heartbeatRecencyCheckCatchesRecentTouch() throws Exception {
        Instant snapshotLastSeen = Instant.parse("2026-05-12T22:00:00Z");
        Instant now              = snapshotLastSeen.plusSeconds(120);

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry snapshot = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-trades"),
                List.of("in/trades"),
                snapshotLastSeen.toString());
        seedQ("q-revived", MAPPER.writeValueAsBytes(snapshot));

        // Simulate a concurrent HEARTBEAT arriving between snapshot iteration and recency re-read.
        // The per-key get returns a refreshed entry with current lastSeen.
        QueryEntry refreshed = snapshot.withLastSeen(now.minusSeconds(5));
        when(view.get(RegistryStore.QUERY_KEY_PREFIX + "q-revived"))
                .thenReturn(MAPPER.writeValueAsBytes(refreshed));

        java.util.List<String> synthesized = new java.util.ArrayList<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC), ct -> {}, (qid, qe) -> synthesized.add(qid));

        IdleFilterSweeper.SweepStats stats = sweeper.sweep();

        assertThat(stats.heartbeatsEvaluated).isEqualTo(1);
        assertThat(stats.heartbeatsSynthesized).isEqualTo(0);
        assertThat(stats.heartbeatsSkippedRecent).isEqualTo(1);
        assertThat(synthesized).isEmpty();
    }

    @Test
    void synthesizeUnregisterErrorContinuesOnNextCandidate() throws Exception {
        Instant lastSeen = Instant.parse("2026-05-12T22:00:00Z");
        Instant now      = lastSeen.plusSeconds(120);

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry stale = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-x"),
                List.of("in/x"),
                lastSeen.toString());
        seedQ("q-bad",  MAPPER.writeValueAsBytes(stale));
        seedQ("q-good", MAPPER.writeValueAsBytes(stale));

        java.util.List<String> synthesized = new java.util.ArrayList<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC),
                ct -> {},
                (qid, qe) -> {
                    if ("q-bad".equals(qid)) throw new RuntimeException("simulated failure");
                    synthesized.add(qid);
                });

        IdleFilterSweeper.SweepStats stats = sweeper.sweep();

        assertThat(stats.heartbeatsEvaluated).isEqualTo(2);
        assertThat(stats.heartbeatsSynthesized).isEqualTo(1);
        assertThat(stats.heartbeatsErrors).isEqualTo(1);
        assertThat(synthesized).containsExactly("q-good");
    }

    // ---------------------------------------------------------------- Metric callback

    /** Builds a sweeper at {@code now} whose every per-stat increment fires into {@code sink}. */
    private IdleFilterSweeper meteredSweeperAt(Instant now, Map<String, Double> sink) {
        return new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC),
                ct -> {},
                (qid, q) -> {},
                (name, value) -> sink.merge(name, value, Double::sum));
    }

    @Test
    void metricCallbackFiresForEvaluatedAndDeletedOnSuccessfulGc() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        Map<String, Double> sink = new HashMap<>();
        meteredSweeperAt(now, sink).sweep();

        assertThat(sink)
                .containsEntry("orchestrator.gc.candidates_evaluated", 1.0)
                .containsEntry("orchestrator.gc.functions_deleted", 1.0)
                .doesNotContainKey("orchestrator.gc.skipped_recent_register")
                .doesNotContainKey("orchestrator.gc.errors");
    }

    @Test
    void metricCallbackFiresForSkippedRecentRegisterOnRecencyMiss() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        String key = "persistent://public/in/trades";

        RegistryEntry idleSnap = baseEntry("trades")
                .withQueryId("q-old").withoutQueryId("q-old", idleSince);
        seedT(key, idleSnap);

        RegistryEntry revived = idleSnap.withQueryId("q-new");
        when(view.get(RegistryStore.TOPIC_KEY_PREFIX + key))
                .thenReturn(MAPPER.writeValueAsBytes(revived));

        Map<String, Double> sink = new HashMap<>();
        meteredSweeperAt(now, sink).sweep();

        assertThat(sink)
                .containsEntry("orchestrator.gc.candidates_evaluated", 1.0)
                .containsEntry("orchestrator.gc.skipped_recent_register", 1.0)
                .doesNotContainKey("orchestrator.gc.functions_deleted");
    }

    @Test
    void metricCallbackFiresForErrorsWhenFunctionDeleteFails() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        doThrow(adminError(403, "Forbidden")).when(functions)
                .deleteFunction(FN_TENANT, FN_NAMESPACE, "selector-filter-trades");

        Map<String, Double> sink = new HashMap<>();
        meteredSweeperAt(now, sink).sweep();

        assertThat(sink)
                .containsEntry("orchestrator.gc.candidates_evaluated", 1.0)
                .containsEntry("orchestrator.gc.errors", 1.0)
                .doesNotContainKey("orchestrator.gc.functions_deleted");
    }

    @Test
    void metricCallbackAccumulatesAcrossMultipleCandidatesInOneTick() throws Exception {
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);

        // Two delete-eligible + one in-use + one within-TTL = 4 evaluated, 2 deleted.
        seedT("in/a", baseEntry("a").withQueryId("q-a").withoutQueryId("q-a", idleSince));
        seedT("in/b", baseEntry("b").withQueryId("q-b").withoutQueryId("q-b", idleSince));
        seedT("in/c", baseEntry("c").withQueryId("q-c"));
        seedT("in/d", baseEntry("d").withQueryId("q-d").withoutQueryId("q-d", now.minusSeconds(10)));

        Map<String, Double> sink = new HashMap<>();
        meteredSweeperAt(now, sink).sweep();

        assertThat(sink)
                .containsEntry("orchestrator.gc.candidates_evaluated", 4.0)
                .containsEntry("orchestrator.gc.functions_deleted", 2.0);
    }

    @Test
    void metricCallbackFiresForHeartbeatSynthesizedAndSkippedRecent() throws Exception {
        Instant lastSeenStale = Instant.parse("2026-05-12T22:00:00Z");
        Instant lastSeenFresh = Instant.parse("2026-05-12T22:01:30Z");
        Instant now           = Instant.parse("2026-05-12T22:02:00Z"); // 120s past stale, 30s past fresh

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry stale = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-x"),
                List.of("in/x"),
                lastSeenStale.toString());
        seedQ("q-stale", MAPPER.writeValueAsBytes(stale));

        // Pre-stale by snapshot time but recency re-read returns a refreshed entry → skipped_recent.
        QueryEntry snapshotStale = new QueryEntry(
                "persistent://public/out/y",
                List.of("persistent://public/filtered/ctrl-y"),
                List.of("in/y"),
                lastSeenStale.toString());
        seedQ("q-revived", MAPPER.writeValueAsBytes(snapshotStale));
        QueryEntry refreshed = snapshotStale.withLastSeen(lastSeenFresh);
        when(view.get(RegistryStore.QUERY_KEY_PREFIX + "q-revived"))
                .thenReturn(MAPPER.writeValueAsBytes(refreshed));

        Map<String, Double> sink = new HashMap<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC),
                ct -> {},
                (qid, q) -> {},
                (name, value) -> sink.merge(name, value, Double::sum));
        sweeper.sweep();

        assertThat(sink)
                .containsEntry("orchestrator.heartbeats.synthesized", 1.0)
                .containsEntry("orchestrator.heartbeats.skipped_recent", 1.0)
                .doesNotContainKey("orchestrator.heartbeats.errors");
    }

    @Test
    void metricCallbackFiresForHeartbeatErrorsWhenSynthesizeThrows() throws Exception {
        Instant lastSeen = Instant.parse("2026-05-12T22:00:00Z");
        Instant now      = lastSeen.plusSeconds(120);

        config = OrchestratorConfig.forTestingWithHeartbeat(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                60L);

        QueryEntry stale = new QueryEntry(
                "persistent://public/out/x",
                List.of("persistent://public/filtered/ctrl-x"),
                List.of("in/x"),
                lastSeen.toString());
        seedQ("q-bad", MAPPER.writeValueAsBytes(stale));

        Map<String, Double> sink = new HashMap<>();
        IdleFilterSweeper sweeper = new IdleFilterSweeper(registry, admin, config,
                Clock.fixed(now, ZoneOffset.UTC),
                ct -> {},
                (qid, q) -> { throw new RuntimeException("simulated"); },
                (name, value) -> sink.merge(name, value, Double::sum));
        sweeper.sweep();

        assertThat(sink)
                .containsEntry("orchestrator.heartbeats.errors", 1.0)
                .doesNotContainKey("orchestrator.heartbeats.synthesized");
    }

    @Test
    void metricCallbackIsNoOpByDefaultThroughLegacyConstructors() throws Exception {
        // Sanity check: existing tests use the 4-arg, 5-arg, and 6-arg constructors. Those wire a
        // no-op metrics callback so any code reaching for it cannot NPE. Drive a tick through the
        // 4-arg constructor with an idle entry to exercise the deletion path with metrics=null-op.
        Instant idleSince = Instant.parse("2026-05-12T20:00:00Z");
        Instant now       = idleSince.plusSeconds(120);
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1").withoutQueryId("q-1", idleSince);
        seedT("in/trades", idle);

        // No exception expected; metrics callback is the no-op default.
        IdleFilterSweeper.SweepStats stats = sweeperAt(now).sweep();
        assertThat(stats.deleted).isEqualTo(1);
    }
}
