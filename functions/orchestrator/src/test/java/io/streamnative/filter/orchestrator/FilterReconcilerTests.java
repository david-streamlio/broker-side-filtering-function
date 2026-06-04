package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FilterReconciler#reconcile()}. Mirrors {@link IdleFilterSweeperTests}:
 * backs the {@link RegistryStore} with an in-memory map that mimics the TableView, mocks the
 * {@link PulsarAdmin#functions()} health check, mocks the {@link FilterFunctionSpawner}, and
 * captures the republished {@link FilterReconciler.PublishQuery} calls. A {@link MutableClock} lets
 * backoff math advance across ticks deterministically.
 */
class FilterReconcilerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final String FN_TENANT    = "public";
    private static final String FN_NAMESPACE = "filtered";

    private TableView<byte[]> view;
    private Producer<byte[]> producer;
    private TypedMessageBuilder<byte[]> msg;
    private PulsarAdmin admin;
    private Functions functions;
    private RegistryStore registry;
    private OrchestratorConfig config;
    private FilterFunctionSpawner spawner;

    private final Map<String, byte[]> storage = new HashMap<>();
    /** Captured publishQuery calls: {controlTopic, queryId, selector, outputTopic}. */
    private final List<String[]> published = new ArrayList<>();
    private String lastKey;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() {
        view      = (TableView<byte[]>) mock(TableView.class);
        producer  = (Producer<byte[]>) mock(Producer.class);
        msg       = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);
        admin     = mock(PulsarAdmin.class);
        functions = mock(Functions.class);
        spawner   = mock(FilterFunctionSpawner.class);

        when(admin.functions()).thenReturn(functions);

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
        // Reconcile enabled, 30s interval, give up after 2 consecutive failures.
        config = OrchestratorConfig.forTestingWithReconcile(
                "persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0",
                FN_TENANT, FN_NAMESPACE,
                true, 30L, 2L);
    }

    // ---------------------------------------------------------------- helpers

    private void seedT(String key, RegistryEntry entry) throws Exception {
        storage.put(RegistryStore.TOPIC_KEY_PREFIX + key, MAPPER.writeValueAsBytes(entry));
    }

    private void seedQ(String queryId, QueryEntry entry) throws Exception {
        storage.put(RegistryStore.QUERY_KEY_PREFIX + queryId, MAPPER.writeValueAsBytes(entry));
    }

    private RegistryEntry baseEntry(String suffix) {
        return new RegistryEntry(
                "persistent://public/filtered/ctrl-" + suffix,
                "selector-filter-" + suffix,
                null, null, ClientType.PULSAR,
                "2026-05-10T00:00:00Z");
    }

    private FilterReconciler reconcilerWith(Clock clock) {
        return new FilterReconciler(registry, admin, config, clock, spawner,
                (ct, qid, sel, out, vs) -> published.add(new String[]{ct, qid, sel, out}),
                (n, v) -> {});
    }

    private FilterReconciler reconcilerAt(Instant now) {
        return reconcilerWith(Clock.fixed(now, ZoneOffset.UTC));
    }

    private static PulsarAdminException.NotFoundException notFound() {
        return new PulsarAdminException.NotFoundException(
                new RuntimeException("not found"), "not found", 404);
    }

    private void stubFunctionMissing(String functionName) throws Exception {
        doThrow(notFound()).when(functions).getFunctionStatus(FN_TENANT, FN_NAMESPACE, functionName);
    }

    // ---------------------------------------------------------------- healthy / no-op

    @Test
    void healthyFunctionIsNotRepaired() throws Exception {
        // getFunctionStatus left unstubbed → returns null (no throw) → treated as present/healthy.
        seedT("persistent://public/in/trades", baseEntry("trades").withQueryId("q-1"));
        seedQ("q-1", new QueryEntry("persistent://public/out/q1",
                List.of("persistent://public/filtered/ctrl-trades"),
                List.of("persistent://public/in/trades"), null, "region = 'US'"));

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.healthy).isEqualTo(1);
        assertThat(stats.missingRespawned).isEqualTo(0);
        verify(spawner, never()).spawn(anyString(), any(), any());
        assertThat(published).isEmpty();
    }

    @Test
    void idleEntryIsNotACandidate() throws Exception {
        // Empty activeQueryIds (idle, owned by the sweeper) — reconciler must not touch it.
        RegistryEntry idle = baseEntry("trades")
                .withQueryId("q-1")
                .withoutQueryId("q-1", Instant.parse("2026-05-12T19:00:00Z"));
        seedT("persistent://public/in/trades", idle);

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.healthy).isEqualTo(0);
        assertThat(stats.missingRespawned).isEqualTo(0);
        verify(functions, never()).getFunctionStatus(anyString(), anyString(), anyString());
        verify(spawner, never()).spawn(anyString(), any(), any());
    }

    @Test
    void preGcNullActiveQueryIdsIsNotACandidate() throws Exception {
        seedT("persistent://public/in/legacy", baseEntry("legacy"));   // null activeQueryIds

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.evaluated).isEqualTo(1);
        assertThat(stats.missingRespawned).isEqualTo(0);
        verify(spawner, never()).spawn(anyString(), any(), any());
    }

    // ---------------------------------------------------------------- missing → repair

    @Test
    void missingFunctionIsRespawnedAndQueriesRepublished() throws Exception {
        String key = "persistent://public/in/trades";
        seedT(key, baseEntry("trades").withQueryId("q-1").withQueryId("q-2"));
        seedQ("q-1", new QueryEntry("persistent://public/out/q1",
                List.of("persistent://public/filtered/ctrl-trades"), List.of(key), null,
                "region = 'US'"));
        seedQ("q-2", new QueryEntry("persistent://public/out/q2",
                List.of("persistent://public/filtered/ctrl-trades"), List.of(key), null,
                "__key__ IN ('AAPL')"));
        stubFunctionMissing("selector-filter-trades");

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.missingRespawned).isEqualTo(1);
        assertThat(stats.queriesRepublished).isEqualTo(2);
        verify(spawner).spawn(eq(key), any(), any());
        assertThat(published).hasSize(2);
        // Each republish targets the control topic with the persisted selector + outputTopic.
        assertThat(published).anySatisfy(p -> {
            assertThat(p[0]).isEqualTo("persistent://public/filtered/ctrl-trades");
            assertThat(p[1]).isEqualTo("q-1");
            assertThat(p[2]).isEqualTo("region = 'US'");
            assertThat(p[3]).isEqualTo("persistent://public/out/q1");
        });
        assertThat(published).anySatisfy(p -> {
            assertThat(p[1]).isEqualTo("q-2");
            assertThat(p[2]).isEqualTo("__key__ IN ('AAPL')");
        });
    }

    @Test
    void legacyQueryWithoutPersistedSelectorRepublishesIdentity() throws Exception {
        String key = "persistent://public/in/trades";
        seedT(key, baseEntry("trades").withQueryId("q-old"));
        // Pre-selector-persistence q: entry — no selector field.
        seedQ("q-old", new QueryEntry("persistent://public/out/qo",
                List.of("persistent://public/filtered/ctrl-trades"), List.of(key)));
        stubFunctionMissing("selector-filter-trades");

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.missingRespawned).isEqualTo(1);
        assertThat(stats.queriesRepublished).isEqualTo(1);
        assertThat(published).hasSize(1);
        assertThat(published.get(0)[1]).isEqualTo("q-old");
        assertThat(published.get(0)[2]).isEqualTo(OrchestratorFunction.IDENTITY_SELECTOR);
    }

    @Test
    void patternEntryRespawnsAsPatternRequest() throws Exception {
        String key = "persistent://public/trades/.*";
        // newEntry from a pattern request records isPattern=true; mark in-use.
        RegistryEntry pat = RegistryEntry.newEntry(
                "persistent://public/filtered/ctrl-pat", "selector-filter-pat",
                io.streamnative.filter.common.FilterRequest.registerPattern(
                        key, "persistent://public/out/x", "region = 'US'",
                        null, null, ClientType.PULSAR))
                .withQueryId("q-1");
        seedT(key, pat);
        seedQ("q-1", new QueryEntry("persistent://public/out/q1",
                List.of("persistent://public/filtered/ctrl-pat"), List.of(key), null, "region = 'US'"));
        stubFunctionMissing("selector-filter-pat");

        reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        // The reconstructed request must carry topicsPattern (not inputTopics) so the spawner
        // re-deploys a pattern subscription.
        verify(spawner).spawn(eq(key), org.mockito.ArgumentMatchers.argThat(req ->
                req != null && key.equals(req.getTopicsPattern()) && req.getInputTopics() == null), any());
    }

    // ---------------------------------------------------------------- recency re-read

    @Test
    void candidateThatBecameIdleSinceSnapshotIsSkipped() throws Exception {
        String key = "persistent://public/in/trades";
        String tKey = RegistryStore.TOPIC_KEY_PREFIX + key;

        // entrySet() (snapshot) sees an in-use entry → candidate.
        RegistryEntry inUse = baseEntry("trades").withQueryId("q-1");
        storage.put(tKey, MAPPER.writeValueAsBytes(inUse));

        // ...but the per-key re-read returns an idle entry (concurrent UNREGISTER landed).
        RegistryEntry idle = inUse.withoutQueryId("q-1", Instant.parse("2026-05-12T19:00:00Z"));
        when(view.get(tKey)).thenReturn(MAPPER.writeValueAsBytes(idle));

        FilterReconciler.ReconcileStats stats = reconcilerAt(Instant.parse("2026-05-12T20:00:00Z")).reconcile();

        assertThat(stats.skippedRecent).isEqualTo(1);
        assertThat(stats.missingRespawned).isEqualTo(0);
        verify(spawner, never()).spawn(anyString(), any(), any());
    }

    // ---------------------------------------------------------------- backoff + give-up

    @Test
    void repeatedSpawnFailureBacksOffThenGivesUp() throws Exception {
        String key = "persistent://public/in/trades";
        seedT(key, baseEntry("trades").withQueryId("q-1"));
        seedQ("q-1", new QueryEntry("persistent://public/out/q1",
                List.of("persistent://public/filtered/ctrl-trades"), List.of(key), null, "region = 'US'"));
        stubFunctionMissing("selector-filter-trades");
        when(spawner.spawn(anyString(), any(), any())).thenThrow(new RuntimeException("boom"));

        MutableClock clock = new MutableClock(Instant.parse("2026-05-12T20:00:00Z"));
        FilterReconciler reconciler = reconcilerWith(clock);

        // Tick 1: first failure (attempt 1 of 2) → errors, not yet given up.
        FilterReconciler.ReconcileStats t1 = reconciler.reconcile();
        assertThat(t1.errors).isEqualTo(1);
        assertThat(t1.gaveUp).isEqualTo(0);

        // Tick 2 immediately after: within backoff window (1 × 30s) → skipped, no new spawn attempt.
        FilterReconciler.ReconcileStats t2 = reconciler.reconcile();
        assertThat(t2.skippedRecent).isEqualTo(1);
        assertThat(t2.errors).isEqualTo(0);

        // Tick 3 past the backoff window: second failure hits maxAttempts (2) → give up.
        clock.advanceSeconds(31);
        FilterReconciler.ReconcileStats t3 = reconciler.reconcile();
        assertThat(t3.errors).isEqualTo(1);
        assertThat(t3.gaveUp).isEqualTo(1);

        // Tick 4: given-up keys are skipped permanently (until restart) — no further spawn attempts.
        clock.advanceSeconds(10_000);
        FilterReconciler.ReconcileStats t4 = reconciler.reconcile();
        assertThat(t4.skippedRecent).isEqualTo(1);
        assertThat(t4.errors).isEqualTo(0);

        // Two real spawn attempts total (tick 1 + tick 3); ticks 2 and 4 were backoff/give-up skips.
        verify(spawner, times(2)).spawn(anyString(), any(), any());
    }

    /** Test clock whose instant can be advanced between reconcile ticks. */
    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant now) {
            this.now = now;
        }

        void advanceSeconds(long seconds) {
            this.now = this.now.plusSeconds(seconds);
        }

        @Override public ZoneId getZone()              { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone)   { return this; }
        @Override public Instant instant()             { return now; }
    }
}
