package io.streamnative.filter.orchestrator;

import org.apache.pulsar.functions.api.Context;

import java.util.Objects;

/**
 * Operator-supplied configuration for the orchestrator function, parsed from the Pulsar function's
 * {@code userConfig} block. Read once at {@link OrchestratorFunction#initialize(Context)} time.
 */
final class OrchestratorConfig {

    /** State TableView topic — registry of per-input-topic filter function instances. */
    static final String KEY_STATE_TOPIC               = "state-topic";

    /** Pulsar package URL of the pre-uploaded selector-filter NAR, e.g. {@code function://public/filters/selector-filter@1.0.0}. */
    static final String KEY_FUNCTION_PACKAGE_URL      = "function-package-url";

    /** Tenant under which spawned filter functions are deployed. */
    static final String KEY_FUNCTION_TENANT           = "function-tenant";

    /** Namespace under which spawned filter functions are deployed. */
    static final String KEY_FUNCTION_NAMESPACE        = "function-namespace";

    /** Tenant for per-filter-function control topics. */
    static final String KEY_CONTROL_TOPIC_TENANT      = "function-control-topic-tenant";

    /** Namespace for per-filter-function control topics. */
    static final String KEY_CONTROL_TOPIC_NAMESPACE   = "function-control-topic-namespace";

    /** Parallelism applied to each spawned filter function. Default: 1. */
    static final String KEY_FUNCTION_PARALLELISM      = "function-parallelism";

    /** CPU cores for each spawned filter function instance. Default: 0.5.
     *  Keep the 1 CPU : 4 GiB ratio with {@link #KEY_FUNCTION_MEMORY_BYTES}. */
    static final String KEY_FUNCTION_CPU              = "function-cpu";

    /** Memory in bytes for each spawned filter function instance. Default: 2 GiB.
     *  Recommended ratio: 1 CPU : 4 GiB — e.g. 0.5 → 2 GiB, 1.0 → 4 GiB, 2.0 → 8 GiB. */
    static final String KEY_FUNCTION_MEMORY_BYTES     = "function-memory-bytes";

    /** Whether to set per-topic compaction on each per-function control topic. */
    static final String KEY_APPLY_COMPACTION          = "apply-compaction";

    /**
     * Pulsar HTTP admin URL the orchestrator uses to spawn filter functions. Required because
     * {@code ctx.getPulsarAdmin()} is only available in workers configured with
     * {@code pulsarFunctionsCluster} — lightweight deployments may not enable it.
     */
    static final String KEY_PULSAR_ADMIN_URL          = "pulsar-admin-url";

    /**
     * Optional bearer token applied to the orchestrator's owned PulsarAdmin client. Required
     * on clusters that enforce broker-side auth (e.g., StreamNative Cloud), where the admin
     * endpoint returns 401 without it. Leave unset for self-managed Pulsar with auth disabled.
     * Typical value: a JWT API key for the SA the orchestrator function runs under (with
     * functions-admin on its own namespace).
     */
    static final String KEY_PULSAR_ADMIN_TOKEN        = "pulsar-admin-token";

    /** Compaction threshold (bytes) for per-function control topics + registry-state topic. */
    static final String KEY_COMPACTION_THRESHOLD      = "compaction-threshold-bytes";

    /** Master switch for idle-filter GC. When false, no sweeper is started. */
    static final String KEY_IDLE_GC_ENABLED           = "idle-gc-enabled";

    /** Continuous-idle time before a filter becomes deletion-eligible. */
    static final String KEY_IDLE_GC_TTL_SECONDS       = "idle-gc-ttl-seconds";

    /** How often the sweeper evaluates t: entries. */
    static final String KEY_IDLE_GC_SWEEP_INTERVAL    = "idle-gc-sweep-interval-seconds";

    /**
     * Maximum allowed gap between SDK heartbeats before the orchestrator synthesizes an
     * UNREGISTER for a queryId. Independent of idle-gc-ttl-seconds — staleness is about
     * "consumer no longer responding," idle TTL is about "all consumers cleanly closed and
     * nothing rebound." Default is 90s (three missed heartbeats at the SDK default 30s interval).
     */
    static final String KEY_HEARTBEAT_MAX_STALE       = "heartbeat-max-stale-seconds";

    /**
     * Master switch for the continuous existence-reconcile loop. When false, no reconciler is
     * started — the orchestrator self-heals only reactively (REGISTER) and by deletion (idle GC).
     * When true, a periodic loop re-spawns filter functions that have gone missing while a live
     * query still references them.
     */
    static final String KEY_EXISTENCE_RECONCILE_ENABLED      = "existence-reconcile-enabled";

    /** How often the reconciler evaluates in-use {@code t:} entries for a missing function. */
    static final String KEY_EXISTENCE_RECONCILE_INTERVAL     = "existence-reconcile-interval-seconds";

    /**
     * Consecutive failed re-spawn attempts for a single registry key before the reconciler gives up
     * on it (until the next clean REGISTER) — backstop against hammering admin on a permanently
     * unspawnable filter (e.g. a bad package URL).
     */
    static final String KEY_EXISTENCE_RECONCILE_MAX_ATTEMPTS = "existence-reconcile-max-attempts";

    /**
     * Output-producer {@code batchingMaxPublishDelayMs} applied to every spawned filter function.
     * The Pulsar Functions framework's default batching delay dominates the filter's value-add
     * latency (it adds up to this many ms before a matched message is flushed downstream); a low
     * value trades a little throughput for much lower per-message latency. {@code 0} disables
     * batching entirely (lowest latency, highest per-message send overhead). Default {@code 1}.
     */
    static final String KEY_FILTER_OUTPUT_BATCH_MAX_DELAY_MS = "filter-output-batch-max-publish-delay-ms";

    private final String stateTopic;
    private final String functionPackageUrl;
    private final String functionTenant;
    private final String functionNamespace;
    private final String controlTopicTenant;
    private final String controlTopicNamespace;
    private final int    functionParallelism;
    private final double functionCpu;
    private final long   functionMemoryBytes;
    private final boolean applyCompaction;
    private final long compactionThresholdBytes;
    private final String pulsarAdminUrl;
    private final String pulsarAdminToken;
    private final boolean idleGcEnabled;
    private final long idleGcTtlSeconds;
    private final long idleGcSweepIntervalSeconds;
    private final long heartbeatMaxStaleSeconds;
    private final boolean existenceReconcileEnabled;
    private final long existenceReconcileIntervalSeconds;
    private final long existenceReconcileMaxAttempts;
    private final int filterOutputBatchMaxPublishDelayMs;

    private OrchestratorConfig(String stateTopic, String functionPackageUrl,
                               String functionTenant, String functionNamespace,
                               String controlTopicTenant, String controlTopicNamespace,
                               int functionParallelism, double functionCpu, long functionMemoryBytes,
                               boolean applyCompaction,
                               long compactionThresholdBytes, String pulsarAdminUrl,
                               String pulsarAdminToken,
                               boolean idleGcEnabled, long idleGcTtlSeconds,
                               long idleGcSweepIntervalSeconds,
                               long heartbeatMaxStaleSeconds,
                               boolean existenceReconcileEnabled,
                               long existenceReconcileIntervalSeconds,
                               long existenceReconcileMaxAttempts,
                               int filterOutputBatchMaxPublishDelayMs) {
        this.stateTopic                  = stateTopic;
        this.functionPackageUrl          = functionPackageUrl;
        this.functionTenant              = functionTenant;
        this.functionNamespace           = functionNamespace;
        this.controlTopicTenant          = controlTopicTenant;
        this.controlTopicNamespace       = controlTopicNamespace;
        this.functionParallelism         = functionParallelism;
        this.functionCpu                 = functionCpu;
        this.functionMemoryBytes         = functionMemoryBytes;
        this.applyCompaction             = applyCompaction;
        this.compactionThresholdBytes    = compactionThresholdBytes;
        this.pulsarAdminUrl              = pulsarAdminUrl;
        this.pulsarAdminToken            = pulsarAdminToken == null ? "" : pulsarAdminToken;
        this.idleGcEnabled               = idleGcEnabled;
        this.idleGcTtlSeconds            = idleGcTtlSeconds;
        this.idleGcSweepIntervalSeconds  = idleGcSweepIntervalSeconds;
        this.heartbeatMaxStaleSeconds    = heartbeatMaxStaleSeconds;
        this.existenceReconcileEnabled            = existenceReconcileEnabled;
        this.existenceReconcileIntervalSeconds    = existenceReconcileIntervalSeconds;
        this.existenceReconcileMaxAttempts        = existenceReconcileMaxAttempts;
        this.filterOutputBatchMaxPublishDelayMs   = filterOutputBatchMaxPublishDelayMs;
    }

    static OrchestratorConfig fromContext(Context ctx) {
        String stateTopic    = requireString(ctx, KEY_STATE_TOPIC);
        String packageUrl    = requireString(ctx, KEY_FUNCTION_PACKAGE_URL);
        String fnTenant      = requireString(ctx, KEY_FUNCTION_TENANT);
        String fnNamespace   = requireString(ctx, KEY_FUNCTION_NAMESPACE);
        String ctrlTenant    = optionalString(ctx, KEY_CONTROL_TOPIC_TENANT, fnTenant);
        String ctrlNamespace = optionalString(ctx, KEY_CONTROL_TOPIC_NAMESPACE, fnNamespace);
        int parallelism      = optionalInt(ctx, KEY_FUNCTION_PARALLELISM, 1);
        double cpu           = optionalDouble(ctx, KEY_FUNCTION_CPU, 0.5);
        long memBytes        = optionalLong(ctx, KEY_FUNCTION_MEMORY_BYTES, 2L * 1024 * 1024 * 1024);
        boolean compaction   = optionalBoolean(ctx, KEY_APPLY_COMPACTION, true);
        long threshold       = optionalLong(ctx, KEY_COMPACTION_THRESHOLD, 10L * 1024 * 1024); // 10 MB
        String adminUrl      = requireString(ctx, KEY_PULSAR_ADMIN_URL);
        String adminToken    = optionalString(ctx, KEY_PULSAR_ADMIN_TOKEN, "");
        boolean gcEnabled    = optionalBoolean(ctx, KEY_IDLE_GC_ENABLED, false);
        long gcTtl           = optionalLong(ctx, KEY_IDLE_GC_TTL_SECONDS, 300L);
        long gcInterval      = optionalLong(ctx, KEY_IDLE_GC_SWEEP_INTERVAL, 30L);
        long hbMaxStale      = optionalLong(ctx, KEY_HEARTBEAT_MAX_STALE, 90L);
        boolean reconcileEnabled  = optionalBoolean(ctx, KEY_EXISTENCE_RECONCILE_ENABLED, false);
        long reconcileInterval    = optionalLong(ctx, KEY_EXISTENCE_RECONCILE_INTERVAL, 60L);
        long reconcileMaxAttempts = optionalLong(ctx, KEY_EXISTENCE_RECONCILE_MAX_ATTEMPTS, 5L);
        int batchMaxDelayMs       = optionalInt(ctx, KEY_FILTER_OUTPUT_BATCH_MAX_DELAY_MS, 1);
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                ctrlTenant, ctrlNamespace, parallelism, cpu, memBytes, compaction, threshold, adminUrl,
                adminToken, gcEnabled, gcTtl, gcInterval, hbMaxStale,
                reconcileEnabled, reconcileInterval, reconcileMaxAttempts, batchMaxDelayMs);
    }

    /** Package-private factory for tests — GC disabled by default. */
    static OrchestratorConfig forTesting(String stateTopic, String packageUrl,
                                         String fnTenant, String fnNamespace) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, 0.5, 2L * 1024 * 1024 * 1024L, true, 10L * 1024 * 1024,
                "http://localhost:8080", "",
                false, 300L, 30L, 90L,
                false, 60L, 5L, 1);
    }

    /** Test factory exposing GC knobs — used by IdleFilterSweeper tests. */
    static OrchestratorConfig forTestingWithGc(String stateTopic, String packageUrl,
                                               String fnTenant, String fnNamespace,
                                               boolean gcEnabled, long gcTtlSeconds,
                                               long gcSweepIntervalSeconds) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, 0.5, 2L * 1024 * 1024 * 1024L, true, 10L * 1024 * 1024,
                "http://localhost:8080", "",
                gcEnabled, gcTtlSeconds, gcSweepIntervalSeconds, 90L,
                false, 60L, 5L, 1);
    }

    /** Test factory exposing both idle-GC and heartbeat knobs. */
    static OrchestratorConfig forTestingWithHeartbeat(String stateTopic, String packageUrl,
                                                     String fnTenant, String fnNamespace,
                                                     long heartbeatMaxStaleSeconds) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, 0.5, 2L * 1024 * 1024 * 1024L, true, 10L * 1024 * 1024,
                "http://localhost:8080", "",
                true, 300L, 30L, heartbeatMaxStaleSeconds,
                false, 60L, 5L, 1);
    }

    /** Test factory exposing existence-reconcile knobs — used by FilterReconciler tests. */
    static OrchestratorConfig forTestingWithReconcile(String stateTopic, String packageUrl,
                                                      String fnTenant, String fnNamespace,
                                                      boolean reconcileEnabled,
                                                      long reconcileIntervalSeconds,
                                                      long reconcileMaxAttempts) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, 0.5, 2L * 1024 * 1024 * 1024L, true, 10L * 1024 * 1024,
                "http://localhost:8080", "",
                false, 300L, 30L, 90L,
                reconcileEnabled, reconcileIntervalSeconds, reconcileMaxAttempts, 1);
    }

    public String stateTopic()                  { return stateTopic; }
    public String functionPackageUrl()          { return functionPackageUrl; }
    public String functionTenant()              { return functionTenant; }
    public String functionNamespace()           { return functionNamespace; }
    public String controlTopicTenant()          { return controlTopicTenant; }
    public String controlTopicNamespace()       { return controlTopicNamespace; }
    public int    functionParallelism()         { return functionParallelism; }
    public double functionCpu()                 { return functionCpu; }
    public long   functionMemoryBytes()         { return functionMemoryBytes; }
    public boolean applyCompaction()            { return applyCompaction; }
    public long compactionThresholdBytes()      { return compactionThresholdBytes; }
    public String pulsarAdminUrl()              { return pulsarAdminUrl; }
    public String pulsarAdminToken()            { return pulsarAdminToken; }
    public boolean idleGcEnabled()              { return idleGcEnabled; }
    public long idleGcTtlSeconds()              { return idleGcTtlSeconds; }
    public long idleGcSweepIntervalSeconds()    { return idleGcSweepIntervalSeconds; }
    public long heartbeatMaxStaleSeconds()      { return heartbeatMaxStaleSeconds; }
    public boolean existenceReconcileEnabled()           { return existenceReconcileEnabled; }
    public long existenceReconcileIntervalSeconds()      { return existenceReconcileIntervalSeconds; }
    public long existenceReconcileMaxAttempts()          { return existenceReconcileMaxAttempts; }
    public int filterOutputBatchMaxPublishDelayMs()      { return filterOutputBatchMaxPublishDelayMs; }

    private static String requireString(Context ctx, String key) {
        Object raw = ctx.getUserConfigValue(key).orElse(null);
        if (raw == null || raw.toString().isBlank()) {
            throw new IllegalArgumentException("Orchestrator userConfig missing required key: " + key);
        }
        return raw.toString();
    }

    private static String optionalString(Context ctx, String key, String defaultValue) {
        return ctx.getUserConfigValue(key).map(Object::toString).filter(s -> !s.isBlank()).orElse(defaultValue);
    }

    private static int optionalInt(Context ctx, String key, int defaultValue) {
        Object raw = ctx.getUserConfigValue(key).orElse(null);
        if (raw == null) return defaultValue;
        if (raw instanceof Number n) return n.intValue();
        // Pulsar may JSON-round-trip ints as floats ("1" → "1.0"); tolerate both.
        String s = raw.toString().trim();
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return (int) Double.parseDouble(s);
        }
    }

    private static long optionalLong(Context ctx, String key, long defaultValue) {
        Object raw = ctx.getUserConfigValue(key).orElse(null);
        if (raw == null) return defaultValue;
        if (raw instanceof Number n) return n.longValue();
        String s = raw.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return (long) Double.parseDouble(s);
        }
    }

    private static boolean optionalBoolean(Context ctx, String key, boolean defaultValue) {
        Object raw = ctx.getUserConfigValue(key).orElse(null);
        if (raw == null) return defaultValue;
        return Boolean.parseBoolean(raw.toString().trim());
    }

    private static double optionalDouble(Context ctx, String key, double defaultValue) {
        Object raw = ctx.getUserConfigValue(key).orElse(null);
        if (raw == null) return defaultValue;
        if (raw instanceof Number n) return n.doubleValue();
        try { return Double.parseDouble(raw.toString().trim()); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OrchestratorConfig that)) return false;
        return functionParallelism == that.functionParallelism
                && applyCompaction == that.applyCompaction
                && compactionThresholdBytes == that.compactionThresholdBytes
                && idleGcEnabled == that.idleGcEnabled
                && idleGcTtlSeconds == that.idleGcTtlSeconds
                && idleGcSweepIntervalSeconds == that.idleGcSweepIntervalSeconds
                && heartbeatMaxStaleSeconds == that.heartbeatMaxStaleSeconds
                && existenceReconcileEnabled == that.existenceReconcileEnabled
                && existenceReconcileIntervalSeconds == that.existenceReconcileIntervalSeconds
                && existenceReconcileMaxAttempts == that.existenceReconcileMaxAttempts
                && filterOutputBatchMaxPublishDelayMs == that.filterOutputBatchMaxPublishDelayMs
                && Objects.equals(stateTopic, that.stateTopic)
                && Objects.equals(functionPackageUrl, that.functionPackageUrl)
                && Objects.equals(functionTenant, that.functionTenant)
                && Objects.equals(functionNamespace, that.functionNamespace)
                && Objects.equals(controlTopicTenant, that.controlTopicTenant)
                && Objects.equals(controlTopicNamespace, that.controlTopicNamespace)
                && Objects.equals(pulsarAdminUrl, that.pulsarAdminUrl)
                && Objects.equals(pulsarAdminToken, that.pulsarAdminToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateTopic, functionPackageUrl, functionTenant, functionNamespace,
                controlTopicTenant, controlTopicNamespace, functionParallelism,
                applyCompaction, compactionThresholdBytes, pulsarAdminUrl, pulsarAdminToken,
                idleGcEnabled, idleGcTtlSeconds, idleGcSweepIntervalSeconds,
                heartbeatMaxStaleSeconds, existenceReconcileEnabled,
                existenceReconcileIntervalSeconds, existenceReconcileMaxAttempts,
                filterOutputBatchMaxPublishDelayMs);
    }
}
