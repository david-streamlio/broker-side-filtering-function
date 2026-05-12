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

    /** Parallelism applied to each spawned filter function. */
    static final String KEY_FUNCTION_PARALLELISM      = "function-parallelism";

    /** Whether to set per-topic compaction on each per-function control topic. */
    static final String KEY_APPLY_COMPACTION          = "apply-compaction";

    /**
     * Pulsar HTTP admin URL the orchestrator uses to spawn filter functions. Required because
     * {@code ctx.getPulsarAdmin()} is only available in workers configured with
     * {@code pulsarFunctionsCluster} — lightweight deployments may not enable it.
     */
    static final String KEY_PULSAR_ADMIN_URL          = "pulsar-admin-url";

    /** Compaction threshold (bytes) for per-function control topics + registry-state topic. */
    static final String KEY_COMPACTION_THRESHOLD      = "compaction-threshold-bytes";

    /** Master switch for idle-filter GC. When false, no sweeper is started. */
    static final String KEY_IDLE_GC_ENABLED           = "idle-gc-enabled";

    /** Continuous-idle time before a filter becomes deletion-eligible. */
    static final String KEY_IDLE_GC_TTL_SECONDS       = "idle-gc-ttl-seconds";

    /** How often the sweeper evaluates t: entries. */
    static final String KEY_IDLE_GC_SWEEP_INTERVAL    = "idle-gc-sweep-interval-seconds";

    private final String stateTopic;
    private final String functionPackageUrl;
    private final String functionTenant;
    private final String functionNamespace;
    private final String controlTopicTenant;
    private final String controlTopicNamespace;
    private final int functionParallelism;
    private final boolean applyCompaction;
    private final long compactionThresholdBytes;
    private final String pulsarAdminUrl;
    private final boolean idleGcEnabled;
    private final long idleGcTtlSeconds;
    private final long idleGcSweepIntervalSeconds;

    private OrchestratorConfig(String stateTopic, String functionPackageUrl,
                               String functionTenant, String functionNamespace,
                               String controlTopicTenant, String controlTopicNamespace,
                               int functionParallelism, boolean applyCompaction,
                               long compactionThresholdBytes, String pulsarAdminUrl,
                               boolean idleGcEnabled, long idleGcTtlSeconds,
                               long idleGcSweepIntervalSeconds) {
        this.stateTopic                  = stateTopic;
        this.functionPackageUrl          = functionPackageUrl;
        this.functionTenant              = functionTenant;
        this.functionNamespace           = functionNamespace;
        this.controlTopicTenant          = controlTopicTenant;
        this.controlTopicNamespace       = controlTopicNamespace;
        this.functionParallelism         = functionParallelism;
        this.applyCompaction             = applyCompaction;
        this.compactionThresholdBytes    = compactionThresholdBytes;
        this.pulsarAdminUrl              = pulsarAdminUrl;
        this.idleGcEnabled               = idleGcEnabled;
        this.idleGcTtlSeconds            = idleGcTtlSeconds;
        this.idleGcSweepIntervalSeconds  = idleGcSweepIntervalSeconds;
    }

    static OrchestratorConfig fromContext(Context ctx) {
        String stateTopic    = requireString(ctx, KEY_STATE_TOPIC);
        String packageUrl    = requireString(ctx, KEY_FUNCTION_PACKAGE_URL);
        String fnTenant      = requireString(ctx, KEY_FUNCTION_TENANT);
        String fnNamespace   = requireString(ctx, KEY_FUNCTION_NAMESPACE);
        String ctrlTenant    = optionalString(ctx, KEY_CONTROL_TOPIC_TENANT, fnTenant);
        String ctrlNamespace = optionalString(ctx, KEY_CONTROL_TOPIC_NAMESPACE, fnNamespace);
        int parallelism      = optionalInt(ctx, KEY_FUNCTION_PARALLELISM, 1);
        boolean compaction   = optionalBoolean(ctx, KEY_APPLY_COMPACTION, true);
        long threshold       = optionalLong(ctx, KEY_COMPACTION_THRESHOLD, 10L * 1024 * 1024); // 10 MB
        String adminUrl      = requireString(ctx, KEY_PULSAR_ADMIN_URL);
        boolean gcEnabled    = optionalBoolean(ctx, KEY_IDLE_GC_ENABLED, false);
        long gcTtl           = optionalLong(ctx, KEY_IDLE_GC_TTL_SECONDS, 300L);
        long gcInterval      = optionalLong(ctx, KEY_IDLE_GC_SWEEP_INTERVAL, 30L);
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                ctrlTenant, ctrlNamespace, parallelism, compaction, threshold, adminUrl,
                gcEnabled, gcTtl, gcInterval);
    }

    /** Package-private factory for tests — GC disabled by default. */
    static OrchestratorConfig forTesting(String stateTopic, String packageUrl,
                                         String fnTenant, String fnNamespace) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, true, 10L * 1024 * 1024,
                "http://localhost:8080",
                false, 300L, 30L);
    }

    /** Test factory exposing GC knobs — used by IdleFilterSweeper tests. */
    static OrchestratorConfig forTestingWithGc(String stateTopic, String packageUrl,
                                               String fnTenant, String fnNamespace,
                                               boolean gcEnabled, long gcTtlSeconds,
                                               long gcSweepIntervalSeconds) {
        return new OrchestratorConfig(stateTopic, packageUrl, fnTenant, fnNamespace,
                fnTenant, fnNamespace, 1, true, 10L * 1024 * 1024,
                "http://localhost:8080",
                gcEnabled, gcTtlSeconds, gcSweepIntervalSeconds);
    }

    public String stateTopic()                  { return stateTopic; }
    public String functionPackageUrl()          { return functionPackageUrl; }
    public String functionTenant()              { return functionTenant; }
    public String functionNamespace()           { return functionNamespace; }
    public String controlTopicTenant()          { return controlTopicTenant; }
    public String controlTopicNamespace()       { return controlTopicNamespace; }
    public int functionParallelism()            { return functionParallelism; }
    public boolean applyCompaction()            { return applyCompaction; }
    public long compactionThresholdBytes()      { return compactionThresholdBytes; }
    public String pulsarAdminUrl()              { return pulsarAdminUrl; }
    public boolean idleGcEnabled()              { return idleGcEnabled; }
    public long idleGcTtlSeconds()              { return idleGcTtlSeconds; }
    public long idleGcSweepIntervalSeconds()    { return idleGcSweepIntervalSeconds; }

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

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof OrchestratorConfig that)) return false;
        return functionParallelism == that.functionParallelism
                && applyCompaction == that.applyCompaction
                && compactionThresholdBytes == that.compactionThresholdBytes
                && idleGcEnabled == that.idleGcEnabled
                && idleGcTtlSeconds == that.idleGcTtlSeconds
                && idleGcSweepIntervalSeconds == that.idleGcSweepIntervalSeconds
                && Objects.equals(stateTopic, that.stateTopic)
                && Objects.equals(functionPackageUrl, that.functionPackageUrl)
                && Objects.equals(functionTenant, that.functionTenant)
                && Objects.equals(functionNamespace, that.functionNamespace)
                && Objects.equals(controlTopicTenant, that.controlTopicTenant)
                && Objects.equals(controlTopicNamespace, that.controlTopicNamespace)
                && Objects.equals(pulsarAdminUrl, that.pulsarAdminUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateTopic, functionPackageUrl, functionTenant, functionNamespace,
                controlTopicTenant, controlTopicNamespace, functionParallelism,
                applyCompaction, compactionThresholdBytes, pulsarAdminUrl,
                idleGcEnabled, idleGcTtlSeconds, idleGcSweepIntervalSeconds);
    }
}
