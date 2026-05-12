package io.streamnative.filter.perf;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared base for performance-harness ITs. Builds a Kafka-on-Pulsar broker and a ShadowTraffic
 * synthetic-load generator wired together on a private Docker network, so the generator drives
 * the broker over the internal alias while the test JVM consumes via the host-published Pulsar
 * binary port.
 *
 * <h2>License handling</h2>
 *
 * ShadowTraffic is a paid product. The six {@code SHADOWTRAFFIC_LICENSE_*} environment variables
 * are operator-supplied at run time and never committed. Subclasses should gate themselves with
 * {@link org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable} on {@code
 * SHADOWTRAFFIC_LICENSE_ID} so the perf suite skips cleanly on machines without a license.
 * {@link #requireLicenseEnvOrFail()} is a defense-in-depth fast-fail for harness code paths that
 * still get reached.
 *
 * <h2>Networking</h2>
 *
 * The broker advertises its Kafka listener as {@code PLAINTEXT://pulsar:9092} so containers on
 * the same network resolve the alias. The Pulsar binary port (6650) is host-published so the
 * test JVM's native Pulsar client can consume directly.
 */
abstract class PerfHarnessIT {

    /**
     * sn-platform bundles the KoP protocol handler, so we don't have to ship a separate NAR.
     * Matches the image already validated by {@code KoPFilterFunctionIT}.
     */
    static final String PULSAR_IMAGE = "streamnative/sn-platform:4.2.0.6";

    /**
     * Pinned once we know which ShadowTraffic version works against KoP — until then, this is
     * {@code latest}. Track in {@code tasks/todo-perf-shadowtraffic.md} R2 ("ShadowTraffic config
     * drift") and pin during Phase 3.
     */
    static final String SHADOWTRAFFIC_IMAGE = "shadowtraffic/shadowtraffic:latest";

    static final String PULSAR_NETWORK_ALIAS = "pulsar";
    static final int KAFKA_PORT = 9092;
    static final int PULSAR_BINARY_PORT = 6650;
    static final int PULSAR_HTTP_PORT = 8080;
    static final int SHADOWTRAFFIC_METRICS_PORT = 9400;

    /** All license env vars ShadowTraffic requires at startup. */
    static final List<String> REQUIRED_LICENSE_VARS = List.of(
            "SHADOWTRAFFIC_LICENSE_ID",
            "SHADOWTRAFFIC_LICENSE_EMAIL",
            "SHADOWTRAFFIC_LICENSE_ORGANIZATION",
            "SHADOWTRAFFIC_LICENSE_EDITION",
            "SHADOWTRAFFIC_LICENSE_EXPIRATION",
            "SHADOWTRAFFIC_LICENSE_SIGNATURE"
    );

    /**
     * Throws with an actionable message listing every missing license variable. Call from any
     * harness path that needs the generator running.
     */
    static void requireLicenseEnvOrFail() {
        List<String> missing = new ArrayList<>();
        for (String var : REQUIRED_LICENSE_VARS) {
            String value = System.getenv(var);
            if (value == null || value.isBlank()) {
                missing.add(var);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "ShadowTraffic license variables are not set: " + missing
                            + ". Export all six SHADOWTRAFFIC_LICENSE_* variables before "
                            + "running the perf suite (mvn -Pperf verify). See perf/README "
                            + "for details.");
        }
    }

    /**
     * Builds a Kafka-on-Pulsar broker on the given network. The Kafka listener is advertised
     * under the network alias {@code pulsar} so sibling containers can reach it; the Pulsar
     * binary port (6650) and admin port (8080) are host-published so the test JVM can drive
     * the broker via the native Pulsar client.
     */
    static GenericContainer<?> newPulsarKoPContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse(PULSAR_IMAGE))
                .withNetwork(network)
                .withNetworkAliases(PULSAR_NETWORK_ALIAS)
                .withExposedPorts(PULSAR_BINARY_PORT, PULSAR_HTTP_PORT, KAFKA_PORT)
                .withEnv("PULSAR_PREFIX_messagingProtocols", "kafka")
                .withEnv("PULSAR_PREFIX_kafkaListeners",
                        "PLAINTEXT://0.0.0.0:" + KAFKA_PORT)
                .withEnv("PULSAR_PREFIX_kafkaAdvertisedListeners",
                        "PLAINTEXT://" + PULSAR_NETWORK_ALIAS + ":" + KAFKA_PORT)
                .withEnv("PULSAR_PREFIX_brokerEntryMetadataInterceptors",
                        "org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor")
                .withEnv("PULSAR_PREFIX_kafkaTransactionCoordinatorEnabled", "false")
                .withEnv("PULSAR_PREFIX_kafkaNamespace", "public/default")
                .withEnv("PULSAR_PREFIX_kafkaEntryFormat", "pulsar")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreation", "true")
                .withEnv("PULSAR_PREFIX_allowAutoTopicCreationType", "non-partitioned")
                .withEnv("PULSAR_PREFIX_brokerDeleteInactiveTopicsEnabled", "false")
                .waitingFor(Wait.forLogMessage(".*Created namespace public/default.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(3)));
    }

    /**
     * Builds a ShadowTraffic container on the same network as the broker, with its config file
     * bind-mounted at {@code /home/config.json} and license vars supplied from the test JVM's
     * environment. Caller is responsible for {@link #requireLicenseEnvOrFail()} first.
     */
    static GenericContainer<?> newShadowTrafficContainer(Network network, Path configHostPath) {
        GenericContainer<?> shadowTraffic = new GenericContainer<>(
                DockerImageName.parse(SHADOWTRAFFIC_IMAGE))
                .withNetwork(network)
                .withExposedPorts(SHADOWTRAFFIC_METRICS_PORT)
                .withCopyFileToContainer(MountableFile.forHostPath(configHostPath),
                        "/home/config.json")
                .withCommand("--config", "/home/config.json")
                .withStartupTimeout(Duration.ofMinutes(2));
        for (String var : REQUIRED_LICENSE_VARS) {
            String value = System.getenv(var);
            if (value != null) {
                shadowTraffic.withEnv(var.replace("SHADOWTRAFFIC_", ""), value);
            }
        }
        return shadowTraffic;
    }

    /**
     * Reads a ShadowTraffic config template from the test classpath, substitutes
     * {@code ${name}} placeholders, and writes the result to a temp file. The temp path is
     * suitable for {@link #newShadowTrafficContainer(Network, Path)}.
     *
     * @param resourceName classpath resource (e.g. {@code shadowtraffic/trades-smoke.json})
     * @param placeholders map of placeholder name → replacement value
     */
    static Path renderConfig(String resourceName, Map<String, String> placeholders) throws IOException {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        String template;
        try (var in = cl.getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("Config template not found on classpath: " + resourceName);
            }
            template = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        String rendered = template;
        for (Map.Entry<String, String> e : placeholders.entrySet()) {
            rendered = rendered.replace("${" + e.getKey() + "}", e.getValue());
        }
        Path tmp = Files.createTempFile("shadowtraffic-", ".json");
        Files.writeString(tmp, rendered, StandardCharsets.UTF_8);
        tmp.toFile().deleteOnExit();
        return tmp;
    }

    /** Convenience for building the placeholder map common to most configs. */
    static Map<String, String> commonPlaceholders(String topic, int records) {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("bootstrap", PULSAR_NETWORK_ALIAS + ":" + KAFKA_PORT);
        m.put("topic", topic);
        m.put("records", Integer.toString(records));
        return m;
    }
}
