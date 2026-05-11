package io.streamnative.filter.common;

/**
 * SDK family that produced a {@link FilterRequest}. Recorded by the orchestrator in the per-input-topic
 * registry entry. v1 has no per-type behavior; reserved for future routing decisions (e.g., output-topic
 * namespace selection for {@link #KAFKA} clients that require KoP-mapped namespaces).
 */
public enum ClientType {
    /** Standard Apache Pulsar Java client. */
    PULSAR,

    /** Apache Kafka client pointed at a KoP-enabled Pulsar broker. */
    KAFKA
}
