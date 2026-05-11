package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TableView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Facade over the orchestrator's state TableView. The state topic carries two logical keyspaces
 * distinguished by key prefix:
 *
 * <ul>
 *   <li>{@code t:<inputTopicFqnOrPattern>} → {@link RegistryEntry} — one entry per registered input
 *       topic (or pattern). Records the spawned filter function's name and control topic.</li>
 *   <li>{@code q:<queryId>} → {@link QueryEntry} — one entry per active query. Records the SDK
 *       outputTopic and the list of per-function control topics this queryId was published to,
 *       so MODIFY can reconstruct downstream QueryMessages and UNREGISTER can fan out
 *       tombstones.</li>
 * </ul>
 *
 * <p>TableView semantics: writes go via the state-topic {@link Producer}; reads are served from the
 * in-memory TableView (eventually consistent — usually sub-second). Tombstones (null body) remove
 * entries.
 */
final class RegistryStore {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryStore.class);

    static final String TOPIC_KEY_PREFIX  = "t:";
    static final String QUERY_KEY_PREFIX  = "q:";

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final TableView<byte[]> view;
    private final Producer<byte[]> producer;

    RegistryStore(TableView<byte[]> view, Producer<byte[]> producer) {
        this.view     = view;
        this.producer = producer;
    }

    // ---------- t:<inputTopic> keyspace ----------

    RegistryEntry getInputTopicEntry(String inputTopicOrPattern) {
        byte[] raw = view.get(TOPIC_KEY_PREFIX + inputTopicOrPattern);
        return raw == null ? null : decode(raw, RegistryEntry.class);
    }

    void putInputTopicEntry(String inputTopicOrPattern, RegistryEntry entry) throws PulsarClientException {
        publish(TOPIC_KEY_PREFIX + inputTopicOrPattern, entry);
    }

    void removeInputTopicEntry(String inputTopicOrPattern) throws PulsarClientException {
        producer.newMessage().key(TOPIC_KEY_PREFIX + inputTopicOrPattern).value(null).send();
    }

    // ---------- q:<queryId> keyspace ----------

    QueryEntry getQueryEntry(String queryId) {
        byte[] raw = view.get(QUERY_KEY_PREFIX + queryId);
        return raw == null ? null : decode(raw, QueryEntry.class);
    }

    void putQueryEntry(String queryId, QueryEntry entry) throws PulsarClientException {
        publish(QUERY_KEY_PREFIX + queryId, entry);
    }

    void removeQueryEntry(String queryId) throws PulsarClientException {
        producer.newMessage().key(QUERY_KEY_PREFIX + queryId).value(null).send();
    }

    // ---------- internals ----------

    private void publish(String key, Object value) throws PulsarClientException {
        byte[] payload;
        try {
            payload = MAPPER.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize registry value for key=" + key, e);
        }
        producer.newMessage().key(key).value(payload).send();
        LOG.debug("RegistryStore wrote key={} ({} bytes)", key, payload.length);
    }

    private static <T> T decode(byte[] raw, Class<T> cls) {
        try {
            return MAPPER.readValue(raw, cls);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to deserialize registry value as " + cls.getSimpleName(), e);
        }
    }
}
