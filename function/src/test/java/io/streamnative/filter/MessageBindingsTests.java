package io.streamnative.filter;

import org.apache.pulsar.functions.api.Record;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MessageBindingsTests {

    @Test
    void resolvesKey() {
        MessageBindings b = new MessageBindings("AAPL", "persistent://t/n/trades", Map.of());
        assertThat(b.lookup(MessageBindings.KEY_BINDING)).isEqualTo("AAPL");
    }

    @Test
    void resolvesTopic() {
        MessageBindings b = new MessageBindings(null, "persistent://t/n/trades", Map.of());
        assertThat(b.lookup(MessageBindings.TOPIC_BINDING)).isEqualTo("persistent://t/n/trades");
    }

    @Test
    void resolvesUserProperty() {
        MessageBindings b = new MessageBindings(null, null, Map.of("region", "US"));
        assertThat(b.lookup("region")).isEqualTo("US");
    }

    @Test
    void unknownPropertyReturnsNull() {
        MessageBindings b = new MessageBindings(null, null, Map.of("region", "US"));
        assertThat(b.lookup("country")).isNull();
    }

    @Test
    void unknownPseudoHeaderReturnsNull() {
        // We do not surface JMS pseudo-headers as bindings. If a property happens to have a
        // JMS-prefixed name, it is looked up like any other user property (no special handling).
        MessageBindings withProp = new MessageBindings("k", "t", Map.of("JMSMessageID", "1"));
        assertThat(withProp.lookup("JMSMessageID")).isEqualTo("1");

        MessageBindings withoutProp = new MessageBindings("k", "t", Map.of());
        assertThat(withoutProp.lookup("JMSMessageID")).isNull();
        assertThat(withoutProp.lookup("JMSPriority")).isNull();
    }

    @Test
    void nullPropertiesMapTolerated() {
        MessageBindings b = new MessageBindings("k", "t", null);
        assertThat(b.getProperties()).isEmpty();
        assertThat(b.lookup("anything")).isNull();
    }

    @Test
    void fromRecordStripsPartitionSuffix() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.of("AAPL"));
        Mockito.when(record.getTopicName()).thenReturn(Optional.of("persistent://t/n/trades-partition-2"));
        Mockito.when(record.getProperties()).thenReturn(Map.of("region", "US"));

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isEqualTo("AAPL");
        assertThat(b.getTopic()).isEqualTo("persistent://t/n/trades");
        assertThat(b.getProperties()).containsEntry("region", "US");
    }

    @Test
    void fromRecordWithMissingKeyAndTopic() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.empty());
        Mockito.when(record.getTopicName()).thenReturn(Optional.empty());
        Mockito.when(record.getProperties()).thenReturn(null);

        MessageBindings b = MessageBindings.from(record);
        assertThat(b.getKey()).isNull();
        assertThat(b.getTopic()).isNull();
        assertThat(b.getProperties()).isEmpty();
    }

    @Test
    void fromRecordIsLazyForKeyOnlyAccess() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getKey()).thenReturn(Optional.of("AAPL"));

        MessageBindings b = MessageBindings.from(record);
        Object resolvedKey = b.lookup(MessageBindings.KEY_BINDING);

        assertThat(resolvedKey).isEqualTo("AAPL");
        assertThat(b.keyResolved()).isTrue();
        assertThat(b.topicResolved()).isFalse();
        assertThat(b.propertiesResolved()).isFalse();
        // getTopicName() / getProperties() were never invoked on the record.
        Mockito.verify(record, Mockito.never()).getTopicName();
        Mockito.verify(record, Mockito.never()).getProperties();
    }

    @Test
    void fromRecordResolvesTopicLazily() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getTopicName()).thenReturn(Optional.of("persistent://t/n/x-partition-3"));

        MessageBindings b = MessageBindings.from(record);
        Object topic = b.lookup(MessageBindings.TOPIC_BINDING);

        assertThat(topic).isEqualTo("persistent://t/n/x");
        assertThat(b.topicResolved()).isTrue();
        assertThat(b.propertiesResolved()).isFalse();
        Mockito.verify(record, Mockito.never()).getProperties();
    }

    @Test
    void fromRecordResolvesPropertiesOnDemandOnly() {
        Record<?> record = Mockito.mock(Record.class);
        Mockito.when(record.getProperties()).thenReturn(Map.of("region", "US"));

        MessageBindings b = MessageBindings.from(record);
        Object value = b.lookup("region");

        assertThat(value).isEqualTo("US");
        assertThat(b.propertiesResolved()).isTrue();
        assertThat(b.topicResolved()).isFalse();
        Mockito.verify(record, Mockito.never()).getTopicName();
    }
}
