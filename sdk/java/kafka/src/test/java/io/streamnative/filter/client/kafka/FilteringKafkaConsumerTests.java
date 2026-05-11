package io.streamnative.filter.client.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.client.FilterControl;
import io.streamnative.filter.client.kafka.KafkaTestFixtures.Mocks;
import io.streamnative.filter.common.QueryMessage;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class FilteringKafkaConsumerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private Mocks mocks;
    private Properties props;
    private Consumer<String, byte[]> underlying;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() throws Exception {
        mocks = KafkaTestFixtures.newSessionWithMocks();
        props = KafkaTestFixtures.stubKafkaProps();
        underlying = (Consumer<String, byte[]>) mock(Consumer.class);
    }

    private Consumer<String, byte[]> subscribe(String selector) throws Exception {
        return FilterableKafkaConsumers.<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topic("persistent://public/in/trades")
                .selector(selector)
                .subscribe();
    }

    // ------------------------------------------------------------- Delegation

    @Test
    void pollDurationDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        @SuppressWarnings("unchecked")
        ConsumerRecords<String, byte[]> recs = (ConsumerRecords<String, byte[]>) mock(ConsumerRecords.class);
        org.mockito.Mockito.when(underlying.poll(Duration.ofSeconds(5))).thenReturn(recs);
        assertThat(c.poll(Duration.ofSeconds(5))).isSameAs(recs);
        verify(underlying).poll(Duration.ofSeconds(5));
    }

    @Test
    void commitSyncDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        c.commitSync();
        verify(underlying).commitSync();
    }

    @Test
    void commitAsyncDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        c.commitAsync();
        verify(underlying).commitAsync();
    }

    @Test
    void seekDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        TopicPartition tp = new TopicPartition("t", 0);
        c.seek(tp, 42L);
        verify(underlying).seek(tp, 42L);
    }

    @Test
    void pauseResumeDelegate() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        TopicPartition tp = new TopicPartition("t", 0);
        c.pause(List.of(tp));
        c.resume(List.of(tp));
        verify(underlying).pause(List.of(tp));
        verify(underlying).resume(List.of(tp));
    }

    @Test
    void wakeupDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        c.wakeup();
        verify(underlying).wakeup();
    }

    @Test
    void positionDelegates() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        TopicPartition tp = new TopicPartition("t", 0);
        org.mockito.Mockito.when(underlying.position(tp)).thenReturn(99L);
        assertThat(c.position(tp)).isEqualTo(99L);
        verify(underlying).position(tp);
    }

    // ------------------------------------------------------------- Frozen subscription

    @Test
    void subscribeCollectionThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        assertThatThrownBy(() -> c.subscribe(List.of("other")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("fixed at build time");
    }

    @Test
    void subscribePatternThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        assertThatThrownBy(() -> c.subscribe(Pattern.compile("foo.*")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void subscribeWithListenerThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        ConsumerRebalanceListener listener = mock(ConsumerRebalanceListener.class);
        assertThatThrownBy(() -> c.subscribe(List.of("other"), listener))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void assignThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        assertThatThrownBy(() -> c.assign(List.of(new TopicPartition("t", 0))))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void unsubscribeThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        assertThatThrownBy(c::unsubscribe)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("close()");
    }

    // ------------------------------------------------------------- FilterControl

    @Test
    void filterControlInitialStateAfterSubscribe() throws Exception {
        Consumer<String, byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        assertThat(fc.currentSelector()).isEqualTo("region = 'US'");
        assertThat(fc.queryId()).isNotBlank();
        assertThat(fc.privateTopic()).startsWith("persistent://public/default/filtered-");
        assertThat(fc.sourceTopics()).containsExactly("persistent://public/in/trades");
    }

    @Test
    void setSelectorRepublishesOnSameQueryIdAndComposesTopicConstraint() throws Exception {
        Consumer<String, byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        String initialQueryId = fc.queryId();

        // Initial publish from subscribe()
        ArgumentCaptor<byte[]> firstPayload = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder).value(firstPayload.capture());
        verify(mocks.controlMsgBuilder).key(initialQueryId);
        QueryMessage initialQm = MAPPER.readValue(firstPayload.getValue(), QueryMessage.class);
        assertThat(initialQm.getSelector())
                .isEqualTo("__topic__ = 'persistent://public/in/trades' AND (region = 'US')");

        // setSelector republishes on SAME queryId
        fc.setSelector("region = 'UK'");
        assertThat(fc.queryId()).isEqualTo(initialQueryId);
        assertThat(fc.currentSelector()).isEqualTo("region = 'UK'");

        ArgumentCaptor<byte[]> all = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder, times(2)).value(all.capture());
        verify(mocks.controlMsgBuilder, times(2)).key(initialQueryId);
        QueryMessage updated = MAPPER.readValue(all.getAllValues().get(1), QueryMessage.class);
        assertThat(updated.getSelector())
                .isEqualTo("__topic__ = 'persistent://public/in/trades' AND (region = 'UK')");
        assertThat(updated.getOutputTopic()).isEqualTo(fc.privateTopic());
    }

    @Test
    void setSelectorNullPublishesTopicConstraintOnly() throws Exception {
        Consumer<String, byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        fc.setSelector(null);
        assertThat(fc.currentSelector()).isNull();

        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder, times(2)).value(payload.capture());
        QueryMessage updated = MAPPER.readValue(payload.getAllValues().get(1), QueryMessage.class);
        assertThat(updated.getSelector()).isEqualTo("__topic__ = 'persistent://public/in/trades'");
    }

    @Test
    void setSelectorMalformedThrowsAndDoesNotRepublish() throws Exception {
        Consumer<String, byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        verify(mocks.controlMsgBuilder, times(1)).value(any(byte[].class));

        assertThatThrownBy(() -> fc.setSelector("THIS @@## INVALID"))
                .isInstanceOf(IllegalArgumentException.class);

        verify(mocks.controlMsgBuilder, times(1)).value(any(byte[].class));
        assertThat(fc.currentSelector()).isEqualTo("region = 'US'"); // unchanged
    }

    @Test
    void setSelectorAfterCloseThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe("region = 'US'");
        FilterControl fc = (FilterControl) c;
        c.close();

        assertThatThrownBy(() -> fc.setSelector("region = 'UK'"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    // ------------------------------------------------------------- Close

    @Test
    void closeClosesUnderlyingAndRetractsQuery() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        FilterControl fc = (FilterControl) c;

        c.close();

        verify(underlying).close();
        verify(mocks.controlMsgBuilder).value(isNull()); // tombstone
        verify(mocks.topics).delete(org.mockito.ArgumentMatchers.eq(fc.privateTopic()),
                org.mockito.ArgumentMatchers.eq(true));
    }

    @Test
    void closeWithTimeoutDelegatesAndRetracts() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        c.close(Duration.ofSeconds(3));

        verify(underlying).close(Duration.ofSeconds(3));
        verify(mocks.controlMsgBuilder).value(isNull()); // tombstone
    }

    @Test
    void closeIsIdempotent() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        c.close();
        c.close();
        verify(underlying, times(1)).close();
        verify(mocks.topics, times(1)).delete(anyString(), anyBoolean());
    }

    @Test
    void closeStillRetractsWhenUnderlyingCloseThrows() throws Exception {
        Consumer<String, byte[]> c = subscribe(null);
        FilterControl fc = (FilterControl) c;
        RuntimeException boom = new RuntimeException("kafka close failed");
        org.mockito.Mockito.doThrow(boom).when(underlying).close();

        assertThatThrownBy(c::close).isSameAs(boom);
        verify(mocks.controlMsgBuilder).value(isNull()); // tombstone still sent
        verify(mocks.topics).delete(org.mockito.ArgumentMatchers.eq(fc.privateTopic()),
                org.mockito.ArgumentMatchers.eq(true));
    }
}
