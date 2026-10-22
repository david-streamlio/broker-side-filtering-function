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

package io.streamnative.filter.client.kafka;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.api.FilterControl;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.kafka.KafkaTestFixtures.Mocks;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.WindowProtocol;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class WindowedKafkaConsumerTests {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final String SRC = "persistent://public/in/orders";
    private static final TopicPartition TP = new TopicPartition("filtered-x", 0);
    private static final String STATS = "{\"queryId\":\"q\",\"windowFromMs\":1,\"windowToMs\":2,\"scanned\":10,"
            + "\"matched\":3,\"bytesScanned\":100,\"elapsedMs\":5,\"endedBy\":\"LOG_END\",\"status\":\"COMPLETE\"}";

    private Mocks mocks;
    private Properties props;
    private Consumer<String, byte[]> underlying;
    private Properties propsSeenByFactory;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setup() throws Exception {
        mocks = KafkaTestFixtures.newSessionWithMocks();
        props = KafkaTestFixtures.stubKafkaProps();
        underlying = mock(Consumer.class);
    }

    private Consumer<String, byte[]> subscribe(TimeWindow window) throws Exception {
        return FilterableKafkaConsumers.<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> {
                    propsSeenByFactory = p;
                    return underlying;
                })
                .topic(SRC)
                .selector("__key__ = '8675309'")
                .window(window)
                .subscribe();
    }

    private List<FilterRequest> published(int count) throws Exception {
        ArgumentCaptor<byte[]> payload = ArgumentCaptor.forClass(byte[].class);
        verify(mocks.controlMsgBuilder, times(count)).value(payload.capture());
        return payload.getAllValues().stream().map(b -> {
            try { return MAPPER.readValue(b, FilterRequest.class); }
            catch (Exception e) { throw new RuntimeException(e); }
        }).toList();
    }

    private static ConsumerRecord<String, byte[]> record(long offset, String value) {
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, 0L, TimestampType.CREATE_TIME, 0, 0,
                "k", value.getBytes(StandardCharsets.UTF_8), new RecordHeaders(), java.util.Optional.empty());
    }

    /** A marker as KoP delivers it: null value, stats in a header. */
    private static ConsumerRecord<String, byte[]> marker(long offset, String markerHeader, String statsJson) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(markerHeader, "true".getBytes(StandardCharsets.UTF_8));
        headers.add(WindowProtocol.STATS_PROPERTY, statsJson.getBytes(StandardCharsets.UTF_8));
        return new ConsumerRecord<>(TP.topic(), TP.partition(), offset, 0L, TimestampType.CREATE_TIME, 0, 0,
                "k", null, headers, java.util.Optional.empty());
    }

    private static ConsumerRecords<String, byte[]> records(ConsumerRecord<String, byte[]>... rs) {
        return new ConsumerRecords<>(Map.of(TP, List.of(rs)));
    }

    @Test
    void boundedWindowRegistersAWindowedQueryAndReadsFromTheStart() throws Exception {
        props.put("auto.offset.reset", "latest");
        Consumer<String, byte[]> c = subscribe(TimeWindow.last(1, TimeUnit.HOURS));

        FilterRequest req = published(1).get(0);
        assertThat(req.isWindowed()).isTrue();
        assertThat(req.getWindowToMs()).isNotNull();
        assertThat(propsSeenByFactory.getProperty("auto.offset.reset")).isEqualTo("earliest");
        assertThat(props.getProperty("auto.offset.reset")).isEqualTo("latest");   // caller's props untouched
        assertThat(((WindowControl) c).isOpenEnded()).isFalse();
    }

    @Test
    void pollDropsTheCompletionRecordAndReportsCompletion() throws Exception {
        Consumer<String, byte[]> c = subscribe(TimeWindow.last(1, TimeUnit.HOURS));
        when(underlying.poll(any(Duration.class))).thenReturn(records(
                record(0, "r1"), record(1, "r2"), marker(2, WindowProtocol.COMPLETE_PROPERTY, STATS)));

        ConsumerRecords<String, byte[]> got = c.poll(Duration.ofSeconds(1));

        assertThat(got.count()).isEqualTo(2);
        assertThat(got.records(TP)).extracting(ConsumerRecord::offset).containsExactly(0L, 1L);
        WindowControl window = (WindowControl) c;
        assertThat(window.isComplete()).isTrue();
        assertThat(window.stats().getMatched()).isEqualTo(3L);
    }

    @Test
    void emptyPollIsNotCompletion() throws Exception {
        Consumer<String, byte[]> c = subscribe(TimeWindow.last(1, TimeUnit.HOURS));
        when(underlying.poll(any(Duration.class))).thenReturn(ConsumerRecords.empty());

        assertThat(c.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        assertThat(((WindowControl) c).isComplete()).isFalse();
    }

    @Test
    void openEndedWindowReportsCaughtUpAndKeepsLiveRecords() throws Exception {
        Consumer<String, byte[]> c = subscribe(TimeWindow.since(1, TimeUnit.HOURS));
        when(underlying.poll(any(Duration.class))).thenReturn(records(
                record(0, "history"), marker(1, WindowProtocol.CAUGHT_UP_PROPERTY, STATS), record(2, "live")));

        ConsumerRecords<String, byte[]> got = c.poll(Duration.ofSeconds(1));

        assertThat(published(1).get(0).getWindowToMs()).isNull();
        assertThat(got.records(TP)).extracting(ConsumerRecord::offset).containsExactly(0L, 2L);
        WindowControl window = (WindowControl) c;
        assertThat(window.isCaughtUp()).isTrue();
        assertThat(window.isComplete()).isFalse();
    }

    @Test
    void unreadableStatsStillCompleteTheWindow() throws Exception {
        Consumer<String, byte[]> c = subscribe(TimeWindow.last(1, TimeUnit.HOURS));
        when(underlying.poll(any(Duration.class))).thenReturn(records(
                marker(0, WindowProtocol.COMPLETE_PROPERTY, "not json")));

        assertThat(c.poll(Duration.ofSeconds(1)).isEmpty()).isTrue();
        assertThat(((WindowControl) c).isComplete()).isTrue();
        assertThat(((WindowControl) c).stats()).isNull();
    }

    @Test
    void windowedSelectorIsFixedButLiveOnlyKeepsSetSelector() throws Exception {
        Consumer<String, byte[]> windowed = subscribe(TimeWindow.last(1, TimeUnit.HOURS));
        assertThatThrownBy(() -> ((FilterControl) windowed).setSelector("x = 1"))
                .isInstanceOf(UnsupportedOperationException.class);

        Consumer<String, byte[]> live = subscribe(TimeWindow.liveOnly());
        ((FilterControl) live).setSelector("x = 1");   // publishes a MODIFY
        assertThat(published(3).get(1).isWindowed()).isFalse();
    }

    @Test
    void liveOnlyIsALiveQueryCaughtUpFromTheStart() throws Exception {
        props.put("auto.offset.reset", "latest");
        Consumer<String, byte[]> c = subscribe(TimeWindow.liveOnly());

        assertThat(published(1).get(0).isWindowed()).isFalse();
        assertThat(propsSeenByFactory.getProperty("auto.offset.reset")).isEqualTo("latest");
        WindowControl window = (WindowControl) c;
        assertThat(window.isCaughtUp()).isTrue();
        assertThat(window.isComplete()).isFalse();
    }

    @Test
    void windowReadingHistoryNeedsExactlyOneTopic() {
        assertThatThrownBy(() -> FilterableKafkaConsumers.<String, byte[]>newConsumer(mocks.session, props)
                .withKafkaConsumerFactory(p -> underlying)
                .topics(List.of(SRC, "persistent://public/in/other"))
                .window(TimeWindow.all())
                .subscribe())
                .isInstanceOf(IllegalStateException.class);
    }
}
