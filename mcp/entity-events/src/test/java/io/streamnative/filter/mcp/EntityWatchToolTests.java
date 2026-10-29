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

package io.streamnative.filter.mcp;

import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.FilterableConsumerBuilder;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * {@link EntityWatchTool} against a fake windowed read: a queue of messages behind a mocked consumer.
 * The read is caught up once the queue is empty, so events queued before the first call are the
 * history and events queued after it are live.
 */
class EntityWatchToolTests {

    private static final String TOPIC = "persistent://public/default/session_events";
    private static final String KEY = "8675309";
    private static final EntityQuery.Limits LIMITS =
            new EntityQuery.Limits(Duration.ofHours(24), 200, Duration.ofSeconds(1));
    private static final EntityWatchTool.WatchLimits WATCH_LIMITS =
            new EntityWatchTool.WatchLimits(2, Duration.ofMinutes(5), Duration.ofSeconds(30));
    private static final WindowStats COMPLETE =
            new WindowStats("q1", 1_000L, null, 65_896, 21, 0, null, null, 0, WindowStats.EndedBy.LOG_END,
                    WindowStats.Status.COMPLETE);

    private final LinkedBlockingQueue<Message<byte[]>> log = new LinkedBlockingQueue<>();
    private final List<Consumer<byte[]>> consumers = new ArrayList<>();
    private final MovableClock clock = new MovableClock();
    private volatile boolean historyLoads = true;
    private FilterableConsumerBuilder<byte[]> builder;
    private EntityWatchTool tool;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        Session session = mock(Session.class);
        builder = mock(FilterableConsumerBuilder.class, RETURNS_SELF);
        when(session.newConsumer()).thenReturn(builder);
        when(builder.subscribe()).thenAnswer(inv -> newConsumer());
        tool = new EntityWatchTool(() -> session, TOPIC, LIMITS, WATCH_LIMITS, clock);
    }

    @AfterEach
    void tearDown() {
        tool.close();
    }

    @SuppressWarnings("unchecked")
    private Consumer<byte[]> newConsumer() throws Exception {
        Consumer<byte[]> c = mock(Consumer.class, withSettings().extraInterfaces(WindowControl.class));
        when(c.receive(anyInt(), any(TimeUnit.class)))
                .thenAnswer(inv -> log.poll((int) inv.getArgument(0), inv.getArgument(1)));
        when(((WindowControl) c).isCaughtUp()).thenAnswer(inv -> historyLoads && log.isEmpty());
        when(((WindowControl) c).stats()).thenReturn(COMPLETE);
        consumers.add(c);
        return c;
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> event(String srcMsgId, String type) {
        Message<byte[]> m = mock(Message.class);
        Map<String, String> props = Map.of(WindowProtocol.SRC_MSGID_PROPERTY, srcMsgId, "event_type", type);
        when(m.getProperty(WindowProtocol.SRC_MSGID_PROPERTY)).thenReturn(srcMsgId);
        when(m.getProperties()).thenReturn(props);
        when(m.getKey()).thenReturn(KEY);
        when(m.getData()).thenReturn(("{\"event_type\":\"" + type + "\"}").getBytes(StandardCharsets.UTF_8));
        when(m.getPublishTime()).thenReturn(1_000L);
        return m;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(McpSchema.CallToolResult r) {
        assertThat(r.isError()).as("tool error: %s", r.content()).isNotEqualTo(Boolean.TRUE);
        return (Map<String, Object>) r.structuredContent();
    }

    private static String text(McpSchema.CallToolResult r) {
        return ((McpSchema.TextContent) r.content().get(0)).text();
    }

    @SuppressWarnings("unchecked")
    private static List<String> types(Map<String, Object> result) {
        return ((List<Map<String, Object>>) result.get("events")).stream()
                .map(e -> ((Map<String, String>) e.get("properties")).get("event_type"))
                .toList();
    }

    private String open(Map<String, Object> args) {
        return (String) body(tool.call(args)).get("watch_id");
    }

    @Test
    void theDefinitionNamesTheToolAndTheServerLimits() {
        McpSchema.Tool def = tool.definition();

        assertThat(def.name()).isEqualTo("watch_entity_events");
        assertThat(def.description()).contains(TOPIC).contains("watch_id").contains("TRUNCATED_BY_RETENTION");
        Map<String, Object> schema = def.inputSchema();
        assertThat(schema).doesNotContainKey("required"); // key on the first call, watch_id after
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) schema.get("properties");
        assertThat(properties).containsOnlyKeys("key", "lookback", "max_events", "watch_id", "wait_s", "stop");
        assertThat(properties.get("wait_s").toString()).contains("At most 30");
    }

    @Test
    void theFirstCallReturnsTheHistoryAndAWatchId() {
        log.add(event("1:0:-1", "payment_submitted"));
        log.add(event("1:1:-1", "3ds_timeout"));

        Map<String, Object> r = body(tool.call(Map.of("key", KEY, "lookback", "15m")));

        assertThat(r).containsEntry("watching", true).containsEntry("key", KEY).containsEntry("status", "COMPLETE")
                .containsEntry("matched", 2L).containsEntry("returned", 2);
        assertThat((String) r.get("watch_id")).isNotBlank();
        assertThat((String) r.get("next")).contains((String) r.get("watch_id"));
        assertThat(types(r)).containsExactly("payment_submitted", "3ds_timeout");
        assertThat(tool.openWatches()).isEqualTo(1);
        verify(builder).topic(TOPIC);
        verify(builder).selector("__key__ = '8675309'");
    }

    @Test
    void theHistoryIsCappedAtMaxEventsButCountsEveryMatch() {
        log.add(event("1:0:-1", "a"));
        log.add(event("1:1:-1", "b"));
        log.add(event("1:2:-1", "c"));

        Map<String, Object> r = body(tool.call(Map.of("key", KEY, "max_events", 2)));

        assertThat(r).containsEntry("matched", 3L).containsEntry("returned", 2).containsEntry("more_events", true);
    }

    @Test
    void aLaterCallReturnsOnlyWhatArrivedSinceAndSkipsRedeliveries() throws Exception {
        log.add(event("1:0:-1", "3ds_timeout"));
        String id = open(Map.of("key", KEY));
        log.add(event("1:0:-1", "3ds_timeout")); // at-least-once: the history event again
        log.add(event("1:1:-1", "3ds_approved"));

        Map<String, Object> r = body(tool.call(Map.of("watch_id", id, "wait_s", 5)));

        assertThat(r).containsEntry("watch_id", id).containsEntry("watching", true).containsEntry("new_events", 1);
        assertThat(types(r)).containsExactly("3ds_approved");
        verify(consumers.get(0), times(3)).acknowledge(any(Message.class));
    }

    @Test
    void aBurstComesBackAsOneAnswerWithoutWaitingOutTheWait() {
        String id = open(Map.of("key", KEY));
        log.add(event("1:1:-1", "3ds_approved"));
        log.add(event("1:2:-1", "payment_approved"));
        log.add(event("1:3:-1", "order_confirmed"));

        long start = System.nanoTime();
        Map<String, Object> r = body(tool.call(Map.of("watch_id", id, "wait_s", 20)));

        assertThat(types(r)).containsExactly("3ds_approved", "payment_approved", "order_confirmed");
        assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void aQuietWaitReturnsNoEventsAndSaysToCallAgain() {
        String id = open(Map.of("key", KEY));

        Map<String, Object> r = body(tool.call(Map.of("watch_id", id, "wait_s", "1")));

        assertThat(r).containsEntry("new_events", 0).containsEntry("watching", true);
        assertThat((String) r.get("next")).contains("Nothing new in 1 s");
    }

    @Test
    void stopClosesTheReadAndALaterCallIsAnError() throws Exception {
        log.add(event("1:0:-1", "chat_opened"));
        String id = open(Map.of("key", KEY));

        Map<String, Object> r = body(tool.call(Map.of("watch_id", id, "stop", true)));

        assertThat(r).containsEntry("watching", false).containsEntry("total_events", 1L);
        assertThat(tool.openWatches()).isZero();
        verify(consumers.get(0)).close();
        McpSchema.CallToolResult after = tool.call(Map.of("watch_id", id));
        assertThat(after.isError()).isTrue();
        assertThat(text(after)).contains("no open watch " + id).contains("start a new one with key");
    }

    @Test
    void anIdleWatchIsClosedByTheReaper() throws Exception {
        String id = open(Map.of("key", KEY));
        clock.advance(Duration.ofMinutes(4));
        tool.closeIdle();
        assertThat(tool.openWatches()).isEqualTo(1);

        clock.advance(Duration.ofMinutes(2));
        tool.closeIdle();

        assertThat(tool.openWatches()).isZero();
        verify(consumers.get(0)).close();
        assertThat(text(tool.call(Map.of("watch_id", id)))).contains("closed after 5m without a call");
    }

    @Test
    void theServerHoldsAtMostMaxWatches() throws Exception {
        open(Map.of("key", KEY));
        open(Map.of("key", "5550101"));

        McpSchema.CallToolResult third = tool.call(Map.of("key", "5550102"));

        assertThat(third.isError()).isTrue();
        assertThat(text(third)).contains("already has 2 open watches").contains("stop: true");
        verify(builder, times(2)).subscribe();
    }

    @Test
    void historyThatDoesNotLoadInTimeIsAnErrorAndLeavesNoWatch() throws Exception {
        historyLoads = false;

        McpSchema.CallToolResult r = tool.call(Map.of("key", KEY));

        assertThat(r.isError()).isTrue();
        assertThat(text(r)).contains("didn't load within 1s").contains("shorter lookback");
        assertThat(tool.openWatches()).isZero();
        verify(consumers.get(0)).close();
    }

    @Test
    void anInvalidKeyNeverReachesTheBroker() throws Exception {
        McpSchema.CallToolResult r = tool.call(Map.of("key", "x' OR '1'='1"));

        assertThat(r.isError()).isTrue();
        assertThat(text(r)).contains("key must be");
        verify(builder, never()).subscribe();
    }

    @Test
    void anInvalidWaitIsAToolError() {
        String id = open(Map.of("key", KEY));

        assertThat(text(tool.call(Map.of("watch_id", id, "wait_s", "soon")))).contains("whole number of seconds");
        assertThat(text(tool.call(Map.of("watch_id", id, "wait_s", 0)))).contains("at least 1");
        assertThat(tool.openWatches()).isEqualTo(1);
    }

    @Test
    void closingTheToolClosesEveryWatch() throws Exception {
        open(Map.of("key", KEY));
        open(Map.of("key", "5550101"));

        tool.close();

        assertThat(tool.openWatches()).isZero();
        for (Consumer<byte[]> c : consumers) {
            verify(c).close();
        }
    }

    /** The system clock, plus an offset a test can move forward. */
    private static final class MovableClock extends Clock {
        private final AtomicLong offsetMs = new AtomicLong();

        void advance(Duration d) {
            offsetMs.addAndGet(d.toMillis());
        }

        @Override
        public long millis() {
            return System.currentTimeMillis() + offsetMs.get();
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
