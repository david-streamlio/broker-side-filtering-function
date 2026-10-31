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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.WindowProtocol;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The {@code watch_entity_events} tool (spike): one entity's history, then its new events as they
 * happen, for an agent that keeps helping while things change.
 *
 * <p>The first call opens a live windowed read ({@code __key__ = '<key>'} over
 * {@code TimeWindow.since(lookback)}) and keeps it open on this server: it returns the history up to the
 * caught-up marker, with the window's completeness status, and a {@code watch_id}. Each later call with
 * that {@code watch_id} waits on the same open read for up to {@code wait_s} and returns only the events
 * that arrived since the last call. Nothing is re-scanned between calls; the agent still has to make
 * each call (MCP tools answer when asked), so this is a long poll on a live subscription.
 *
 * <p>Watches close on {@code stop: true}, or after {@code WATCH_IDLE_TIMEOUT} without a call. At most
 * {@code MAX_WATCHES} are open at once: each holds a filter on the server.
 */
final class EntityWatchTool {

    static final String NAME = "watch_entity_events";

    private static final Logger LOG = LoggerFactory.getLogger(EntityWatchTool.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    /** After the first new event, linger this long so a burst comes back as one answer. */
    private static final long LINGER_MS = 400;

    /** Operator settings for watches. */
    record WatchLimits(int maxWatches, Duration idleTimeout, Duration maxWait) {
    }

    private final Supplier<Session> sessions;
    private final String topic;
    private final EntityQuery.Limits limits;
    private final WatchLimits watchLimits;
    private final Clock clock;
    private final Map<String, Watch> watches = new ConcurrentHashMap<>();
    private final ScheduledExecutorService reaper = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "watch-reaper");
        t.setDaemon(true);
        return t;
    });

    EntityWatchTool(Supplier<Session> sessions, String topic, EntityQuery.Limits limits, WatchLimits watchLimits,
                    Clock clock) {
        this.sessions = sessions;
        this.topic = topic;
        this.limits = limits;
        this.watchLimits = watchLimits;
        this.clock = clock;
        reaper.scheduleWithFixedDelay(this::closeIdle, 30, 30, TimeUnit.SECONDS);
    }

    /** The tool as the agent sees it. */
    McpSchema.Tool definition() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("key", Map.of("type", "string",
                "description", "First call: the entity's key, for example a customer or order ID. Letters, digits and . _ : - only."));
        props.put("lookback", Map.of("type", "string", "default", "1h",
                "description", "First call: how much history to return before watching, for example \"15m\". At most "
                        + EntityQuery.format(limits.maxLookback()) + "."));
        props.put("max_events", Map.of("type", "integer", "minimum", 1, "default", EntityQuery.DEFAULT_MAX_EVENTS,
                "description", "The most events to return, listed oldest first. First call: the newest history events "
                        + "(older ones are counted, not returned). Later calls: new events (defaults to the first "
                        + "call's value); any beyond it wait for your next call."));
        props.put("watch_id", Map.of("type", "string",
                "description", "Later calls: the watch_id the first call returned, to get the events since your last call."));
        props.put("wait_s", Map.of("type", "integer", "minimum", 1, "default", 20,
                "description", "Later calls: how long to wait for new events, in seconds. At most "
                        + watchLimits.maxWait().toSeconds() + "."));
        props.put("stop", Map.of("type", "boolean", "default", false,
                "description", "With watch_id: stop watching and free the server's resources."));
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", props);
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(NAME)
                .title("Watch entity events")
                .description("Watches one entity's events in the event log (" + topic + ") while you help: the first "
                        + "call (with key) returns the history and a watch_id; each later call (with watch_id) waits "
                        + "for and returns only the events that happened since your last call. Use it when the "
                        + "situation may change while you work, for example a customer retrying a checkout. Call "
                        + "again with watch_id after each answer to keep watching; stop when done. Times are when "
                        + "the log received each event. If status is TRUNCATED_BY_RETENTION, history may be "
                        + "missing: don't conclude that nothing happened.")
                .inputSchema(schema)
                .build();
    }

    /** Handle one call: invalid arguments and failures come back as tool errors the agent can read. */
    McpSchema.CallToolResult call(Map<String, Object> args) {
        Object id = args == null ? null : args.get("watch_id");
        try {
            Map<String, Object> result = id == null ? open(args) : next(String.valueOf(id), args);
            return McpSchema.CallToolResult.builder()
                    .addTextContent(JSON.writeValueAsString(result))
                    .structuredContent(result)
                    .build();
        } catch (IllegalArgumentException | IllegalStateException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            LOG.warn("{} failed", NAME, e);
            return error("couldn't watch: " + e.getMessage());
        }
    }

    /** First call: open the live read, return the history up to the caught-up marker. */
    private Map<String, Object> open(Map<String, Object> args) throws Exception {
        EntityQuery query = EntityQuery.parse(args, limits);
        if (watches.size() >= watchLimits.maxWatches()) {
            throw new IllegalStateException("this server already has " + watches.size()
                    + " open watches (its limit); stop one with {watch_id, stop: true} first");
        }
        long start = clock.millis();
        long deadline = start + limits.timeout().toMillis();
        Consumer<byte[]> consumer = sessions.get().newConsumer()
                .topic(topic)
                .selector(query.selector())
                .window(TimeWindow.since(query.lookback()))
                .subscribe();
        Watch w = new Watch(UUID.randomUUID().toString().substring(0, 8), query, consumer);
        List<Map<String, Object>> events = new ArrayList<>();
        try {
            WindowControl window = (WindowControl) consumer;
            while (!window.isCaughtUp()) {
                if (clock.millis() > deadline) {
                    throw new IllegalStateException("the history didn't load within " + EntityQuery.format(limits.timeout())
                            + "; try a shorter lookback");
                }
                Message<byte[]> m = consumer.receive(500, TimeUnit.MILLISECONDS);
                if (m != null) {
                    w.take(m, events, query.maxEvents());
                }
            }
        } catch (Exception e) {
            w.close();
            throw e;
        }
        w.touch(clock.millis());
        watches.put(w.id, w);
        Map<String, Object> r = EntityEventsTool.result(query, topic, ((WindowControl) consumer).stats(), events,
                w.matched, clock.millis() - start);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("watch_id", w.id);
        out.put("watching", true);
        out.putAll(r);
        out.put("next", "Call watch_entity_events with {\"watch_id\": \"" + w.id
                + "\"} to get this entity's new events as they happen.");
        LOG.info("{} {} key={} lookback={}: {} history events, status {}", NAME, w.id, query.key(),
                EntityQuery.format(query.lookback()), w.matched, r.get("status"));
        return out;
    }

    /** Later calls: wait on the open read for new events. */
    private Map<String, Object> next(String id, Map<String, Object> args) throws Exception {
        Watch w = watches.get(id);
        if (w == null) {
            throw new IllegalArgumentException("no open watch " + id + " (it was stopped, or closed after "
                    + EntityQuery.format(watchLimits.idleTimeout()) + " without a call); start a new one with key");
        }
        if (Boolean.TRUE.equals(args.get("stop"))) {
            watches.remove(id);
            w.close();
            LOG.info("{} {} stopped after {} events", NAME, id, w.matched);
            return Map.of("watch_id", id, "watching", false, "total_events", w.matched);
        }
        long waitMs = Math.min(parseWait(args.get("wait_s")), watchLimits.maxWait().toSeconds()) * 1000L;
        Object rawMax = args.get("max_events");
        int max = rawMax == null ? w.query.maxEvents() : EntityQuery.parseCount(rawMax);
        List<Map<String, Object>> events = new ArrayList<>();
        synchronized (w) {
            long until = clock.millis() + waitMs;
            // Stop reading at max: the rest stay unread (not acked) and come back on the next call.
            while (clock.millis() < until && events.size() < max) {
                Message<byte[]> m = w.consumer.receive((int) Math.max(1, Math.min(500, until - clock.millis())),
                        TimeUnit.MILLISECONDS);
                if (m != null) {
                    w.take(m, events, max);
                    until = Math.min(until, clock.millis() + LINGER_MS); // gather the rest of a burst, then answer
                }
            }
            w.touch(clock.millis());
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("watch_id", id);
        out.put("watching", true);
        out.put("key", w.query.key());
        boolean more = events.size() >= max;
        out.put("new_events", events.size());
        out.put("more_events", more);
        out.put("events", events);
        out.put("next", events.isEmpty()
                ? "Nothing new in " + waitMs / 1000 + " s. Call again with the same watch_id to keep watching."
                : more ? "More events are waiting: call again with the same watch_id to get them."
                : "Call again with the same watch_id to keep watching.");
        LOG.info("{} {}: {} new events", NAME, id, events.size());
        return out;
    }

    /** Close watches idle for longer than {@code WATCH_IDLE_TIMEOUT}; runs every 30 s (package-private for tests). */
    void closeIdle() {
        long now = clock.millis();
        watches.values().removeIf(w -> {
            if (now - w.lastUsed > watchLimits.idleTimeout().toMillis()) {
                LOG.info("{} {} closed after {} idle", NAME, w.id, EntityQuery.format(watchLimits.idleTimeout()));
                w.close();
                return true;
            }
            return false;
        });
    }

    /** Close every watch (server shutdown). */
    void close() {
        reaper.shutdownNow();
        watches.values().forEach(Watch::close);
        watches.clear();
    }

    int openWatches() {
        return watches.size();
    }

    private static int parseWait(Object raw) {
        if (raw == null) {
            return 20;
        }
        try {
            int s = raw instanceof Number n ? n.intValue() : Integer.parseInt(String.valueOf(raw).trim());
            if (s < 1) {
                throw new IllegalArgumentException("wait_s must be at least 1");
            }
            return s;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("wait_s must be a whole number of seconds (got \"" + raw + "\")");
        }
    }

    private static McpSchema.CallToolResult error(String message) {
        return McpSchema.CallToolResult.builder().addTextContent(message).isError(true).build();
    }

    /** One open live read. */
    private static final class Watch {
        final String id;
        final EntityQuery query;
        final Consumer<byte[]> consumer;
        final Set<String> seen = new HashSet<>();
        volatile long lastUsed;
        long matched;

        Watch(String id, EntityQuery query, Consumer<byte[]> consumer) {
            this.id = id;
            this.query = query;
            this.consumer = consumer;
        }

        /**
         * Add a message to events if it's new (delivery is at-least-once), keeping the newest {@code max};
         * ack it. Later calls stop reading at max, so only the history ever lets older events go.
         */
        void take(Message<byte[]> m, List<Map<String, Object>> events, int max) throws Exception {
            String sourceId = m.getProperty(WindowProtocol.SRC_MSGID_PROPERTY);
            if (seen.add(sourceId != null ? sourceId : m.getMessageId().toString())) {
                matched++;
                EntityEventsTool.keepNewest(events,
                        EntityEventsTool.event(m.getKey(), m.getProperties(), m.getData(), m.getPublishTime()), max);
            }
            consumer.acknowledge(m);
        }

        void touch(long now) {
            lastUsed = now;
        }

        void close() {
            try {
                consumer.close();
            } catch (Exception e) {
                LOG.debug("closing watch {}", id, e);
            }
        }
    }
}
