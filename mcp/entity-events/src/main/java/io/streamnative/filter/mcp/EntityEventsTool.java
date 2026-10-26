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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.api.TimeWindow;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.client.Session;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The {@code get_entity_events} tool: one entity's events over a recent time window, read
 * server-side with a windowed consumer ({@code __key__ = '<key>'} over {@code TimeWindow.last(...)}),
 * so only that entity's events reach the server this runs on.
 *
 * <p>It answers at the window's completion record, never on an empty receive, and passes the
 * server's status through with what it means for the agent. Matches are deduplicated on
 * {@code __src_msgid__}: delivery is at-least-once.
 */
final class EntityEventsTool {

    static final String NAME = "get_entity_events";

    private static final Logger LOG = LoggerFactory.getLogger(EntityEventsTool.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Supplier<Session> sessions;
    private final String topic;
    private final EntityQuery.Limits limits;
    private final Clock clock;

    EntityEventsTool(Supplier<Session> sessions, String topic, EntityQuery.Limits limits, Clock clock) {
        this.sessions = sessions;
        this.topic = topic;
        this.limits = limits;
        this.clock = clock;
    }

    /** The tool as the agent sees it. */
    McpSchema.Tool definition() {
        Map<String, Object> key = Map.of("type", "string",
                "description", "The entity's key, for example an order ID. Letters, digits and . _ : - only.");
        Map<String, Object> lookback = Map.of("type", "string", "default", "1h",
                "description", "How far back to read: for example \"15m\", \"6h\" or \"24h\". At most "
                        + EntityQuery.format(limits.maxLookback()) + ".");
        Map<String, Object> maxEvents = Map.of("type", "integer", "minimum", 1, "default", EntityQuery.DEFAULT_MAX_EVENTS,
                "description", "The most events to return, oldest first. At most " + limits.maxEvents() + ".");
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of("key", key, "lookback", lookback, "max_events", maxEvents));
        schema.put("required", List.of("key"));
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(NAME)
                .title("Entity events")
                .description("Returns one entity's events over a recent time window, from the event log (" + topic
                        + "), with a completeness status. The window is read and filtered on the server, so only "
                        + "this entity's events are returned. Use it to answer \"what happened to <entity> in the "
                        + "last <time>?\". Times are when the log received each event. If status is "
                        + "TRUNCATED_BY_RETENTION, events may be missing: don't conclude that nothing happened.")
                .inputSchema(schema)
                .build();
    }

    /** Handle one call: invalid arguments and failures come back as tool errors the agent can read. */
    McpSchema.CallToolResult call(Map<String, Object> args) {
        EntityQuery query;
        try {
            query = EntityQuery.parse(args, limits);
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        }
        try {
            Map<String, Object> result = read(query);
            LOG.info("{} key={} lookback={}: {} matched, status {}", NAME, query.key(),
                    EntityQuery.format(query.lookback()), result.get("matched"), result.get("status"));
            return McpSchema.CallToolResult.builder()
                    .addTextContent(JSON.writeValueAsString(result))
                    .structuredContent(result)
                    .build();
        } catch (WindowTimeoutException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            LOG.warn("{} key={} failed", NAME, query.key(), e);
            return error("couldn't read the window: " + e.getMessage());
        }
    }

    private Map<String, Object> read(EntityQuery query) throws Exception {
        long start = clock.millis();
        long deadline = start + limits.timeout().toMillis();
        List<Map<String, Object>> events = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        try (Consumer<byte[]> consumer = sessions.get().newConsumer()
                .topic(topic)
                .selector(query.selector())
                .window(TimeWindow.last(query.lookback()))
                .subscribe()) {
            WindowControl window = (WindowControl) consumer;
            while (!window.isComplete()) {
                if (clock.millis() > deadline) {
                    throw new WindowTimeoutException("the window didn't complete within "
                            + EntityQuery.format(limits.timeout()) + " (" + seen.size()
                            + " matching events so far); try a shorter lookback");
                }
                Message<byte[]> m = consumer.receive(500, TimeUnit.MILLISECONDS);
                if (m == null) {
                    continue; // nothing yet; only the completion record means done
                }
                String sourceId = m.getProperty(WindowProtocol.SRC_MSGID_PROPERTY);
                if (seen.add(sourceId != null ? sourceId : m.getMessageId().toString()) && events.size() < query.maxEvents()) {
                    events.add(event(m.getKey(), m.getProperties(), m.getData(), m.getPublishTime()));
                }
                consumer.acknowledge(m);
            }
            return result(query, topic, window.stats(), events, seen.size(), clock.millis() - start);
        }
    }

    /** The tool's result: the window, its status and what it means, the counts, and the events. */
    static Map<String, Object> result(EntityQuery query, String topic, WindowStats stats,
                                      List<Map<String, Object>> events, long matched, long elapsedMs) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("key", query.key());
        r.put("topic", topic);
        String status = stats == null ? "UNKNOWN" : stats.getStatus().name();
        String logStart = stats == null || stats.getFirstScannedMs() == null ? null
                : Instant.ofEpochMilli(stats.getFirstScannedMs()).toString();
        r.put("status", status);
        r.put("status_meaning", switch (status) {
            case "COMPLETE" -> "Every event in the window was read: an empty list means this entity had no events in it.";
            // The server can't tell retention from a topic that is newer than the window: say both.
            case "TRUNCATED_BY_RETENTION" -> "The window starts before the oldest event the log holds"
                    + (logStart != null ? " (" + logStart + ")" : " (it holds none)")
                    + ": either retention removed older events, or the topic is newer than the window. Events "
                    + "from before that point may be missing, so don't conclude that nothing happened then.";
            default -> "The window completed, but its stats couldn't be read: treat the result as possibly incomplete.";
        });
        if (stats != null) {
            Map<String, Object> window = new LinkedHashMap<>();
            window.put("from", Instant.ofEpochMilli(stats.getWindowFromMs()).toString());
            if (stats.getWindowToMs() != null) {
                window.put("to", Instant.ofEpochMilli(stats.getWindowToMs()).toString());
            }
            r.put("window", window);
            r.put("scanned", stats.getScanned());
        }
        r.put("matched", matched);
        r.put("returned", events.size());
        r.put("more_events", matched > events.size());
        r.put("elapsed_ms", elapsedMs);
        r.put("events", events);
        return r;
    }

    /**
     * One event: when the log received it, its key, its own properties (not the filter's {@code __}
     * ones), and its value: parsed JSON when it is JSON, else text.
     */
    static Map<String, Object> event(String key, Map<String, String> properties, byte[] value, long publishTime) {
        Map<String, Object> e = new LinkedHashMap<>();
        String logTime = properties.get(WindowProtocol.SRC_PUBLISH_TIME_PROPERTY);
        e.put("time", Instant.ofEpochMilli(logTime != null ? Long.parseLong(logTime) : publishTime).toString());
        e.put("key", key);
        Map<String, String> own = new TreeMap<>();
        properties.forEach((k, v) -> {
            if (!k.startsWith("__")) {
                own.put(k, v);
            }
        });
        e.put("properties", own);
        e.put("value", value(value));
        return e;
    }

    private static Object value(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        String text = new String(bytes, StandardCharsets.UTF_8);
        try {
            JsonNode node = JSON.readTree(text);
            return node != null && (node.isObject() || node.isArray()) ? node : text;
        } catch (JsonProcessingException e) {
            return text;
        }
    }

    private static McpSchema.CallToolResult error(String message) {
        return McpSchema.CallToolResult.builder().addTextContent(message).isError(true).build();
    }

    /** The window didn't complete within the server's per-call timeout. */
    static final class WindowTimeoutException extends Exception {
        WindowTimeoutException(String message) {
            super(message);
        }
    }
}
