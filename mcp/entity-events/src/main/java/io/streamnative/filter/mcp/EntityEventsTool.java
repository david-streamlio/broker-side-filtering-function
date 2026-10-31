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
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * The {@code get_entity_events} tool: the events over a recent time window that match a key, a JMS
 * selector, or both, read server-side with a windowed consumer ({@code __key__ = '<key>' AND (<selector>)}
 * over {@code TimeWindow.last(...)}) on one of the allowed topics, so only matching events reach the
 * server this runs on. The selector is validated against the topic's fields ({@code get_schema})
 * before any filter starts.
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
    private final Topics topics;
    private final Function<String, FieldCatalog.Fields> fields;
    private final EntityQuery.Limits limits;
    private final Clock clock;

    /** Fields known by name only: selectors are checked against the names, with no type hints. */
    EntityEventsTool(Supplier<Session> sessions, Topics topics, Function<String, Set<String>> fieldNames,
                     EntityQuery.Limits limits, Clock clock) {
        this(sessions, topics, limits, clock, t -> FieldCatalog.Fields.ofNames(t, fieldNames.apply(t)));
    }

    /** Fields from the catalog: selectors are checked against them, and comparisons that can't match are flagged. */
    EntityEventsTool(Supplier<Session> sessions, Topics topics, FieldCatalog catalog, EntityQuery.Limits limits,
                     Clock clock) {
        this(sessions, topics, limits, clock, catalog::fields);
    }

    private EntityEventsTool(Supplier<Session> sessions, Topics topics, EntityQuery.Limits limits, Clock clock,
                             Function<String, FieldCatalog.Fields> fields) {
        this.sessions = sessions;
        this.topics = topics;
        this.fields = fields;
        this.limits = limits;
        this.clock = clock;
    }

    /** One topic and no known fields: key-only calls, as before selectors. */
    EntityEventsTool(Supplier<Session> sessions, String topic, EntityQuery.Limits limits, Clock clock) {
        this(sessions, Topics.of(topic), t -> Set.of(), limits, clock);
    }

    /** The {@code topic} argument: the allowed short names, defaulting to the first. */
    static Map<String, Object> topicProperty(Topics topics, String forWhichCall) {
        return Map.of("type", "string", "default", Topics.shortName(topics.first()),
                "description", forWhichCall + "the topic to read: one of "
                        + String.join(", ", topics.all().stream().map(Topics::shortName).toList())
                        + " (the full topic name works too). Defaults to " + Topics.shortName(topics.first()) + ".");
    }

    /** The {@code selector} argument. */
    static Map<String, Object> selectorProperty(String forWhichCall) {
        return Map.of("type", "string", "maxLength", EntityQuery.MAX_SELECTOR_LENGTH,
                "description", forWhichCall + "a JMS message selector (SQL-92 WHERE syntax: = <> < > LIKE IN "
                        + "BETWEEN IS NULL AND OR NOT) choosing which events to return, for example "
                        + "event_type LIKE '3ds_%'. Use only field names get_schema lists for the topic. Key and "
                        + "header fields are cheap to filter on; body fields decode each event's value. With key, "
                        + "both must match. Try it first with check_selector.");
    }

    /** The tool as the agent sees it. */
    McpSchema.Tool definition() {
        Map<String, Object> key = Map.of("type", "string",
                "description", "The entity's key, for example a customer or order ID. Letters, digits and . _ : - only. "
                        + "Give a key, a selector, or both.");
        Map<String, Object> lookback = Map.of("type", "string", "default", "1h",
                "description", "How far back to read: for example \"15m\", \"6h\" or \"24h\". At most "
                        + EntityQuery.format(limits.maxLookback()) + ".");
        Map<String, Object> maxEvents = Map.of("type", "integer", "minimum", 1, "default", EntityQuery.DEFAULT_MAX_EVENTS,
                "description", "The most events to return: the newest ones, listed oldest first. Set it higher or "
                        + "lower as you need; matched and more_events say whether older ones were left out.");
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("topic", topicProperty(topics, "Optional: "));
        properties.put("key", key);
        properties.put("selector", selectorProperty("Optional: "));
        properties.put("lookback", lookback);
        properties.put("max_events", maxEvents);
        schema.put("properties", properties);
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(NAME)
                .title("Entity events")
                .description("Returns the events over a recent time window that match a key, a selector, or "
                        + "both, from an event log topic (" + String.join(", ", topics.all()) + "), with a "
                        + "completeness status. The window is read and filtered on the server, so only matching "
                        + "events are returned. Use it to answer \"what happened to <entity> in the last <time>?\" "
                        + "(key) or \"which events like this happened?\" (selector). Call get_schema first to "
                        + "learn the fields a selector can use. Times are when the log received each event. If "
                        + "status is TRUNCATED_BY_RETENTION, events may be missing: don't conclude that nothing happened.")
                .inputSchema(schema)
                .build();
    }

    /** Handle one call: invalid arguments and failures come back as tool errors the agent can read. */
    McpSchema.CallToolResult call(Map<String, Object> args) {
        EntityQuery query;
        try {
            query = EntityQuery.parseAgainst(args, limits, topics, fields);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return error(e.getMessage());
        }
        try {
            Map<String, Object> result = read(query);
            LOG.info("{} topic={} filter={} lookback={}: {} matched, status {}", NAME, query.topic(), query.filter(),
                    EntityQuery.format(query.lookback()), result.get("matched"), result.get("status"));
            return McpSchema.CallToolResult.builder()
                    .addTextContent(JSON.writeValueAsString(result))
                    .structuredContent(result)
                    .build();
        } catch (WindowTimeoutException e) {
            return error(e.getMessage());
        } catch (Exception e) {
            LOG.warn("{} topic={} filter={} failed", NAME, query.topic(), query.filter(), e);
            return error("couldn't read the window: " + e.getMessage());
        }
    }

    private Map<String, Object> read(EntityQuery query) throws Exception {
        long start = clock.millis();
        long deadline = start + limits.timeout().toMillis();
        List<Map<String, Object>> events = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        try (Consumer<byte[]> consumer = sessions.get().newConsumer()
                .topic(query.topic())
                .selector(query.filter())
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
                if (seen.add(sourceId != null ? sourceId : m.getMessageId().toString())) {
                    keepNewest(events, event(m.getKey(), m.getProperties(), m.getData(), m.getPublishTime()), query.maxEvents());
                }
                consumer.acknowledge(m);
            }
            return result(query, query.topic(), window.stats(), events, seen.size(), clock.millis() - start);
        }
    }

    /** The key and selector the agent gave, each only when given. */
    static void putFilter(Map<String, Object> r, EntityQuery query) {
        if (query.key() != null) {
            r.put("key", query.key());
        }
        if (query.selector() != null) {
            r.put("selector", query.selector());
        }
    }

    /**
     * Add an event, keeping only the newest {@code max}, oldest first. The window is read oldest to
     * newest, and for "what just happened?" the newest events matter most, so the oldest give way.
     */
    static void keepNewest(List<Map<String, Object>> events, Map<String, Object> event, int max) {
        if (events.size() >= max) {
            events.remove(0);
        }
        events.add(event);
    }

    /**
     * The tool's result: what was asked, the window, its status and what it means, the counts, and the
     * events. {@code topic} is used when the query has none of its own.
     */
    static Map<String, Object> result(EntityQuery query, String topic, WindowStats stats,
                                      List<Map<String, Object>> events, long matched, long elapsedMs) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("topic", query.topic() != null ? query.topic() : topic);
        putFilter(r, query);
        if (!query.selectorWarnings().isEmpty()) {
            // Valid, so not rejected, but likely not what the agent meant: say so next to the (maybe empty) result.
            r.put("selector_warnings", query.selectorWarnings());
        }
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
        if (matched > events.size()) {
            r.put("omitted", (matched - events.size()) + " older matching events aren't in this list; these are the newest "
                    + events.size() + ". Ask again with a larger max_events to include them.");
        }
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
