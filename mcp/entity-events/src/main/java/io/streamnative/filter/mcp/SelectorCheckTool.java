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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.common.selector.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * The {@code check_selector} tool: a dry run of a selector before the agent pays for a scan. It validates the
 * selector exactly as {@code get_entity_events} would ({@link EntityQuery#checkSelector}, so the errors are the
 * same), says which fields it uses and what they cost, and tries it on the recent messages {@link FieldCatalog}
 * already sampled for {@code get_schema}, with a resolver that mirrors the filter's own
 * ({@link SelectorAdvice#resolver}). It starts no filter and opens no SDK session: the only broker read is the
 * catalog's, which is cached.
 *
 * <p>The sample is a handful of the newest events, so its counts say whether the selector can match and how
 * selective it looks, not how many events a long lookback holds. The result says so ({@code sample_note}).
 */
final class SelectorCheckTool {

    static final String NAME = "check_selector";

    private static final Logger LOG = LoggerFactory.getLogger(SelectorCheckTool.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_VALUES_SHOWN = 5;

    private final Topics topics;
    private final Function<String, FieldCatalog.Fields> fields;

    SelectorCheckTool(Topics topics, Function<String, FieldCatalog.Fields> fields) {
        this.topics = topics;
        this.fields = fields;
    }

    SelectorCheckTool(Topics topics, FieldCatalog catalog) {
        this(topics, catalog::fields);
    }

    /** The tool as the agent sees it. */
    McpSchema.Tool definition() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("selector", Map.of("type", "string", "maxLength", EntityQuery.MAX_SELECTOR_LENGTH,
                "description", "The selector to check (a SQL WHERE clause without the WHERE: the SQL-92 subset JMS "
                        + "selectors use), as you would pass it to get_entity_events or watch_entity_events, for "
                        + "example event_type LIKE '3ds_%'. Give selector or sql."));
        properties.put("sql", EntityEventsTool.sqlProperty("Instead of selector: "));
        properties.put("topic", EntityEventsTool.topicProperty(topics, "Optional: "));
        properties.put("key", Map.of("type", "string",
                "description", "Optional: the entity key you would pass with it; the check then covers both, "
                        + "as the read would."));
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", properties);
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(NAME)
                .title("Check a selector")
                .description("Checks a selector, or a SQL query (sql) and the selector and arguments it translates "
                        + "to, without reading the topic or starting a filter: whether it's valid "
                        + "(and how to fix it if not), the filter that would run, the fields it uses and what they "
                        + "cost, how many of the topic's most recent events it matches, and warnings about selectors "
                        + "that match everything, match nothing, or can't match as written. Call it before "
                        + "get_entity_events or watch_entity_events with a long lookback.")
                .inputSchema(schema)
                .build();
    }

    /** Handle one call. An invalid selector is a normal result ({@code valid: false}); a bad topic or key is an error. */
    McpSchema.CallToolResult call(Map<String, Object> args) {
        Map<String, Object> a = args == null ? Map.of() : args;
        if (a.get("sql") != null) {
            return callSql(a);
        }
        String topic;
        String key;
        try {
            topic = topics.resolve(a.get("topic"));
            key = a.get("key") == null ? null : EntityQuery.parseKey(a.get("key"));
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("topic", topic);
        return checkAndReport(r, a.get("selector"), topic, key);
    }

    /**
     * A call with {@code sql}: the translation (the selector, and the topic, lookback and max_events the SQL
     * sets), then the usual check of that selector. A SQL that can't be translated is {@code valid: false}.
     */
    private McpSchema.CallToolResult callSql(Map<String, Object> a) {
        String key;
        SqlTranslation sql;
        Map<String, Object> r = new LinkedHashMap<>();
        try {
            key = a.get("key") == null ? null : EntityQuery.parseKey(a.get("key"));
            topics.resolve(a.get("topic"));
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        }
        String topic;
        try {
            sql = EntityQuery.translate(a, topics, fields);
            topic = topics.resolve(EntityQuery.withSql(a, sql, topics).get("topic"));
        } catch (IllegalArgumentException e) {
            r.put("topic", topics.resolve(a.get("topic")));
            r.put("sql", a.get("sql"));
            r.put("valid", false);
            r.put("error", e.getMessage());
            LOG.info("{} sql: invalid: {}", NAME, e.getMessage());
            return ok(r);
        } catch (IllegalStateException e) {
            return error(e.getMessage());
        }
        r.put("topic", topic);
        r.put("sql", sql.sql());
        r.put("translation", sql.describe());
        if (sql.selector() == null) {
            r.put("valid", false);
            r.put("error", "the SQL has no condition besides the time window, so there is no selector to check; "
                    + "add a WHERE condition, for example event_type = 'login'");
            return ok(r);
        }
        return checkAndReport(r, sql.selector(), topic, key);
    }

    private McpSchema.CallToolResult checkAndReport(Map<String, Object> r, Object selector, String topic, String key) {
        EntityQuery.CheckedSelector checked;
        try {
            checked = EntityQuery.checkSelector(selector, topic, fields);
        } catch (IllegalArgumentException e) {
            r.put("valid", false);
            r.put("error", e.getMessage());
            LOG.info("{} topic={}: invalid: {}", NAME, topic, e.getMessage());
            return ok(r);
        } catch (IllegalStateException e) {
            return error(e.getMessage());
        }
        check(r, topic, key, checked);
        LOG.info("{} topic={} filter={}: matched {} of {} sampled", NAME, topic, r.get("filter"),
                r.get("matched_in_sample"), r.get("sampled"));
        return ok(r);
    }

    private static void check(Map<String, Object> r, String topic, String key, EntityQuery.CheckedSelector checked) {
        FieldCatalog.Fields f = checked.fields();
        String filter = new EntityQuery(topic, key, checked.text(), EntityQuery.DEFAULT_LOOKBACK, 1).filter();
        r.put("valid", true);
        r.put("filter", filter);

        Set<String> names = new LinkedHashSet<>();
        if (key != null) {
            names.add("__key__");
        }
        names.addAll(checked.compiled().referencedNames());
        List<Map<String, Object>> used = new ArrayList<>();
        List<String> bodyFields = new ArrayList<>();
        for (String name : names) {
            Optional<FieldCatalog.Field> field = f.field(name);
            Map<String, Object> u = new LinkedHashMap<>();
            u.put("name", name);
            u.put("source", field.map(FieldCatalog.Field::source).orElse("unknown"));
            u.put("type", field.map(FieldCatalog.Field::type).orElse("unknown"));
            used.add(u);
            if (field.isPresent() && "body".equals(field.get().source())) {
                bodyFields.add(name);
            }
        }
        r.put("fields_used", used);
        r.put("cost", bodyFields.isEmpty() ? "metadata only" : "decodes each event's value");

        // Comparisons that can't match as written come first: they usually explain a sample that matched nothing.
        List<String> warnings = new ArrayList<>(checked.warnings());
        List<FieldCatalog.Sample> samples = f.samples();
        r.put("sampled", samples.size());
        boolean timeMissing = checked.compiled().referencedNames().contains("__publish_time__")
                && samples.stream().anyMatch(s -> s.publishTime() == null);
        if (samples.isEmpty()) {
            warnings.add("The topic had no recent events to try the selector on, so it wasn't checked against data.");
        } else if (timeMissing) {
            r.put("sample_skipped", "The sampled events don't carry their publish time, so a selector on "
                    + "__publish_time__ can't be tried on them. Use lookback for the time window instead.");
        } else {
            Selector effective = Selector.compile(filter);
            int matched = 0;
            int selectorMatched = 0;
            for (FieldCatalog.Sample s : samples) {
                Function<String, Object> resolve = SelectorAdvice.resolver(s);
                if (effective.matches(resolve)) {
                    matched++;
                }
                if (checked.compiled().matches(resolve)) {
                    selectorMatched++;
                }
            }
            r.put("matched_in_sample", matched);
            if (key != null) {
                r.put("selector_matched_in_sample", selectorMatched);
            }
            int n = samples.size();
            if (selectorMatched == n) {
                warnings.add("The selector matched all " + n + " sampled events, so it is likely too broad: expect "
                        + "most events in the window to match.");
            } else if (selectorMatched == 0) {
                warnings.add("The selector matched none of the " + n + " sampled events. Check the field names and "
                        + "values; the sampled values were " + sampledValues(checked.compiled().referencedNames(),
                        samples) + ".");
            } else if (key != null && matched == 0) {
                warnings.add("None of the sampled events have key " + key + ", so the sample can't show whether this "
                        + "key's events match. That's expected unless the key is very active.");
            }
        }
        if (!SelectorAdvice.picksExactValues(checked.text(), key)) {
            warnings.add("Nothing in the filter picks exact values (no key, and no = or IN on a field), so every "
                    + "matching event in the window is read and returned. Keep the lookback short, or add an "
                    + "equality such as __key__ = '<id>'.");
        }
        if (!bodyFields.isEmpty()) {
            List<String> headers = f.fields().stream().filter(x -> "header".equals(x.source()))
                    .map(FieldCatalog.Field::name).toList();
            warnings.add(String.join(", ", bodyFields) + (bodyFields.size() == 1 ? " is a body field" : " are body fields")
                    + ", so the filter decodes each event's value in the window to read "
                    + (bodyFields.size() == 1 ? "it" : "them") + "."
                    + (headers.isEmpty() ? "" : " If a header carries similar data (this topic's headers: "
                    + String.join(", ", headers) + "), filtering on it is cheaper."));
        }
        for (SelectorAdvice.Term t : SelectorAdvice.leadingWildcards(checked.text())) {
            warnings.add(t.text() + " starts with %, so it matches the text anywhere in the value, which is often "
                    + "broader than meant. If you know how the values begin, write the pattern as 'prefix%'.");
        }
        r.put("warnings", warnings);

        String note = "The sample is small and recent: the " + samples.size() + " newest events on the topic, read "
                + "when its fields were last learned (cached for a few minutes). Its counts show whether the "
                + "selector can match and how selective it looks, not how many events a longer lookback holds.";
        if (!bodyFields.isEmpty() && f.bodyFieldsFrom().contains("AVRO")) {
            note += " This topic's values are Avro, which the check reads as JSON only, so body fields read as "
                    + "absent in the sample even where the filter would match them.";
        }
        r.put("sample_note", note);
    }

    /** {@code event_type: 'created', 'shipped'; amount: 12.5}, from the sample, per field the selector names. */
    private static String sampledValues(Set<String> names, List<FieldCatalog.Sample> samples) {
        List<String> parts = new ArrayList<>();
        for (String name : names) {
            Set<String> values = new LinkedHashSet<>();
            for (FieldCatalog.Sample s : samples) {
                Object v = SelectorAdvice.resolver(s).apply(name);
                if (v != null && values.size() < MAX_VALUES_SHOWN) {
                    values.add(SelectorAdvice.literal(v));
                }
            }
            parts.add(name + ": " + (values.isEmpty() ? "absent from every sampled event" : String.join(", ", values)));
        }
        return String.join("; ", parts);
    }

    private static McpSchema.CallToolResult ok(Map<String, Object> result) {
        try {
            return McpSchema.CallToolResult.builder()
                    .addTextContent(JSON.writeValueAsString(result))
                    .structuredContent(result)
                    .build();
        } catch (JsonProcessingException e) {
            return error("couldn't write the result: " + e.getMessage());
        }
    }

    private static McpSchema.CallToolResult error(String message) {
        return McpSchema.CallToolResult.builder().addTextContent(message).isError(true).build();
    }
}
