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

import io.streamnative.filter.common.selector.Selector;
import io.streamnative.filter.common.selector.SelectorParseException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A validated {@code get_entity_events} request: which topic to read, an entity key and/or a JMS
 * selector, how far back to read, and how many events to return.
 *
 * <p>Agent input is treated as input. The topic must be on the server's allowlist ({@link Topics}).
 * The key must match {@link #KEY}, which admits no quotes or spaces, so it can't change the selector
 * it's placed in. A selector is parsed here, with the same parser the broker uses, and may only name
 * fields the topic has ({@code get_schema}), so a typo is an error the agent can fix rather than a
 * filter that silently matches nothing. All of this happens before any filter starts on the broker.
 * A lookback beyond the server's maximum is rejected rather than shortened, because a shorter window
 * answers a different question. The agent chooses how many events come back with {@code max_events}
 * (default {@link #DEFAULT_MAX_EVENTS}, no server cap); the result says how many events matched either way.
 *
 * <p>{@code topic} and {@code selector} are null when the call didn't give them: a null topic means
 * the tool's own (first) topic. {@code selectorWarnings} are comparisons in the selector that can't match
 * as written (a header compared with a number, say): returned with the result rather than rejected, since
 * the selector is valid and the agent may mean it.
 */
record EntityQuery(String topic, String key, String selector, Duration lookback, int maxEvents,
                   List<String> selectorWarnings, SqlTranslation sql) {

    /** Letters, digits and {@code . _ : -}, up to 128 characters. */
    static final Pattern KEY = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    static final Duration DEFAULT_LOOKBACK = Duration.ofHours(1);
    /** Events returned when the agent doesn't set {@code max_events}; it can ask for more or fewer. */
    static final int DEFAULT_MAX_EVENTS = 1000;

    /** Longest selector accepted: long enough for real filters, short enough to keep agent text bounded. */
    static final int MAX_SELECTOR_LENGTH = 1000;

    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*([smhd])");

    /** What one call may ask for: set by the server's operator, not by the agent. */
    record Limits(Duration maxLookback, Duration timeout) {
    }

    /** A selector that passed {@link #checkSelector}: its text, compiled form, the topic's fields, and hints. */
    record CheckedSelector(String text, Selector compiled, FieldCatalog.Fields fields, List<String> warnings) {
    }

    EntityQuery {
        selectorWarnings = selectorWarnings == null ? List.of() : List.copyOf(selectorWarnings);
    }

    /** A query not written as SQL. */
    EntityQuery(String topic, String key, String selector, Duration lookback, int maxEvents,
                List<String> selectorWarnings) {
        this(topic, key, selector, lookback, maxEvents, selectorWarnings, null);
    }

    /** A query with no selector warnings. */
    EntityQuery(String topic, String key, String selector, Duration lookback, int maxEvents) {
        this(topic, key, selector, lookback, maxEvents, List.of());
    }

    /** A key-only query on the tool's own topic. */
    EntityQuery(String key, Duration lookback, int maxEvents) {
        this(null, key, null, lookback, maxEvents);
    }

    /**
     * Validate key-only tool arguments against {@code limits}: the key is required, the topic is the
     * tool's own.
     *
     * @throws IllegalArgumentException with a message the agent can act on
     */
    static EntityQuery parse(Map<String, Object> args, Limits limits) {
        Object rawKey = args == null ? null : args.get("key");
        String key = parseKey(rawKey);
        return new EntityQuery(null, key, null, parseLookback(args, limits), parseMaxEvents(args));
    }

    /**
     * Validate tool arguments: a topic from {@code topics}, and a key, a selector, or both. A selector
     * may only name fields in {@code fieldNames.apply(topic)}, which is only asked when a selector is
     * given, so a key-only call never reads from the broker here.
     *
     * @throws IllegalArgumentException with a message the agent can act on
     * @throws IllegalStateException if the topic's fields can't be read
     */
    static EntityQuery parse(Map<String, Object> args, Limits limits, Topics topics,
                             Function<String, Set<String>> fieldNames) {
        return parseAgainst(args, limits, topics, t -> FieldCatalog.Fields.ofNames(t, fieldNames.apply(t)));
    }

    /**
     * {@link #parse(Map, Limits, Topics, Function)} against the topic's full {@link FieldCatalog.Fields}, which
     * also say each field's source and type, so comparisons that can't match come back as selector warnings.
     */
    static EntityQuery parseAgainst(Map<String, Object> args, Limits limits, Topics topics,
                                    Function<String, FieldCatalog.Fields> fields) {
        Map<String, Object> a = args == null ? Map.of() : args;
        SqlTranslation sql = null;
        if (a.get("sql") != null) {
            sql = translate(a, topics, fields);
            a = withSql(a, sql, topics);
        }
        String topic = topics.resolve(a.get("topic"));
        Object rawKey = a.get("key");
        String key = rawKey == null ? null : parseKey(rawKey);
        CheckedSelector checked = a.get("selector") == null ? null : checkSelector(a.get("selector"), topic, fields);
        if (key == null && checked == null) {
            if (sql != null) {
                throw new IllegalArgumentException("the SQL has no condition besides the time window, and no key was "
                        + "given: add a WHERE condition (for example event_type = 'login') or pass key");
            }
            throw new IllegalArgumentException("give a key, a selector, or both: a key picks one entity's events, "
                    + "a selector (for example event_type LIKE '3ds_%') picks events by their fields");
        }
        Duration lookback;
        try {
            lookback = parseLookback(a, limits);
        } catch (IllegalArgumentException e) {
            throw sql != null && sql.lookback() != null
                    ? new IllegalArgumentException("the SQL's time window: " + e.getMessage()) : e;
        }
        return new EntityQuery(topic, key, checked == null ? null : checked.text(), lookback,
                parseMaxEvents(a), checked == null ? List.of() : checked.warnings(), sql);
    }

    /**
     * The call's {@code sql}, translated ({@link SqlTranslator}). {@code sql} is used instead of {@code selector},
     * never with it.
     *
     * @throws IllegalArgumentException if both are given, or the SQL can't be translated
     */
    static SqlTranslation translate(Map<String, Object> args, Topics topics,
                                    Function<String, FieldCatalog.Fields> fields) {
        Object raw = args.get("sql");
        if (args.get("selector") != null) {
            throw new IllegalArgumentException("pass either sql or selector, not both: sql is a whole query whose "
                    + "WHERE becomes the selector, selector is the WHERE condition alone");
        }
        if (!(raw instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("sql must be a non-empty string, for example " + SqlTranslator.EXAMPLE);
        }
        return SqlTranslator.translate(text, topics, args.get("topic"), fields);
    }

    /**
     * The call's arguments with what the SQL set: its selector, and the topic, lookback and max_events it implies.
     * An explicit argument that agrees with the SQL is fine; one that disagrees is an error, since either could be
     * the one meant.
     */
    static Map<String, Object> withSql(Map<String, Object> args, SqlTranslation sql, Topics topics) {
        Map<String, Object> a = new java.util.HashMap<>(args);
        a.remove("sql");
        if (sql.topic() != null) {
            Object explicit = args.get("topic");
            if (explicit != null && !String.valueOf(explicit).isBlank() && !topics.resolve(explicit).equals(sql.topic())) {
                throw new IllegalArgumentException("the SQL reads FROM " + Topics.shortName(sql.topic()) + " but topic is "
                        + explicit + "; drop one of them, or make them name the same topic");
            }
            a.put("topic", sql.topic());
        }
        if (sql.lookback() != null) {
            Object explicit = args.get("lookback");
            if (explicit != null && !parseDuration(String.valueOf(explicit)).equals(sql.lookback())) {
                throw new IllegalArgumentException("the SQL's time window is " + format(sql.lookback()) + " but lookback is "
                        + explicit + "; drop one of them, or make them agree");
            }
            a.put("lookback", format(sql.lookback()));
        }
        if (sql.maxEvents() != null) {
            Object explicit = args.get("max_events");
            if (explicit != null && parseCount(explicit) != sql.maxEvents()) {
                throw new IllegalArgumentException("the SQL says LIMIT " + sql.maxEvents() + " but max_events is "
                        + explicit + "; drop one of them, or make them agree");
            }
            a.put("max_events", sql.maxEvents());
        }
        if (sql.selector() != null) {
            a.put("selector", sql.selector());
        } else {
            a.remove("selector");
        }
        return a;
    }

    /** An event as the result returns it: trimmed to the SQL's {@code SELECT} columns, when it named some. */
    Map<String, Object> project(Map<String, Object> event) {
        return sql == null ? event : sql.project(event);
    }

    /**
     * What the broker filters on: the key's selector, the agent's selector, or both joined with
     * {@code AND}. The agent's selector is parenthesised so an {@code OR} in it can't escape the key.
     * The key is safe to concatenate: {@link #KEY} admits no quotes.
     */
    String filter() {
        if (selector == null) {
            return keySelector();
        }
        return key == null ? selector : keySelector() + " AND (" + selector + ")";
    }

    private String keySelector() {
        return "__key__ = '" + key + "'";
    }

    static String parseKey(Object rawKey) {
        if (!(rawKey instanceof String key) || !KEY.matcher(key).matches()) {
            throw new IllegalArgumentException("key must be 1-128 letters, digits or . _ : - (got "
                    + (rawKey == null ? "nothing" : "\"" + rawKey + "\"") + ")");
        }
        return key;
    }

    /**
     * Validate a selector for {@code topic}: its length, its syntax (with the broker's parser), and that every
     * name it uses is one of the topic's fields. Cheap checks first; the broker is only asked for the topic's
     * fields once the syntax is valid. Shared by the event tools and {@code check_selector}, so both give the
     * same errors.
     *
     * @throws IllegalArgumentException with a message the agent can act on in one retry
     * @throws IllegalStateException if the topic's fields can't be read
     */
    static CheckedSelector checkSelector(Object raw, String topic, Function<String, FieldCatalog.Fields> fieldsOf) {
        if (!(raw instanceof String text) || text.isBlank()) {
            throw new IllegalArgumentException("selector must be a non-empty string, for example event_type LIKE '3ds_%'");
        }
        String written = text.trim();
        // SQL habits JMS doesn't share (!=, quoted identifiers) are rewritten first; everything else is checked as written.
        String selector = SelectorNormalizer.normalize(written);
        if (selector.length() > MAX_SELECTOR_LENGTH) {
            throw new IllegalArgumentException("selector is " + selector.length() + " characters; the most this server "
                    + "accepts is " + MAX_SELECTOR_LENGTH + ". Use a shorter expression");
        }
        Selector compiled;
        try {
            compiled = Selector.compile(selector);
        } catch (SelectorParseException e) {
            throw new IllegalArgumentException(SelectorAdvice.parseError(selector, e));
        }
        FieldCatalog.Fields fields = fieldsOf.apply(topic);
        Set<String> known = fields.names();
        List<String> unknown = compiled.referencedNames().stream().filter(n -> !known.contains(n)).toList();
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException(SelectorAdvice.unknownFields(topic, unknown, known));
        }
        List<String> warnings = new ArrayList<>();
        if (!selector.equals(written)) {
            warnings.add("Rewrote the selector into JMS syntax (!= as <>, quoted field names unquoted); it ran as: " + selector);
        }
        warnings.addAll(SelectorAdvice.typeHints(selector, fields));
        return new CheckedSelector(selector, compiled, fields, List.copyOf(warnings));
    }

    private static Duration parseLookback(Map<String, Object> args, Limits limits) {
        Duration lookback = DEFAULT_LOOKBACK.compareTo(limits.maxLookback()) > 0 ? limits.maxLookback() : DEFAULT_LOOKBACK;
        Object rawLookback = args.get("lookback");
        if (rawLookback != null) {
            lookback = parseDuration(String.valueOf(rawLookback));
        }
        if (lookback.compareTo(limits.maxLookback()) > 0) {
            throw new IllegalArgumentException("lookback " + rawLookback + " is longer than this server allows ("
                    + format(limits.maxLookback()) + "); ask for a shorter window");
        }
        return lookback;
    }

    private static int parseMaxEvents(Map<String, Object> args) {
        Object rawMax = args.get("max_events");
        return rawMax == null ? DEFAULT_MAX_EVENTS : parseCount(rawMax);
    }

    /** {@code "90s"}, {@code "15m"}, {@code "24h"}, {@code "1d"}. */
    static Duration parseDuration(String text) {
        Matcher m = DURATION.matcher(text.trim().toLowerCase());
        if (!m.matches()) {
            throw new IllegalArgumentException("lookback must look like 90s, 15m, 6h or 1d (got \"" + text + "\")");
        }
        long n = Long.parseLong(m.group(1));
        Duration d = switch (m.group(2)) {
            case "s" -> Duration.ofSeconds(n);
            case "m" -> Duration.ofMinutes(n);
            case "h" -> Duration.ofHours(n);
            default -> Duration.ofDays(n);
        };
        if (d.isZero()) {
            throw new IllegalArgumentException("lookback must be longer than zero");
        }
        return d;
    }

    /** Hours up to two days ({@code "24h"}, not {@code "1d"}), then days. */
    static String format(Duration d) {
        long s = d.toSeconds();
        if (s % 86_400 == 0 && s >= 2 * 86_400) {
            return s / 86_400 + "d";
        }
        if (s % 3_600 == 0) {
            return s / 3_600 + "h";
        }
        return s % 60 == 0 ? s / 60 + "m" : s + "s";
    }

    static int parseCount(Object raw) {
        long n;
        try {
            n = raw instanceof Number num ? num.longValue() : Long.parseLong(String.valueOf(raw).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("max_events must be a whole number (got \"" + raw + "\")");
        }
        if (n < 1) {
            throw new IllegalArgumentException("max_events must be at least 1");
        }
        return (int) Math.min(n, Integer.MAX_VALUE);
    }
}
