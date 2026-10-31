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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.selector.Selector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SqlTranslatorTests {

    private static final String ORDERS = "persistent://public/default/orders";
    private static final String SESSIONS = "persistent://public/default/session_events";
    private static final Topics TOPICS = Topics.of(ORDERS, SESSIONS);
    private static final EntityQuery.Limits LIMITS =
            new EntityQuery.Limits(Duration.ofHours(24), Duration.ofSeconds(60));

    private static FieldCatalog.Field f(String name, String source, String type, String... examples) {
        return new FieldCatalog.Field(name, source, type, List.of(examples));
    }

    /** orders: no field called ts or timestamp, so they are the time column. */
    private static final FieldCatalog.Fields ORDER_FIELDS = new FieldCatalog.Fields(ORDERS, List.of(
            f("__key__", "key", "string"),
            f("__publish_time__", "metadata", "number"),
            f("event_type", "header", "string", "order_placed", "payment_failed", "Order_Placed"),
            f("seq", "header", "string", "1", "2"),
            f("status", "body", "string", "failed", "ok"),
            f("amount", "body", "number", "12.5"),
            f("customer.tier", "body", "string", "Gold", "gold", "silver"),
            f("customer.id", "body", "string", "c1"),
            f("flag", "body", "boolean", "true")), 5, "sampled JSON values");

    /** session_events: has a body field called ts, so ts there is an ordinary field. */
    private static final FieldCatalog.Fields SESSION_FIELDS = new FieldCatalog.Fields(SESSIONS, List.of(
            f("__key__", "key", "string"),
            f("__publish_time__", "metadata", "number"),
            f("event_type", "header", "string", "login"),
            f("ts", "body", "number", "1700000000000")), 5, "sampled JSON values");

    private final List<String> asked = new ArrayList<>();
    private final Function<String, FieldCatalog.Fields> fields = topic -> {
        asked.add(topic);
        return topic.equals(SESSIONS) ? SESSION_FIELDS : ORDER_FIELDS;
    };

    private SqlTranslation translate(String sql) {
        return SqlTranslator.translate(sql, TOPICS, null, fields);
    }

    private static Map<String, Object> args(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    // ---------------------------------------------------------------- translations

    static Stream<Arguments> translations() {
        return Stream.of(
                // bare conditions, and WHERE <condition>
                arguments("event_type = 'order_placed'", "event_type = 'order_placed'"),
                arguments("WHERE event_type = 'order_placed'", "event_type = 'order_placed'"),
                arguments("where amount > 5", "amount > 5"),
                arguments("event_type != 'x'", "event_type <> 'x'"),
                arguments("event_type <> 'x'", "event_type <> 'x'"),
                arguments("amount < 5 AND amount <= 6 AND amount >= 1", "amount < 5 AND amount <= 6 AND amount >= 1"),
                arguments("event_type = 'a' OR status = 'failed'", "event_type = 'a' OR status = 'failed'"),
                arguments("(event_type = 'a' OR event_type = 'b') AND status = 'failed'",
                        "(event_type = 'a' OR event_type = 'b') AND status = 'failed'"),
                arguments("event_type = 'a' OR (event_type = 'b' AND status = 'failed')",
                        "event_type = 'a' OR event_type = 'b' AND status = 'failed'"),
                arguments("NOT event_type = 'a'", "NOT event_type = 'a'"),
                arguments("NOT (event_type = 'a' OR status = 'ok')", "NOT (event_type = 'a' OR status = 'ok')"),
                arguments("event_type IN ('a', 'b')", "event_type IN ('a', 'b')"),
                arguments("event_type NOT IN ('a')", "event_type NOT IN ('a')"),
                arguments("amount IN (5, 10)", "amount = 5 OR amount = 10"),
                arguments("amount IN (5, 10) AND status = 'ok'", "(amount = 5 OR amount = 10) AND status = 'ok'"),
                arguments("amount NOT IN (5, -1.5)", "NOT (amount = 5 OR amount = -1.5)"),
                arguments("event_type LIKE '3ds_%'", "event_type LIKE '3ds_%'"),
                arguments("event_type NOT LIKE 'a!_%' ESCAPE '!'", "event_type NOT LIKE 'a!_%' ESCAPE '!'"),
                arguments("amount BETWEEN 1 AND 10", "amount BETWEEN 1 AND 10"),
                arguments("amount NOT BETWEEN 1 AND 10", "amount NOT BETWEEN 1 AND 10"),
                arguments("status IS NULL", "status IS NULL"),
                arguments("status IS NOT NULL", "status IS NOT NULL"),
                arguments("amount * 2 + 1 > 10", "amount * 2 + 1 > 10"),
                arguments("amount - (1 - 2) > 0", "amount - (1 - 2) > 0"),
                arguments("(amount + 1) * 2 > 10", "(amount + 1) * 2 > 10"),
                arguments("amount / 2 >= 1.5", "amount / 2 >= 1.5"),
                arguments("-amount < -5", "-amount < -5"),
                arguments("amount > 1e3", "amount > 1e3"),
                arguments("status = 'it''s'", "status = 'it''s'"),
                arguments("flag = TRUE AND status <> 'ok'", "flag = TRUE AND status <> 'ok'"),
                arguments("flag = false", "flag = FALSE"),
                arguments("customer.tier = 'gold'", "customer.tier = 'gold'"),
                arguments("\"event_type\" = 'a'", "event_type = 'a'"),
                arguments("`event_type` = 'a'", "event_type = 'a'"),
                arguments("\"customer\".\"tier\" = 'gold'", "customer.tier = 'gold'"),
                arguments("__key__ = 'o-1' AND seq = '2'", "__key__ = 'o-1' AND seq = '2'"),
                arguments("__publish_time__ > 1700000000000", "__publish_time__ > 1700000000000"),
                arguments("flag AND status = 'ok'", "flag AND status = 'ok'"),
                // JSqlParser 5.3 folds what follows an IN list into the list; the translator keeps it out
                arguments("event_type IN ('a', 'b') AND status = 'ok'", "event_type IN ('a', 'b') AND status = 'ok'"),
                arguments("status NOT IN ('a') OR amount > 1", "status NOT IN ('a') OR amount > 1"),
                arguments("amount > 1 AND NOT status IN ('a') OR event_type = 'b'",
                        "amount > 1 AND NOT status IN ('a') OR event_type = 'b'"),
                arguments("event_type in ('x') and status in ('a)', 'it''s', 'NOT IN (') and amount > 0",
                        "event_type IN ('x') AND status IN ('a)', 'it''s', 'NOT IN (') AND amount > 0"),
                arguments("status = 'in (x)' AND event_type IN ('a')", "status = 'in (x)' AND event_type IN ('a')"),
                // full queries
                arguments("SELECT * FROM orders WHERE event_type = 'a'", "event_type = 'a'"),
                arguments("SELECT * WHERE event_type = 'a'", "event_type = 'a'"),
                arguments("select * from orders where status = 'failed' limit 5;", "status = 'failed'"),
                arguments("SELECT event_type, amount FROM orders WHERE amount > 5 ORDER BY ts DESC LIMIT 10",
                        "amount > 5"),
                arguments("SELECT * FROM orders WHERE ts > now() - INTERVAL '15' MINUTE AND event_type = 'a'",
                        "event_type = 'a'"),
                arguments("SELECT * FROM orders WHERE (event_type = 'a' AND ts > now() - INTERVAL '1' HOUR) "
                        + "AND status = 'ok'", "event_type = 'a' AND status = 'ok'"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("translations")
    void translatesToTheSelector(String sql, String selector) {
        assertThat(translate(sql).selector()).isEqualTo(selector);
    }

    /** Every emitted selector compiles with the filter's own parser and names only columns that the SQL names. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("translations")
    void everyEmittedSelectorCompilesAndUsesOnlyTheSqlsColumns(String sql, String expected) {
        String selector = translate(sql).selector();
        Selector compiled = Selector.compile(selector);
        String unquotedSql = sql.replace("\"", "").replace("`", "");
        for (String name : compiled.referencedNames()) {
            assertThat(identifiers(unquotedSql)).as("%s in %s", name, sql).contains(name);
        }
        // and the check the tools run accepts it unchanged
        assertThat(EntityQuery.checkSelector(selector, ORDERS, fields).text()).isEqualTo(selector);
    }

    private static List<String> identifiers(String sql) {
        List<String> out = new ArrayList<>();
        Matcher m = Pattern.compile("'(?:[^']|'')*'|([A-Za-z_$][A-Za-z0-9_$]*(?:\\.[A-Za-z_$][A-Za-z0-9_$]*)*)")
                .matcher(sql);
        while (m.find()) {
            if (m.group(1) != null) {
                out.add(m.group(1));
            }
        }
        return out;
    }

    /**
     * Where the printer drops or adds parentheses, the selector must still mean what the SQL meant: evaluate both
     * the translation and a hand-parenthesised selector over every combination of field values.
     */
    @Test
    void reprintedPrecedenceMatchesTheSqlOnEveryCombination() {
        Map<String, String> sqlToMeaning = new LinkedHashMap<>();
        sqlToMeaning.put("event_type = 'a' OR (event_type = 'b' AND status = 'failed')",
                "(event_type = 'a') OR ((event_type = 'b') AND (status = 'failed'))");
        sqlToMeaning.put("event_type IN ('a', 'b') AND status = 'failed' OR amount > 5",
                "((event_type IN ('a', 'b')) AND (status = 'failed')) OR (amount > 5)");
        sqlToMeaning.put("amount > 1 AND NOT status IN ('ok') OR event_type = 'b'",
                "((amount > 1) AND (NOT (status IN ('ok')))) OR (event_type = 'b')");
        sqlToMeaning.put("NOT (event_type = 'a' OR status = 'ok') AND amount IN (1, 5)",
                "(NOT ((event_type = 'a') OR (status = 'ok'))) AND ((amount = 1) OR (amount = 5))");
        sqlToMeaning.put("amount - (1 - 2) * 2 > 5", "(amount - ((1 - 2) * 2)) > 5");
        String[] types = {"a", "b", null};
        String[] statuses = {"ok", "failed", null};
        Object[] amounts = {1L, 5L, 10L, null};
        for (Map.Entry<String, String> e : sqlToMeaning.entrySet()) {
            Selector translated = Selector.compile(translate(e.getKey()).selector());
            Selector meant = Selector.compile(e.getValue());
            for (String type : types) {
                for (String status : statuses) {
                    for (Object amount : amounts) {
                        Map<String, Object> m = new HashMap<>();
                        m.put("event_type", type);
                        m.put("status", status);
                        m.put("amount", amount);
                        assertThat(translated.matches(m::get)).as("%s with %s", e.getKey(), m)
                                .isEqualTo(meant.matches(m::get));
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- rejections

    static Stream<Arguments> rejections() {
        return Stream.of(
                arguments("LOWER(event_type) = 'order_placed'", "case-insensitive matching"),
                arguments("LOWER(customer.tier) = 'gold'", "customer.tier IN ('Gold', 'gold')"),
                arguments("event_type ILIKE 'order%'", "event_type IN ('order_placed', 'Order_Placed')"),
                arguments("UPPER(status) = 'OK'", "LOWER, UPPER and ILIKE aren't available"),
                arguments("CAST(seq AS INT) > 5", "casts"),
                arguments("seq::int > 5", "casts"),
                arguments("COALESCE(status, 'x') = 'x'", "coupon IS NULL"),
                arguments("SUBSTRING(status, 1, 2) = 'fa'", "LIKE 'ABC%'"),
                arguments("status || 'x' = 'okx'", "|| (string concatenation)"),
                arguments("CASE WHEN amount > 5 THEN 1 ELSE 0 END = 1", "CASE … END"),
                arguments("status SIMILAR TO 'f.*'", "regular expressions"),
                arguments("status REGEXP 'f.*'", "regular expressions"),
                arguments("status ~ 'f.*'", "regular expressions"),
                arguments("EXISTS (SELECT 1 FROM orders)", "subqueries"),
                arguments("status IN (SELECT status FROM orders)", "subqueries"),
                arguments("SELECT * FROM (SELECT * FROM orders) t", "subqueries"),
                arguments("SELECT * FROM orders JOIN session_events ON a = b", "joins"),
                arguments("SELECT * FROM orders, session_events", "joins"),
                arguments("SELECT count(*) FROM orders", "aggregates"),
                arguments("SELECT status FROM orders GROUP BY status", "aggregates"),
                arguments("SELECT DISTINCT status FROM orders", "DISTINCT"),
                arguments("SELECT * FROM orders UNION SELECT * FROM session_events", "set operations"),
                arguments("SELECT * FROM orders LIMIT 5 OFFSET 10", "OFFSET"),
                arguments("SELECT * FROM orders LIMIT 10, 5", "OFFSET"),
                arguments("status = ?", "parameters"),
                arguments("status = :status", "parameters"),
                arguments("SELECT * FROM orders; SELECT * FROM orders", "more than one statement"),
                arguments("status = 'a'; DROP TABLE orders", "more than one statement"),
                arguments("DELETE FROM orders WHERE status = 'a'", "DELETE statements aren't supported"),
                arguments("status = ", "couldn't parse the SQL"),
                arguments("status = 'a' 'b'", "couldn't parse the SQL"),
                arguments("status = NULL", "IS NULL"),
                arguments("status IS TRUE", "flag = TRUE"),
                arguments("amount % 2 = 0", "% and DIV"),
                arguments("SELECT * FROM orders ORDER BY amount", "ORDER BY amount isn't supported"),
                arguments("SELECT * FROM orders ORDER BY ts", "ORDER BY ts isn't supported"),
                arguments("SELECT * FROM orders ORDER BY ts ASC", "ORDER BY <time> DESC"),
                arguments("SELECT * FROM orders LIMIT 0", "at least 1"),
                arguments("SELECT * FROM orders LIMIT ALL", "LIMIT n"),
                arguments("SELECT TOP 5 * FROM orders", "LIMIT n"),
                arguments("SELECT status AS s FROM orders", "column aliases"),
                arguments("SELECT * FROM orders o", "table aliases"),
                arguments("SELECT * FROM nope", "isn't one this server reads"),
                arguments("SELECT colour FROM orders", "SELECT names a field that orders doesn't have: colour"),
                arguments("\"event-type\" = 'a'", "quoted name \"event-type\" can't be used"),
                arguments("ts > now() - INTERVAL '15' MINUTE OR status = 'ok'", "AND-ed at the top level"),
                arguments("NOT ts > now() - INTERVAL '15' MINUTE", "AND-ed at the top level"),
                arguments("ts > now() - INTERVAL '1' WEEK", "unit WEEK"),
                arguments("ts > now() - INTERVAL '15' MINUTE AND ts > now() - INTERVAL '1' HOUR",
                        "two time windows"),
                arguments("amount > now()", "now(), CURRENT_TIMESTAMP and INTERVAL can only appear"),
                arguments("ts = 5", "ts isn't a field of orders"),
                arguments("SELECT * FROM orders WHERE status = 'ok' FOR UPDATE", "FOR UPDATE"),
                arguments("x'1F' = status", "hex literals"),
                arguments("ts > NOW() - INTERVAL '0' MINUTE", "empty"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("rejections")
    void rejectsWithTheConstructAndTheNearestForm(String sql, String message) {
        assertThatThrownBy(() -> translate(sql)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    @Test
    void aBlankOrOverlongSqlIsRejected() {
        assertThatThrownBy(() -> translate("  ")).hasMessageContaining("non-empty");
        assertThatThrownBy(() -> translate(";")).hasMessageContaining("non-empty");
        assertThatThrownBy(() -> translate("WHERE ")).hasMessageContaining("no condition");
        assertThatThrownBy(() -> translate("status = '" + "x".repeat(SqlTranslator.MAX_SQL_LENGTH) + "'"))
                .hasMessageContaining("characters");
    }

    @Test
    void aParseErrorCarriesTheParsersDetailTrimmed() {
        assertThatThrownBy(() -> translate("SELECT * FROM WHERE status = 'a'"))
                .hasMessageStartingWith("couldn't parse the SQL: ")
                .hasMessageContaining("line 1")
                .hasMessageNotContaining("Was expecting");
        assertThatThrownBy(() -> translate("SELECT * FROM persistent://public/default/orders"))
                .hasMessageContaining("Quote a full topic name");
        assertThatThrownBy(() -> translate("status = 'a' ORDER BY ts DESC"))
                .hasMessageContaining("ORDER needs a full query");
    }

    // ---------------------------------------------------------------- the time window

    static Stream<Arguments> timeWindows() {
        return Stream.of(
                arguments("ts > now() - INTERVAL '15' MINUTE", Duration.ofMinutes(15)),
                arguments("ts >= now() - INTERVAL '15' MINUTE", Duration.ofMinutes(15)),
                arguments("ts > now() - INTERVAL 15 MINUTE", Duration.ofMinutes(15)),
                arguments("ts > now() - INTERVAL '15' MINUTES", Duration.ofMinutes(15)),
                arguments("ts > now() - INTERVAL '15 minutes'", Duration.ofMinutes(15)),
                arguments("ts > now() - interval '1 minute'", Duration.ofMinutes(1)),
                arguments("ts > now() - INTERVAL '90' SECOND", Duration.ofSeconds(90)),
                arguments("ts > now() - INTERVAL '30 seconds'", Duration.ofSeconds(30)),
                arguments("time > now() - INTERVAL '2' HOUR", Duration.ofHours(2)),
                arguments("timestamp > now() - INTERVAL '6 hours'", Duration.ofHours(6)),
                arguments("publish_time > CURRENT_TIMESTAMP - INTERVAL '1' DAY", Duration.ofDays(1)),
                arguments("__publish_time__ > now() - INTERVAL '2 days'", Duration.ofDays(2)),
                arguments("ts > current_timestamp() - INTERVAL '5' MINUTE", Duration.ofMinutes(5)),
                arguments("ts > LOCALTIMESTAMP - INTERVAL '5' MINUTE", Duration.ofMinutes(5)),
                arguments("now() - INTERVAL '5' MINUTE < ts", Duration.ofMinutes(5)),
                arguments("ts > (now() - INTERVAL '5' MINUTE)", Duration.ofMinutes(5)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("timeWindows")
    void aTopLevelTimeTermBecomesTheLookback(String term, Duration lookback) {
        SqlTranslation t = translate("SELECT * FROM orders WHERE event_type = 'a' AND " + term);

        assertThat(t.lookback()).isEqualTo(lookback);
        assertThat(t.selector()).isEqualTo("event_type = 'a'");
        assertThat(t.notes()).anyMatch(n -> n.contains("→ lookback " + EntityQuery.format(lookback)));
    }

    @Test
    void aTimeWindowAloneLeavesNoSelector() {
        SqlTranslation t = translate("ts > now() - INTERVAL '15' MINUTE");

        assertThat(t.selector()).isNull();
        assertThat(t.lookback()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void anAliasThatIsARealFieldStaysAPredicate() {
        // session_events has a body field ts: the filter would read it, not the publish time.
        SqlTranslation t = translate("SELECT * FROM session_events WHERE ts > 1700000000000");
        assertThat(t.selector()).isEqualTo("ts > 1700000000000");
        assertThat(t.lookback()).isNull();
        assertThat(SqlTranslator.translate("ts = 5", TOPICS, "session_events", fields).selector()).isEqualTo("ts = 5");

        // and a time window on it is an error that says so, pointing at __publish_time__
        assertThatThrownBy(() -> translate("SELECT * FROM session_events WHERE ts > now() - INTERVAL '15' MINUTE"))
                .hasMessageContaining("ts is a field of session_events").hasMessageContaining("__publish_time__");
        SqlTranslation real = translate("SELECT * FROM session_events WHERE __publish_time__ > now() - INTERVAL '15' "
                + "MINUTE AND ts > 5");
        assertThat(real.lookback()).isEqualTo(Duration.ofMinutes(15));
        assertThat(real.selector()).isEqualTo("ts > 5");
    }

    @Test
    void theTimeWindowUnderOrOrNotIsAnError() {
        for (String sql : List.of("status = 'a' OR ts > now() - INTERVAL '15' MINUTE",
                "NOT (ts > now() - INTERVAL '15' MINUTE)",
                "status = 'a' AND (status = 'b' OR ts > now() - INTERVAL '1' HOUR)")) {
            assertThatThrownBy(() -> translate(sql)).as(sql)
                    .hasMessageContaining("a time window can only be AND-ed at the top level");
        }
    }

    @Test
    void aBareConditionAsksForNoFieldsUnlessItNeedsThem() {
        translate("status = 'a' AND amount > 5");
        assertThat(asked).isEmpty();
        translate("ts > now() - INTERVAL '1' HOUR");
        assertThat(asked).containsExactly(ORDERS);
    }

    // ---------------------------------------------------------------- FROM, LIMIT, ORDER BY

    @Test
    void fromLimitAndOrderByBecomeTheToolsArguments() {
        SqlTranslation t = translate("SELECT * FROM session_events WHERE event_type = 'login' "
                + "AND __publish_time__ > now() - INTERVAL '15' MINUTE ORDER BY __publish_time__ DESC LIMIT 50");
        assertThat(t.topic()).isEqualTo(SESSIONS);
        assertThat(t.selector()).isEqualTo("event_type = 'login'");
        assertThat(t.lookback()).isEqualTo(Duration.ofMinutes(15));
        assertThat(t.maxEvents()).isEqualTo(50);
        assertThat(t.notes()).contains("LIMIT 50 → max_events 50 (the newest 50, listed oldest first)")
                .contains("FROM session_events → topic " + SESSIONS);
        assertThat(t.notes()).anyMatch(n -> n.startsWith("ORDER BY __publish_time__ DESC → the default order"));
    }

    @Test
    void aFullTopicNameInFromIsQuoted() {
        assertThat(translate("SELECT * FROM \"" + SESSIONS + "\" WHERE event_type = 'a'").topic()).isEqualTo(SESSIONS);
        assertThat(translate("SELECT * FROM `orders`").topic()).isEqualTo(ORDERS);
    }

    @Test
    void timeOrderingUsesTheAliasOnlyWhenItIsNotARealField() {
        assertThat(translate("SELECT * FROM orders ORDER BY ts DESC").selector()).isNull();
        assertThatThrownBy(() -> translate("SELECT * FROM session_events ORDER BY ts DESC"))
                .hasMessageContaining("isn't supported");
    }

    @Test
    void sqlArgumentsAgreeingWithTheCallAreFineAndConflictsAreErrors() {
        String sql = "SELECT * FROM orders WHERE status = 'ok' AND ts > now() - INTERVAL '15' MINUTE LIMIT 50";

        EntityQuery agreed = EntityQuery.parseAgainst(args("sql", sql, "topic", "orders", "lookback", "15m",
                "max_events", 50), LIMITS, TOPICS, fields);
        assertThat(agreed.topic()).isEqualTo(ORDERS);
        assertThat(agreed.lookback()).isEqualTo(Duration.ofMinutes(15));
        assertThat(agreed.maxEvents()).isEqualTo(50);
        assertThat(agreed.selector()).isEqualTo("status = 'ok'");
        assertThat(agreed.sql().sql()).isEqualTo(sql);

        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", sql, "topic", "session_events"), LIMITS, TOPICS,
                fields)).hasMessageContaining("FROM orders but topic is session_events");
        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", sql, "lookback", "1h"), LIMITS, TOPICS, fields))
                .hasMessageContaining("time window is 15m but lookback is 1h");
        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", sql, "max_events", 10), LIMITS, TOPICS, fields))
                .hasMessageContaining("LIMIT 50 but max_events is 10");
    }

    @Test
    void theSqlFillsInOnlyWhatItSays() {
        EntityQuery q = EntityQuery.parseAgainst(args("sql", "status = 'ok'", "topic", "orders", "lookback", "6h",
                "max_events", 7, "key", "o-1"), LIMITS, TOPICS, fields);

        assertThat(q.lookback()).isEqualTo(Duration.ofHours(6));
        assertThat(q.maxEvents()).isEqualTo(7);
        assertThat(q.filter()).isEqualTo("__key__ = 'o-1' AND (status = 'ok')");
    }

    @Test
    void aTimeWindowOverTheServersMaximumIsTheUsualRejection() {
        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", "status = 'ok' AND ts > now() - INTERVAL '2' DAY"),
                LIMITS, TOPICS, fields)).hasMessageContaining("the SQL's time window: lookback 2d is longer");
    }

    @Test
    void sqlAndSelectorTogetherIsAnError() {
        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", "status = 'ok'", "selector", "status = 'ok'"),
                LIMITS, TOPICS, fields)).hasMessageContaining("either sql or selector, not both");
    }

    @Test
    void aTimeWindowAloneNeedsAKey() {
        assertThatThrownBy(() -> EntityQuery.parseAgainst(args("sql", "ts > now() - INTERVAL '15' MINUTE"), LIMITS,
                TOPICS, fields)).hasMessageContaining("no condition besides the time window");
        EntityQuery q = EntityQuery.parseAgainst(args("sql", "ts > now() - INTERVAL '15' MINUTE", "key", "o-1"),
                LIMITS, TOPICS, fields);
        assertThat(q.filter()).isEqualTo("__key__ = 'o-1'");
        assertThat(q.lookback()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void aHeaderComparedWithANumberGetsTheUsualWarning() {
        EntityQuery q = EntityQuery.parseAgainst(args("sql", "SELECT * FROM orders WHERE seq > 5"), LIMITS, TOPICS,
                fields);

        assertThat(q.selector()).isEqualTo("seq > 5");
        assertThat(q.selectorWarnings()).anyMatch(w -> w.contains("seq"));
    }

    // ---------------------------------------------------------------- projection

    private static final ObjectMapper JSON = new ObjectMapper();

    private static Map<String, Object> event() throws Exception {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("event_type", "order_placed");
        props.put("seq", "3");
        JsonNode value = JSON.readTree("{\"status\":\"ok\",\"amount\":12.5,\"customer\":{\"tier\":\"gold\",\"id\":\"c1\"}}");
        return EntityEventsTool.event("o-1", props, JSON.writeValueAsBytes(value), 1_000L);
    }

    @Test
    void selectColumnsTrimEachEventKeepingTimeAndKey() throws Exception {
        SqlTranslation t = translate("SELECT event_type, customer.tier, ts, __key__ FROM orders WHERE amount > 5");

        assertThat(t.columns()).containsExactly("event_type", "customer.tier");
        Map<String, Object> p = t.project(event());
        assertThat(p).containsOnlyKeys("time", "key", "properties", "value");
        assertThat(p).containsEntry("key", "o-1");
        assertThat(p.get("properties")).isEqualTo(Map.of("event_type", "order_placed"));
        assertThat(p.get("value").toString()).isEqualTo("{\"customer\":{\"tier\":\"gold\"}}");
        assertThat(t.notes()).anyMatch(n -> n.contains("each event keeps time and key and event_type, customer.tier"));
    }

    @Test
    void selectStarAndABareConditionKeepWholeEvents() throws Exception {
        assertThat(translate("SELECT * FROM orders").columns()).isNull();
        assertThat(translate("status = 'ok'").columns()).isNull();
        Map<String, Object> e = event();
        assertThat(translate("SELECT * FROM orders").project(e)).isSameAs(e);
    }

    @Test
    void aTextValueHasNoFieldsToKeep() {
        Map<String, Object> e = EntityEventsTool.event("k", Map.of("seq", "1"), "plain".getBytes(StandardCharsets.UTF_8),
                1L);
        Map<String, Object> p = translate("SELECT seq FROM orders").project(e);
        assertThat(p.get("value")).isNull();
        assertThat(p.get("properties")).isEqualTo(Map.of("seq", "1"));
    }

    @Test
    void theTranslationDescribesWhatItSet() {
        SqlTranslation t = translate("SELECT status FROM orders WHERE amount > 5 AND ts > now() - INTERVAL '15' MINUTE "
                + "ORDER BY ts DESC LIMIT 50");
        Map<String, Object> d = t.describe();
        assertThat(d).containsEntry("selector", "amount > 5").containsEntry("topic", ORDERS)
                .containsEntry("lookback", "15m").containsEntry("max_events", 50)
                .containsEntry("columns", List.of("status")).containsKey("notes");
    }
}
