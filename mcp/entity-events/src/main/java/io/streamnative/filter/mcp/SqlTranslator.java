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

import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.IntegerDivision;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.conditional.XorExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.SimilarToExpression;
import net.sf.jsqlparser.expression.operators.relational.SupportsOldOracleJoinSyntax;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.parser.Token;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Translates an agent's SQL into what the filter runs: a JMS selector plus the tools' own arguments. Agents
 * write SQL fluently, and Confluent RTCE's {@code queryData} takes {@code SELECT … WHERE … ORDER BY … LIMIT};
 * a JMS selector is already a SQL-92 {@code WHERE} clause, so most of the work is the statement around it.
 *
 * <p>Accepted: a bare condition, {@code WHERE <condition>}, or one {@code SELECT … [FROM t] [WHERE …]
 * [ORDER BY <time> DESC] [LIMIT n]}. {@code FROM} becomes {@code topic} (through the allowlist),
 * {@code LIMIT} becomes {@code max_events}, a top-level {@code AND} term {@code <time> > now() - INTERVAL …}
 * becomes {@code lookback}, and the {@code SELECT} columns trim each returned event. The {@code WHERE} is
 * printed back out from JSqlParser's tree through a whitelist of node types; anything off it (functions,
 * {@code CASE}, {@code ILIKE}, subqueries, joins, aggregates, …) is rejected with the construct's name and
 * the nearest supported form. Nothing is approximated and no condition is dropped.
 *
 * <p>The translator adds no trust: its selector goes through {@link EntityQuery#checkSelector} like a
 * hand-written one (length, syntax with the filter's own parser, field allowlist, type hints).
 *
 * <p>The time column is {@code __publish_time__}, or one of {@link #TIME_ALIASES} when the topic has no real
 * field of that name: the filter resolves a real field first, so an alias that names a body field or header
 * stays an ordinary predicate.
 *
 * <p>Parsing uses JSqlParser's parser directly rather than {@link CCJSqlParserUtil#parse}, which runs each
 * parse on an executor thread for its timeout; the input is length-capped instead.
 */
final class SqlTranslator {

    /** Longest SQL accepted; the selector it becomes is capped again by {@link EntityQuery#MAX_SELECTOR_LENGTH}. */
    static final int MAX_SQL_LENGTH = 4000;

    static final String TIME_FIELD = "__publish_time__";
    /** Names RTCE-style SQL uses for the event time, read as {@link #TIME_FIELD} when they aren't real fields. */
    static final Set<String> TIME_ALIASES = Set.of("ts", "time", "timestamp", "publish_time");

    static final String EXAMPLE = "SELECT * FROM orders WHERE event_type = 'login' "
            + "AND ts > now() - INTERVAL '15' MINUTE ORDER BY ts DESC LIMIT 50";

    private static final String SUPPORTED = "Selectors support = <> < <= > >= AND OR NOT, IN and NOT IN of literal "
            + "values, LIKE and NOT LIKE (with ESCAPE), BETWEEN, IS [NOT] NULL, + - * /, numbers, 'strings', TRUE "
            + "and FALSE, and field names (dotted for nested fields)";
    private static final Set<String> STATEMENT_WORDS = Set.of("INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT",
            "REPLACE", "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "CALL", "EXEC", "EXECUTE", "SET",
            "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "USE", "BEGIN", "COMMIT", "ROLLBACK");
    private static final Set<String> AGGREGATES = Set.of("count", "sum", "avg", "min", "max", "array_agg",
            "string_agg", "group_concat", "listagg", "stddev", "variance", "any_value", "first_value", "last_value",
            "approx_count_distinct", "median", "percentile_cont", "percentile_disc");
    private static final Set<String> CASE_FUNCTIONS = Set.of("lower", "upper", "lcase", "ucase", "initcap");
    private static final Set<String> NOW_FUNCTIONS = Set.of("now", "current_timestamp", "localtimestamp",
            "sysdate", "getdate", "systimestamp", "current_time", "current_date");
    private static final Pattern WHERE_PREFIX = Pattern.compile("(?is)^\\s*WHERE\\b(.*)$");
    private static final Pattern FIRST_WORD = Pattern.compile("^\\s*([A-Za-z]+)");
    private static final Pattern INTERVAL_TEXT = Pattern.compile("(?i)^\\s*(\\d+)\\s*([a-z]+)\\s*$");
    private static final Pattern DIGITS = Pattern.compile("^\\s*(\\d+)\\s*$");
    private static final int MAX_SHOWN = 60;
    private static final int MAX_EXAMPLE_SPELLINGS = 5;

    // Precedence levels, matching the selector grammar (common/.../selector/Parser): a child printed below
    // the level its parent needs is parenthesised.
    private static final int OR = 1;
    private static final int AND = 2;
    private static final int NOT = 3;
    private static final int PREDICATE = 4;
    private static final int ADDITIVE = 5;
    private static final int MULTIPLICATIVE = 6;
    private static final int UNARY = 7;
    private static final int PRIMARY = 8;

    private record Printed(String text, int level) {
    }

    private final Topics topics;
    private final Object explicitTopic;
    private final java.util.function.Function<String, FieldCatalog.Fields> fieldsOf;
    private final List<String> notes = new ArrayList<>();
    /** The IN lists taken out before parsing ({@link #hideInLists}), by placeholder number. */
    private final List<String> inLists = new ArrayList<>();
    private String fromTopic;
    private FieldCatalog.Fields fields;
    private Duration lookback;
    private String lookbackTerm;

    private SqlTranslator(Topics topics, Object explicitTopic,
                          java.util.function.Function<String, FieldCatalog.Fields> fieldsOf) {
        this.topics = topics;
        this.explicitTopic = explicitTopic;
        this.fieldsOf = fieldsOf;
    }

    /**
     * Translate {@code sql}. {@code explicitTopic} is the call's own {@code topic} argument (may be null): the
     * topic whose fields decide whether a time alias is a real field, when the SQL has no {@code FROM}.
     * {@code fieldsOf} is asked only when the SQL needs it (a time alias, a column list, a case-insensitive
     * comparison to suggest spellings for).
     *
     * @throws IllegalArgumentException naming what can't be translated and the nearest supported form
     * @throws IllegalStateException    if the topic's fields can't be read
     */
    static SqlTranslation translate(String sql, Topics topics, Object explicitTopic,
                                    java.util.function.Function<String, FieldCatalog.Fields> fieldsOf) {
        return new SqlTranslator(topics, explicitTopic, fieldsOf).run(sql);
    }

    // ---------------------------------------------------------------- the statement

    private SqlTranslation run(String sql) {
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("sql must be a non-empty string, for example " + EXAMPLE);
        }
        if (sql.length() > MAX_SQL_LENGTH) {
            throw new IllegalArgumentException("sql is " + sql.length() + " characters; the most this server accepts "
                    + "is " + MAX_SQL_LENGTH + ". Use a shorter query");
        }
        String text = sql.strip();
        while (text.endsWith(";")) {
            text = text.substring(0, text.length() - 1).stripTrailing();
        }
        if (text.isEmpty()) {
            throw new IllegalArgumentException("sql must be a non-empty string, for example " + EXAMPLE);
        }
        text = hideInLists(text);
        Matcher first = FIRST_WORD.matcher(text);
        String word = first.find() ? first.group(1).toUpperCase(Locale.ROOT) : "";
        if (word.equals("SELECT") || word.equals("WITH") || word.equals("VALUES") || word.equals("TABLE")) {
            return statement(sql, text);
        }
        if (STATEMENT_WORDS.contains(word)) {
            throw new IllegalArgumentException(word + " statements aren't supported: sql is a read, either a SELECT "
                    + "query (" + EXAMPLE + ") or a bare WHERE condition");
        }
        Matcher where = WHERE_PREFIX.matcher(text);
        String condition = where.matches() ? where.group(1) : text;
        if (condition.isBlank()) {
            throw new IllegalArgumentException("sql has WHERE but no condition after it; for example "
                    + "WHERE event_type = 'login'");
        }
        Expression e = expression(condition);
        notes.add("A bare condition: it is the WHERE clause");
        String selector = where(e);
        return new SqlTranslation(sql, null, selector, lookback, null, null, notes);
    }

    private SqlTranslation statement(String sql, String text) {
        Statements statements;
        try {
            statements = CCJSqlParserUtil.newParser(text).Statements();
        } catch (ParseException | RuntimeException | StackOverflowError e) {
            throw unparseable(text, e);
        }
        if (statements == null || statements.isEmpty()) {
            throw new IllegalArgumentException("sql holds no statement; for example " + EXAMPLE);
        }
        if (statements.size() > 1) {
            throw multipleStatements();
        }
        Statement st = statements.get(0);
        if (st instanceof SetOperationList) {
            throw reject("UNION, INTERSECT and EXCEPT (set operations) aren't supported: one SELECT reads one topic. "
                    + "Combine the conditions with OR instead, for example WHERE event_type = 'a' OR event_type = 'b'");
        }
        if (st instanceof Select s && s.getWithItemsList() != null && !s.getWithItemsList().isEmpty()) {
            throw reject("WITH (common table expressions) isn't supported: write one SELECT … FROM <topic> WHERE … "
                    + "with literal values");
        }
        if (st instanceof ParenthesedSelect) {
            throw reject("a parenthesised query isn't supported: write the SELECT without the outer parentheses");
        }
        if (!(st instanceof PlainSelect ps)) {
            throw reject("only a SELECT query or a bare WHERE condition can be translated; for example " + EXAMPLE);
        }
        return select(sql, ps);
    }

    private SqlTranslation select(String sql, PlainSelect ps) {
        if (ps.getDistinct() != null) {
            throw reject("DISTINCT isn't supported: the tools return events, not distinct values. Drop DISTINCT; "
                    + "each event comes back once (the tools deduplicate redeliveries)");
        }
        if (ps.getGroupBy() != null || ps.getHaving() != null) {
            throw aggregates();
        }
        if (ps.getJoins() != null && !ps.getJoins().isEmpty()) {
            throw reject("joins aren't supported: one call reads one topic. Read each topic with its own call");
        }
        if (ps.getOffset() != null || (ps.getLimit() != null && ps.getLimit().getOffset() != null)) {
            throw reject("OFFSET isn't supported: the tools return the newest matching events. To see further back, "
                    + "raise LIMIT (max_events); matched and more_events say how many there were");
        }
        if (ps.getTop() != null || ps.getFetch() != null) {
            throw reject("TOP and FETCH FIRST aren't supported; write LIMIT n, for example " + EXAMPLE);
        }
        if (ps.getIntoTables() != null || ps.getForMode() != null || ps.getForUpdateTable() != null) {
            throw reject("SELECT … INTO and FOR UPDATE aren't supported: sql is a read; for example " + EXAMPLE);
        }

        // FROM first: the topic decides which fields are real (time aliases, the column list).
        FromItem from = ps.getFromItem();
        if (from != null) {
            if (!(from instanceof Table table)) {
                throw subquery();
            }
            if (table.getAlias() != null) {
                throw reject("table aliases (FROM " + table.getFullyQualifiedName() + table.getAlias() + ") aren't "
                        + "supported: name the topic alone and write field names without a prefix");
            }
            String name = unquote(table.getFullyQualifiedName());
            fromTopic = topics.resolve(name);
            notes.add("FROM " + name + " → topic " + fromTopic);
        }

        List<String> columns = columns(ps.getSelectItems());
        Integer maxEvents = limit(ps.getLimit());
        orderBy(ps.getOrderByElements());

        String selector = null;
        if (ps.getWhere() != null) {
            selector = where(ps.getWhere());
        }

        // A last guard: the SELECT as rebuilt from the parts translated above must be the whole statement, so a
        // clause this translator doesn't know (QUALIFY, WINDOW, a dialect's extension) is rejected, not dropped.
        String rebuilt = rebuild(ps);
        if (!rebuilt.equals(ps.toString())) {
            throw reject("the query has a clause this server doesn't translate (" + shown(ps.toString()) + "); "
                    + "use only SELECT, FROM, WHERE, ORDER BY <time> DESC and LIMIT, for example " + EXAMPLE);
        }
        return new SqlTranslation(sql, fromTopic, selector, lookback, maxEvents, columns, notes);
    }

    private static String rebuild(PlainSelect ps) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(ps.getSelectItems().stream().map(Object::toString).collect(Collectors.joining(", ")));
        if (ps.getFromItem() != null) {
            sb.append(" FROM ").append(ps.getFromItem());
        }
        if (ps.getWhere() != null) {
            sb.append(" WHERE ").append(ps.getWhere());
        }
        if (ps.getOrderByElements() != null && !ps.getOrderByElements().isEmpty()) {
            sb.append(" ORDER BY ").append(ps.getOrderByElements().stream().map(Object::toString)
                    .collect(Collectors.joining(", ")));
        }
        if (ps.getLimit() != null) {
            sb.append(ps.getLimit());
        }
        return sb.toString();
    }

    /** The column list: {@code null} for {@code *}; otherwise the fields to keep (time and key always stay). */
    private List<String> columns(List<SelectItem<?>> items) {
        boolean star = false;
        Set<String> names = new LinkedHashSet<>();
        Set<String> builtIns = new LinkedHashSet<>();
        for (SelectItem<?> item : items) {
            if (item.getAlias() != null) {
                throw reject("column aliases (" + shown(item.toString()) + ") aren't supported: name the field "
                        + "itself, for example SELECT event_type, amount FROM orders");
            }
            Expression e = item.getExpression();
            if (e instanceof AllColumns || e instanceof AllTableColumns) {
                if (!e.toString().endsWith("*")) { // * EXCEPT (…), * REPLACE (…)
                    throw reject(shown(e.toString()) + " isn't supported; write SELECT * or name the fields");
                }
                star = true;
            } else if (e instanceof Column c) {
                String name = columnName(c);
                if (isTimeColumn(name)) {
                    builtIns.add(name);
                } else if (name.equals("__key__")) {
                    builtIns.add(name);
                } else {
                    names.add(name);
                }
            } else if (e instanceof Function f && AGGREGATES.contains(f.getName().toLowerCase(Locale.ROOT))) {
                throw aggregates();
            } else if (e instanceof AnalyticExpression) {
                throw aggregates();
            } else {
                throw reject("SELECT " + shown(e.toString()) + " isn't supported: SELECT takes * or field names, "
                        + "and each returned event is trimmed to those fields (time and key always stay)");
            }
        }
        if (star) {
            notes.add("SELECT * → whole events");
            return null;
        }
        // Names the topic doesn't have would only ever come back empty: say so now, as the selector check would.
        FieldCatalog.Fields f = fields();
        Set<String> known = f.names();
        List<String> unknown = names.stream().filter(n -> !known.contains(n)).toList();
        if (!unknown.isEmpty()) {
            String shortName = Topics.shortName(f.topic());
            StringBuilder sb = new StringBuilder("SELECT names ").append(unknown.size() == 1 ? "a field" : "fields")
                    .append(" that ").append(shortName).append(" doesn't have: ").append(String.join(", ", unknown))
                    .append(". ");
            for (String name : unknown) {
                SelectorAdvice.suggest(name, known).ifPresent(s -> sb.append("For ").append(name)
                        .append(", did you mean ").append(s).append("? "));
            }
            throw new IllegalArgumentException(sb.append("Call get_schema with topic \"").append(shortName)
                    .append("\" for the fields you can use, or write SELECT *").toString());
        }
        List<String> kept = List.copyOf(names);
        String note = "SELECT " + String.join(", ", items.stream().map(Object::toString).toList())
                + " → each event keeps time and key" + (kept.isEmpty() ? " only" : " and " + String.join(", ", kept));
        if (!builtIns.isEmpty()) {
            note += " (" + String.join(", ", builtIns) + (builtIns.size() == 1 ? " is" : " are")
                    + " always returned, as time and key)";
        }
        notes.add(note);
        return kept;
    }

    private Integer limit(Limit limit) {
        if (limit == null) {
            return null;
        }
        if (limit.isLimitAll() || limit.isLimitNull() || limit.getByExpressions() != null) {
            throw reject(shown(limit.toString().strip()) + " isn't supported; write LIMIT n with n at least 1, or leave "
                    + "LIMIT out (the default is " + EntityQuery.DEFAULT_MAX_EVENTS + " events)");
        }
        Expression n = limit.getRowCount();
        if (!(n instanceof LongValue v)) {
            throw reject("LIMIT takes a whole number, for example LIMIT 50 (got " + shown(String.valueOf(n)) + ")");
        }
        long count = v.getValue();
        if (count < 1) {
            throw reject("LIMIT must be at least 1 (got " + count + ")");
        }
        int max = (int) Math.min(count, Integer.MAX_VALUE);
        notes.add("LIMIT " + count + " → max_events " + max + " (the newest " + max + ", listed oldest first)");
        return max;
    }

    private void orderBy(List<OrderByElement> order) {
        if (order == null || order.isEmpty()) {
            return;
        }
        for (OrderByElement o : order) {
            boolean ok = o.getExpression() instanceof Column c && isTimeColumn(columnName(c)) && !o.isAsc()
                    && o.getNullOrdering() == null && order.size() == 1;
            if (!ok) {
                throw reject("ORDER BY " + shown(order.stream().map(Object::toString)
                        .collect(Collectors.joining(", "))) + " isn't supported: the tools always return the newest "
                        + "matching events, listed oldest first. The only ordering accepted is ORDER BY <time> DESC "
                        + "(ts, timestamp or __publish_time__), or leave ORDER BY out");
            }
        }
        notes.add("ORDER BY " + order.get(0) + " → the default order (the newest events, listed oldest first)");
    }

    // ---------------------------------------------------------------- WHERE

    private Expression expression(String condition) {
        CCJSqlParser parser = CCJSqlParserUtil.newParser(condition);
        Expression e;
        Token next;
        try {
            e = parser.Expression();
            next = parser.getNextToken();
        } catch (ParseException | RuntimeException | StackOverflowError ex) {
            throw unparseable(condition, ex);
        }
        if (next != null && next.kind != 0) { // 0 is EOF
            if (";".equals(next.image)) {
                throw multipleStatements();
            }
            String image = next.image == null ? "" : next.image.toUpperCase(Locale.ROOT);
            String hint = Set.of("ORDER", "LIMIT", "GROUP", "HAVING", "FROM", "OFFSET").contains(image)
                    ? " " + image + " needs a full query: SELECT * FROM <topic> WHERE … ORDER BY ts DESC LIMIT n"
                    : "";
            throw new IllegalArgumentException("couldn't parse the SQL: unexpected \"" + next.image + "\" at column "
                    + next.beginColumn + " after the condition " + shown(e.toString()) + "." + hint
                    + " Write a bare condition (event_type = 'login' AND amount > 100) or a query such as " + EXAMPLE);
        }
        return e;
    }

    /** The selector the {@code WHERE} becomes, after taking out a top-level time window; null if nothing is left. */
    private String where(Expression where) {
        List<Expression> terms = new ArrayList<>();
        conjuncts(where, terms);
        List<Expression> kept = new ArrayList<>();
        for (Expression term : terms) {
            Optional<Duration> window = timeWindow(term);
            if (window.isPresent()) {
                if (lookback != null) {
                    throw reject("the SQL has two time windows (" + lookbackTerm + " and " + shown(term.toString())
                            + "); keep one, for example ts > now() - INTERVAL '15' MINUTE");
                }
                lookback = window.get();
                lookbackTerm = term.toString();
                notes.add(term + " → lookback " + EntityQuery.format(lookback) + ", taken out of the selector");
                continue;
            }
            kept.add(term);
        }
        if (kept.isEmpty()) {
            return null;
        }
        // One term needs no parentheses; several are AND-ed, so an OR among them is parenthesised.
        String selector = kept.size() == 1 ? print(kept.get(0)).text()
                : kept.stream().map(t -> at(t, AND)).collect(Collectors.joining(" AND "));
        notes.add("WHERE → selector " + selector);
        return selector;
    }

    /** The top-level AND terms, looking through parentheses. */
    private static void conjuncts(Expression e, List<Expression> out) {
        Expression inner = unwrap(e);
        if (inner instanceof AndExpression and) {
            conjuncts(and.getLeftExpression(), out);
            conjuncts(and.getRightExpression(), out);
        } else {
            out.add(inner);
        }
    }

    /** One parenthesised expression, without its parentheses; anything else as it is. */
    private static Expression unwrap(Expression e) {
        Expression cur = e;
        while (cur instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
            cur = list.get(0);
        }
        return cur;
    }

    private String at(Expression e, int min) {
        Printed p = print(e);
        return p.level() < min ? "(" + p.text() + ")" : p.text();
    }

    private Printed print(Expression raw) {
        Expression e = raw;
        if (e instanceof ParenthesedExpressionList<?> list) {
            if (list.size() == 1) {
                return print(list.get(0));
            }
            throw reject("a list of values " + shown(e.toString()) + " can only follow IN, for example "
                    + "event_type IN ('a', 'b')");
        }
        if (isTimeWindowShape(e)) {
            throw reject("a time window can only be AND-ed at the top level: " + shown(e.toString()) + " is inside OR "
                    + "or NOT, so it can't become the lookback. Move it out, for example "
                    + "ts > now() - INTERVAL '15' MINUTE AND (event_type = 'a' OR event_type = 'b')");
        }
        if (e instanceof AndExpression a) {
            return new Printed(at(a.getLeftExpression(), AND) + " AND " + at(a.getRightExpression(), AND), AND);
        }
        if (e instanceof OrExpression o) {
            return new Printed(at(o.getLeftExpression(), OR) + " OR " + at(o.getRightExpression(), OR), OR);
        }
        if (e instanceof XorExpression) {
            throw reject("XOR isn't supported; write (a AND NOT b) OR (NOT a AND b)");
        }
        if (e instanceof NotExpression n) {
            return new Printed("NOT " + at(n.getExpression(), NOT), NOT);
        }
        if (e instanceof ComparisonOperator c) {
            return comparison(c);
        }
        if (e instanceof Between b) {
            return new Printed(at(b.getLeftExpression(), ADDITIVE) + (b.isNot() ? " NOT BETWEEN " : " BETWEEN ")
                    + at(b.getBetweenExpressionStart(), ADDITIVE) + " AND " + at(b.getBetweenExpressionEnd(), ADDITIVE),
                    PREDICATE);
        }
        if (e instanceof InExpression in) {
            return in(in);
        }
        if (e instanceof LikeExpression like) {
            return like(like);
        }
        if (e instanceof IsNullExpression isNull) {
            boolean not = isNull.isNot() || isNull.isUseNotNull();
            return new Printed(at(isNull.getLeftExpression(), ADDITIVE) + (not ? " IS NOT NULL" : " IS NULL"), PREDICATE);
        }
        if (e instanceof IsBooleanExpression) {
            throw reject(shown(e.toString()) + " isn't supported; compare with = instead, for example flag = TRUE");
        }
        if (e instanceof Addition || e instanceof Subtraction) {
            var b = (net.sf.jsqlparser.expression.BinaryExpression) e;
            if (containsNow(b)) {
                throw nowOutsideWindow(e);
            }
            return new Printed(at(b.getLeftExpression(), ADDITIVE) + (e instanceof Addition ? " + " : " - ")
                    + at(b.getRightExpression(), MULTIPLICATIVE), ADDITIVE);
        }
        if (e instanceof Multiplication || e instanceof Division) {
            var b = (net.sf.jsqlparser.expression.BinaryExpression) e;
            return new Printed(at(b.getLeftExpression(), MULTIPLICATIVE) + (e instanceof Multiplication ? " * " : " / ")
                    + at(b.getRightExpression(), UNARY), MULTIPLICATIVE);
        }
        if (e instanceof Concat) {
            throw reject("|| (string concatenation) isn't supported: compare the field with one literal, or match "
                    + "part of it with LIKE, for example name LIKE 'abc%'");
        }
        if (e instanceof Modulo || e instanceof IntegerDivision) {
            throw reject(shown(e.toString()) + ": % and DIV aren't supported; selectors have + - * / only");
        }
        if (e instanceof SignedExpression s) {
            if (s.getSign() != '-' && s.getSign() != '+') {
                throw reject("the operator " + s.getSign() + " in " + shown(e.toString()) + " isn't supported. "
                        + SUPPORTED);
            }
            String inner = at(s.getExpression(), UNARY);
            String sep = inner.startsWith("-") || inner.startsWith("+") ? " " : "";
            return new Printed(s.getSign() + sep + inner, UNARY);
        }
        if (e instanceof LongValue v) {
            return new Printed(v.getStringValue(), PRIMARY);
        }
        if (e instanceof DoubleValue d) {
            return new Printed(d.toString(), PRIMARY);
        }
        if (e instanceof StringValue s) {
            return new Printed(string(s), PRIMARY);
        }
        if (e instanceof BooleanValue b) {
            return new Printed(b.getValue() ? "TRUE" : "FALSE", PRIMARY);
        }
        if (e instanceof NullValue) {
            throw reject("NULL can only be tested with IS NULL or IS NOT NULL, for example coupon IS NULL");
        }
        if (e instanceof Column c) {
            return new Printed(field(c), PRIMARY);
        }
        if (e instanceof HexValue) {
            throw reject("hex literals (" + shown(e.toString()) + ") aren't supported; write the number in decimal");
        }
        throw unsupported(e);
    }

    private Printed comparison(ComparisonOperator c) {
        if (((SupportsOldOracleJoinSyntax) c).getOldOracleJoinSyntax() != SupportsOldOracleJoinSyntax.NO_ORACLE_JOIN
                || ((SupportsOldOracleJoinSyntax) c).getOraclePriorPosition() != SupportsOldOracleJoinSyntax.NO_ORACLE_PRIOR) {
            throw reject("Oracle join syntax (" + shown(c.toString()) + ") isn't supported");
        }
        String op;
        if (c instanceof EqualsTo) {
            op = "=";
        } else if (c instanceof NotEqualsTo) {
            op = "<>";
        } else if (c instanceof GreaterThan) {
            op = ">";
        } else if (c instanceof GreaterThanEquals) {
            op = ">=";
        } else if (c instanceof MinorThan) {
            op = "<";
        } else if (c instanceof MinorThanEquals) {
            op = "<=";
        } else {
            throw unsupported(c);
        }
        Expression l = unwrap(c.getLeftExpression());
        Expression r = unwrap(c.getRightExpression());
        if (c instanceof EqualsTo && c.getRightExpression() instanceof Column list && inList(list) >= 0) {
            return in(c.getLeftExpression(), inList(list), list.getColumnName().startsWith(NOT_IN_MARK));
        }
        if (l instanceof NullValue || r instanceof NullValue) {
            throw reject(shown(c.toString()) + " never matches: nothing equals NULL. Test it with IS NULL or IS NOT "
                    + "NULL, for example coupon IS NULL");
        }
        if (l instanceof AnyComparisonExpression || r instanceof AnyComparisonExpression) {
            throw subquery();
        }
        if (caseFolded(l) || caseFolded(r)) {
            throw caseInsensitive(c, l, r);
        }
        return new Printed(at(l, ADDITIVE) + " " + op + " " + at(r, ADDITIVE), PREDICATE);
    }

    /** An IN that wasn't hidden before parsing ({@link #hideInLists} leaves subqueries): only to reject it. */
    private Printed in(InExpression in) {
        Expression right = in.getRightExpression();
        if (right instanceof Select || right instanceof ParenthesedSelect
                || (right instanceof net.sf.jsqlparser.expression.BinaryExpression b
                && unwrap(leftmost(b)) instanceof Select)) {
            throw subquery();
        }
        throw reject("IN takes a parenthesised list of literal values, for example event_type IN ('a', 'b') "
                + "(got " + shown(in.toString()) + ")");
    }

    private static Expression leftmost(net.sf.jsqlparser.expression.BinaryExpression b) {
        Expression cur = b;
        while (cur instanceof net.sf.jsqlparser.expression.BinaryExpression bin) {
            cur = bin.getLeftExpression();
        }
        return cur;
    }

    /** {@code left [NOT] IN (list)}, with the list as it was written (taken out before parsing). */
    private Printed in(Expression leftExpression, int index, boolean not) {
        String original = (not ? "NOT IN (" : "IN (") + inLists.get(index) + ")";
        ExpressionList<?> list;
        try {
            CCJSqlParser parser = CCJSqlParserUtil.newParser("__in__ IN (" + inLists.get(index) + ")");
            Expression parsed = parser.Expression();
            Token next = parser.getNextToken();
            if (!(parsed instanceof InExpression in) || !(in.getRightExpression() instanceof ExpressionList<?> l)
                    || l.isEmpty() || (next != null && next.kind != 0)) {
                throw reject("IN takes a parenthesised list of literal values, for example event_type IN ('a', 'b') "
                        + "(got " + shown(original) + ")");
            }
            list = l;
        } catch (ParseException | RuntimeException | StackOverflowError e) {
            if (e instanceof IllegalArgumentException iae) {
                throw iae;
            }
            throw reject("IN takes a parenthesised list of literal values, for example event_type IN ('a', 'b') "
                    + "(got " + shown(original) + ")");
        }
        Expression leftUnwrapped = unwrap(leftExpression);
        if (caseFolded(leftUnwrapped)) {
            throw caseInsensitive(leftExpression, leftUnwrapped, null);
        }
        String left = at(leftExpression, ADDITIVE);
        List<String> values = new ArrayList<>();
        boolean allStrings = true;
        for (Object o : list) {
            Expression v = unwrap((Expression) o);
            if (v instanceof StringValue s) {
                values.add(string(s));
            } else if (v instanceof LongValue || v instanceof DoubleValue || v instanceof BooleanValue
                    || (v instanceof SignedExpression s && (s.getExpression() instanceof LongValue
                    || s.getExpression() instanceof DoubleValue))) {
                values.add(print(v).text());
                allStrings = false;
            } else if (v instanceof NullValue) {
                throw reject("IN (…, NULL) never matches NULL; test it separately with IS NULL, for example "
                        + "(event_type IN ('a', 'b') OR event_type IS NULL)");
            } else if (v instanceof Select || v instanceof ParenthesedSelect) {
                throw subquery();
            } else {
                throw reject("IN takes literal values only, for example event_type IN ('a', 'b'); "
                        + shown(v.toString()) + " isn't one");
            }
        }
        if (allStrings) {
            return new Printed(left + (not ? " NOT IN (" : " IN (") + String.join(", ", values) + ")", PREDICATE);
        }
        // A selector's IN takes strings only; numbers and booleans are the same test written as equalities.
        String chain = values.stream().map(v -> left + " = " + v).collect(Collectors.joining(" OR "));
        notes.add(left + " " + original + " has non-string values, which a selector's IN can't hold; written as "
                + (not ? "NOT (" + chain + ")" : chain));
        return not ? new Printed("NOT (" + chain + ")", NOT) : new Printed(chain, values.size() == 1 ? PREDICATE : OR);
    }

    // JSqlParser 5.3 parses the right side of IN as a whole expression, so in "x IN ('a') AND y = 1" the
    // "AND y = 1" ends up inside the IN's list, and any AND, OR or comparison after an IN binds wrongly. So
    // before parsing, each "[NOT] IN (<list>)" is replaced with "= <placeholder>", which parses with the right
    // precedence (IN and = are both predicates), and the list is put back when the selector is printed. A
    // subquery (IN (SELECT …)) is left in place: it is rejected anyway.
    private static final String IN_MARK = "__sql_in_";
    private static final String NOT_IN_MARK = "__sql_notin_";
    private static final Pattern PLACEHOLDER = Pattern.compile("= (__sql_(?:not)?in_)(\\d+)__");
    private static final Pattern TRAILING_NOT = Pattern.compile("(?is)^(.*\\W|)NOT\\s*$");

    private String hideInLists(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        int i = 0;
        int n = sql.length();
        while (i < n) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`' || c == '[') {
                int end = skipQuoted(sql, i);
                out.append(sql, i, end);
                i = end;
            } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                end = end < 0 ? n : end;
                out.append(sql, i, end);
                i = end;
            } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                end = end < 0 ? n : end + 2;
                out.append(sql, i, end);
                i = end;
            } else if (Character.isLetter(c) || c == '_' || c == '$') {
                int end = i;
                while (end < n && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_'
                        || sql.charAt(end) == '$')) {
                    end++;
                }
                String word = sql.substring(i, end);
                int open = end;
                while (open < n && Character.isWhitespace(sql.charAt(open))) {
                    open++;
                }
                int close = word.equalsIgnoreCase("IN") && open < n && sql.charAt(open) == '(' ? closing(sql, open) : -1;
                String content = close < 0 ? null : sql.substring(open + 1, close);
                if (content == null || Pattern.compile("(?is)^\\s*(SELECT|WITH|\\()").matcher(content).find()) {
                    out.append(word);
                    i = end;
                    continue;
                }
                Matcher not = TRAILING_NOT.matcher(out);
                boolean negated = not.matches();
                if (negated) {
                    out.setLength(not.group(1).length());
                    if (out.length() > 0 && !Character.isWhitespace(out.charAt(out.length() - 1))) {
                        out.append(' ');
                    }
                }
                out.append("= ").append(negated ? NOT_IN_MARK : IN_MARK).append(inLists.size()).append("__");
                inLists.add(content);
                i = close + 1;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** Past the quoted text starting at {@code i}: a string ({@code ''} inside it is a quote) or a quoted name. */
    private static int skipQuoted(String s, int i) {
        char open = s.charAt(i);
        char close = open == '[' ? ']' : open;
        int j = i + 1;
        while (j < s.length()) {
            if (s.charAt(j) == close) {
                if (open == '\'' && j + 1 < s.length() && s.charAt(j + 1) == '\'') {
                    j += 2;
                    continue;
                }
                return j + 1;
            }
            j++;
        }
        return s.length();
    }

    /** The index of the ')' matching the '(' at {@code open}, or -1. */
    private static int closing(String s, int open) {
        int depth = 0;
        int j = open;
        while (j < s.length()) {
            char c = s.charAt(j);
            if (c == '\'' || c == '"' || c == '`') {
                j = skipQuoted(s, j);
                continue;
            }
            if (c == '(') {
                depth++;
            } else if (c == ')' && --depth == 0) {
                return j;
            }
            j++;
        }
        return -1;
    }

    /** The placeholder's list number, or -1 if {@code c} isn't one. */
    private int inList(Column c) {
        String name = c.getColumnName();
        if (c.getTable() == null && (name.startsWith(IN_MARK) || name.startsWith(NOT_IN_MARK)) && name.endsWith("__")) {
            try {
                int index = Integer.parseInt(name.substring(name.startsWith(IN_MARK) ? IN_MARK.length()
                        : NOT_IN_MARK.length(), name.length() - 2));
                return index < inLists.size() ? index : -1;
            } catch (NumberFormatException e) {
                return -1;
            }
        }
        return -1;
    }

    /** Text with the placeholders turned back into the IN lists they stand for, for messages. */
    private String restore(String text) {
        Matcher m = PLACEHOLDER.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            int index = Integer.parseInt(m.group(2));
            String list = index < inLists.size() ? inLists.get(index) : "…";
            m.appendReplacement(sb, Matcher.quoteReplacement((m.group(1).equals(NOT_IN_MARK) ? "NOT IN (" : "IN (")
                    + list + ")"));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private Printed like(LikeExpression like) {
        LikeExpression.KeyWord kw = like.getLikeKeyWord();
        if (kw == LikeExpression.KeyWord.ILIKE || like.isCaseInsensitive()) {
            throw caseInsensitive(like, unwrap(like.getLeftExpression()), unwrap(like.getRightExpression()));
        }
        if (kw != LikeExpression.KeyWord.LIKE) {
            throw regex(like);
        }
        if (like.isUseBinary()) {
            throw reject("LIKE BINARY isn't supported; LIKE is already case-sensitive, so write " +
                    "field LIKE 'pattern'");
        }
        if (caseFolded(unwrap(like.getLeftExpression()))) {
            throw caseInsensitive(like, unwrap(like.getLeftExpression()), unwrap(like.getRightExpression()));
        }
        Expression pattern = unwrap(like.getRightExpression());
        if (!(pattern instanceof StringValue p)) {
            throw reject("LIKE takes a 'quoted' pattern, for example event_type LIKE '3ds_%' (got "
                    + shown(String.valueOf(pattern)) + ")");
        }
        StringBuilder sb = new StringBuilder(at(like.getLeftExpression(), ADDITIVE))
                .append(like.isNot() ? " NOT LIKE " : " LIKE ").append(string(p));
        if (like.getEscape() != null) {
            Expression esc = unwrap(like.getEscape());
            if (!(esc instanceof StringValue s) || s.getNotExcapedValue().length() != 1) {
                throw reject("ESCAPE takes one character in quotes, for example ESCAPE '!' (got "
                        + shown(String.valueOf(esc)) + ")");
            }
            sb.append(" ESCAPE ").append(string(s));
        }
        return new Printed(sb.toString(), PREDICATE);
    }

    /** A single-quoted JMS string: the literal's text with each {@code '} doubled. */
    private String string(StringValue s) {
        if (s.getPrefix() != null) {
            throw new IllegalArgumentException("prefixed string literals (" + shown(s.toString()) + ") aren't "
                    + "supported; write a plain 'string'");
        }
        return "'" + s.getNotExcapedValue().replace("'", "''") + "'";
    }

    // ---------------------------------------------------------------- names

    private String field(Column c) {
        if (c.getArrayConstructor() != null) {
            throw reject("array indexes (" + shown(c.toString()) + ") aren't supported: arrays can't be named in a "
                    + "selector. Call get_schema for the fields you can use");
        }
        if (inList(c) >= 0) {
            throw reject("IN (…) can only be used as a condition, for example event_type IN ('a', 'b')");
        }
        String name = columnName(c);
        if (aliasOnly(name)) {
            throw reject(name + " isn't a field of " + Topics.shortName(fields().topic()) + "; as the time column it "
                    + "can only appear in a time window, AND-ed at the top level: " + name
                    + " > now() - INTERVAL '15' MINUTE. To compare the time with epoch milliseconds, use "
                    + TIME_FIELD);
        }
        return name;
    }

    /**
     * A column's name as a selector names it: its parts unquoted (double quotes, backticks or brackets) and
     * joined with dots. A name the selector grammar can't express is rejected here.
     */
    private static String columnName(Column c) {
        return name(c.getFullyQualifiedName());
    }

    static String name(String qualified) {
        String name = unquote(qualified);
        if (!FieldCatalog.nameable(name)) {
            throw new IllegalArgumentException("the " + (name.equals(qualified) ? "" : "quoted ") + "name " + qualified
                    + " can't be used in a selector: field names must be plain identifiers (letters, digits, _ and $, "
                    + "dots for nested fields) and not keywords. Call get_schema for the fields you can filter on");
        }
        return name;
    }

    /** A dotted name with each part's quotes (double quotes, backticks or brackets) taken off. */
    static String unquote(String qualified) {
        List<String> parts = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        int i = 0;
        while (i < qualified.length()) {
            char ch = qualified.charAt(i);
            char close = ch == '"' ? '"' : ch == '`' ? '`' : ch == '[' ? ']' : 0;
            if (close != 0 && cur.length() == 0) {
                int end = qualified.indexOf(close, i + 1);
                if (end < 0) {
                    end = qualified.length();
                }
                cur.append(qualified, i + 1, end);
                i = end + 1;
            } else if (ch == '.') {
                parts.add(cur.toString());
                cur.setLength(0);
                i++;
            } else {
                cur.append(ch);
                i++;
            }
        }
        parts.add(cur.toString());
        return String.join(".", parts);
    }

    // ---------------------------------------------------------------- the time window

    /** The topic's fields, read once: the FROM topic, else the call's topic, else the default. */
    private FieldCatalog.Fields fields() {
        if (fields == null) {
            String topic = fromTopic != null ? fromTopic : topics.resolve(explicitTopic);
            fields = fieldsOf.apply(topic);
        }
        return fields;
    }

    private boolean isTimeColumn(String name) {
        return name.equals(TIME_FIELD) || aliasOnly(name);
    }

    /** A time alias ({@code ts}, {@code timestamp}, …) that isn't a real field on the topic. */
    private boolean aliasOnly(String name) {
        return TIME_ALIASES.contains(name.toLowerCase(Locale.ROOT)) && !fields().names().contains(name);
    }

    /** {@code <time> > now() - INTERVAL …} (or {@code >=}, or written the other way round), with the time column. */
    private boolean isTimeWindowShape(Expression e) {
        return timeWindowParts(e) != null;
    }

    /** [the column, the interval] of a time-window comparison, or null. */
    private Expression[] timeWindowParts(Expression raw) {
        Expression e = unwrap(raw);
        Expression column;
        Expression bound;
        if (e instanceof GreaterThan || e instanceof GreaterThanEquals) {
            column = unwrap(((ComparisonOperator) e).getLeftExpression());
            bound = unwrap(((ComparisonOperator) e).getRightExpression());
        } else if (e instanceof MinorThan || e instanceof MinorThanEquals) {
            column = unwrap(((ComparisonOperator) e).getRightExpression());
            bound = unwrap(((ComparisonOperator) e).getLeftExpression());
        } else {
            return null;
        }
        if (!(column instanceof Column c) || !(bound instanceof Subtraction s)) {
            return null;
        }
        if (!isNow(unwrap(s.getLeftExpression())) || !(unwrap(s.getRightExpression()) instanceof IntervalExpression)) {
            return null;
        }
        String name = columnName(c);
        if (!TIME_ALIASES.contains(name.toLowerCase(Locale.ROOT)) && !name.equals(TIME_FIELD)) {
            return null;
        }
        if (!isTimeColumn(name)) {
            throw reject(name + " is a field of " + Topics.shortName(fields().topic()) + ", so " + shown(e.toString())
                    + " is a condition on that field, not the time window, and now() can't be used in one. For the "
                    + "time the event was received, write " + TIME_FIELD + " > now() - INTERVAL '15' MINUTE, or pass "
                    + "lookback");
        }
        return new Expression[]{column, unwrap(s.getRightExpression())};
    }

    private Optional<Duration> timeWindow(Expression term) {
        Expression[] parts = timeWindowParts(term);
        if (parts == null) {
            return Optional.empty();
        }
        return Optional.of(interval((IntervalExpression) parts[1]));
    }

    private static boolean isNow(Expression e) {
        if (e instanceof Function f) {
            return Set.of("now", "current_timestamp", "localtimestamp").contains(f.getName().toLowerCase(Locale.ROOT))
                    && (f.getParameters() == null || f.getParameters().isEmpty());
        }
        if (e instanceof TimeKeyExpression t) {
            String v = t.getStringValue().toUpperCase(Locale.ROOT).replace("()", "");
            return v.equals("CURRENT_TIMESTAMP") || v.equals("LOCALTIMESTAMP");
        }
        if (e instanceof Column c) { // NOW and LOCALTIMESTAMP without parentheses parse as names
            String v = c.getFullyQualifiedName().toUpperCase(Locale.ROOT);
            return v.equals("NOW") || v.equals("LOCALTIMESTAMP") || v.equals("CURRENT_TIMESTAMP");
        }
        return false;
    }

    /** {@code INTERVAL '15' MINUTE}, {@code INTERVAL 15 MINUTE} or {@code INTERVAL '15 minutes'}. */
    private Duration interval(IntervalExpression i) {
        String hint = " Write INTERVAL '15' MINUTE or INTERVAL '15 minutes'; the units are SECOND, MINUTE, HOUR and DAY";
        if (i.getExpression() != null || i.getParameter() == null) {
            throw new IllegalArgumentException("the interval " + shown(i.toString()) + " isn't supported." + hint);
        }
        String param = i.getParameter().strip();
        if (param.length() >= 2 && param.startsWith("'") && param.endsWith("'")) {
            param = param.substring(1, param.length() - 1);
        }
        long n;
        String unit;
        if (i.getIntervalType() == null) {
            Matcher m = INTERVAL_TEXT.matcher(param);
            if (!m.matches()) {
                throw new IllegalArgumentException("the interval " + shown(i.toString()) + " isn't supported: it "
                        + "needs a whole number and a unit." + hint);
            }
            n = Long.parseLong(m.group(1));
            unit = m.group(2);
        } else {
            Matcher m = DIGITS.matcher(param);
            if (!m.matches()) {
                throw new IllegalArgumentException("the interval " + shown(i.toString()) + " isn't supported: it "
                        + "needs a whole number." + hint);
            }
            n = Long.parseLong(m.group(1));
            unit = i.getIntervalType();
        }
        String u = unit.toLowerCase(Locale.ROOT);
        if (u.endsWith("s")) {
            u = u.substring(0, u.length() - 1);
        }
        Duration d = switch (u) {
            case "second" -> Duration.ofSeconds(n);
            case "minute" -> Duration.ofMinutes(n);
            case "hour" -> Duration.ofHours(n);
            case "day" -> Duration.ofDays(n);
            default -> throw new IllegalArgumentException("the interval unit " + unit + " in " + shown(i.toString())
                    + " isn't supported." + hint + " (for example INTERVAL '7' DAY for a week)");
        };
        if (d.isZero()) {
            throw new IllegalArgumentException("the time window " + shown(i.toString()) + " is empty; use an "
                    + "interval longer than zero");
        }
        return d;
    }

    private static boolean containsNow(net.sf.jsqlparser.expression.BinaryExpression b) {
        Expression l = unwrap(b.getLeftExpression());
        Expression r = unwrap(b.getRightExpression());
        return isNow(l) || isNow(r) || l instanceof IntervalExpression || r instanceof IntervalExpression;
    }

    // ---------------------------------------------------------------- rejections

    private static boolean caseFolded(Expression e) {
        return e instanceof Function f && CASE_FUNCTIONS.contains(f.getName().toLowerCase(Locale.ROOT));
    }

    /**
     * Case-insensitive matching ({@code LOWER(x) = 'y'}, {@code ILIKE}): selectors compare case-sensitively, so
     * the nearest form is IN over the spellings that occur, taken from get_schema's examples when it has some.
     */
    private IllegalArgumentException caseInsensitive(Expression whole, Expression left, Expression right) {
        Expression target = left;
        if (target instanceof Function f && f.getParameters() != null && f.getParameters().size() == 1) {
            target = unwrap((Expression) f.getParameters().get(0));
        }
        String field = target instanceof Column c ? safeName(c) : "field";
        String literal = right instanceof StringValue s ? s.getNotExcapedValue() : null;
        Set<String> spellings = new LinkedHashSet<>();
        try {
            if (target instanceof Column) {
                fields().field(field).ifPresent(f -> f.examples().stream()
                        .filter(x -> literal == null || x.equalsIgnoreCase(literal)
                                || (literal.contains("%") && x.toLowerCase(Locale.ROOT).startsWith(
                                literal.substring(0, literal.indexOf('%')).toLowerCase(Locale.ROOT))))
                        .limit(MAX_EXAMPLE_SPELLINGS).forEach(spellings::add));
            }
        } catch (RuntimeException e) {
            // no examples to offer: suggest the shape alone
        }
        String suggestion;
        if (!spellings.isEmpty()) {
            suggestion = field + " IN (" + spellings.stream().map(s -> "'" + s.replace("'", "''") + "'")
                    .collect(Collectors.joining(", ")) + "), the spellings in get_schema's examples";
        } else {
            String v = literal == null || literal.contains("%") ? "value" : literal;
            suggestion = field + " IN ('" + v.toLowerCase(Locale.ROOT).replace("'", "''") + "', '"
                    + v.toUpperCase(Locale.ROOT).replace("'", "''") + "', …) with the spellings that occur "
                    + "(get_schema lists example values)";
        }
        return new IllegalArgumentException("case-insensitive matching (" + shown(whole.toString()) + ") isn't "
                + "supported: selectors compare strings case-sensitively, and LOWER, UPPER and ILIKE aren't available. "
                + "Use " + suggestion);
    }

    private static String safeName(Column c) {
        try {
            return columnName(c);
        } catch (IllegalArgumentException e) {
            return c.getFullyQualifiedName();
        }
    }

    private IllegalArgumentException regex(Expression e) {
        return new IllegalArgumentException("regular expressions (" + shown(e.toString()) + ": SIMILAR TO, REGEXP, "
                + "RLIKE, ~) aren't supported. Use LIKE with % (any text) and _ (one character), for example "
                + "event_type LIKE '3ds_%', or IN over the exact values");
    }

    private static IllegalArgumentException aggregates() {
        return new IllegalArgumentException("aggregates (COUNT, SUM, GROUP BY, HAVING, window functions) aren't "
                + "supported: the tools return matching events, not summaries. Select the events with "
                + "SELECT * … WHERE …; the result's matched field counts them");
    }

    private static IllegalArgumentException subquery() {
        return new IllegalArgumentException("subqueries (including EXISTS, IN (SELECT …), ANY and FROM (SELECT …)) "
                + "aren't supported: one call reads one topic with literal values. Write the values out, for example "
                + "order_id IN ('a', 'b')");
    }

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("sql holds more than one statement; pass one SELECT or one condition, "
                + "for example " + EXAMPLE);
    }

    private IllegalArgumentException nowOutsideWindow(Expression e) {
        return new IllegalArgumentException("now(), CURRENT_TIMESTAMP and INTERVAL can only appear in a time window "
                + "on the time column, AND-ed at the top level: ts > now() - INTERVAL '15' MINUTE (got "
                + shown(e.toString()) + ")");
    }

    private IllegalArgumentException unsupported(Expression e) {
        if (e instanceof Function f) {
            String name = f.getName().toLowerCase(Locale.ROOT);
            if (AGGREGATES.contains(name)) {
                return aggregates();
            }
            if (CASE_FUNCTIONS.contains(name)) {
                return caseInsensitive(e, e, null);
            }
            if (NOW_FUNCTIONS.contains(name)) {
                return nowOutsideWindow(e);
            }
            String nearest = switch (name) {
                case "coalesce", "ifnull", "nvl", "isnull" -> "test each case, for example "
                        + "(coupon = 'X' OR coupon IS NULL)";
                case "substring", "substr", "left", "starts_with", "startswith" -> "match the start with LIKE, "
                        + "for example order_id LIKE 'ABC%'";
                case "concat" -> "compare with one literal, or LIKE for part of the value";
                case "length", "char_length", "len" -> "LIKE with _ for each character, for example code LIKE '___'";
                case "abs" -> "BETWEEN, for example delta BETWEEN -5 AND 5";
                case "date", "date_trunc", "to_timestamp", "from_unixtime", "extract", "date_sub", "dateadd",
                        "datediff", "timestampdiff" -> "a time window (ts > now() - INTERVAL '15' MINUTE, or "
                        + "lookback), or compare __publish_time__ with epoch milliseconds";
                default -> "compare the field itself with a literal";
            };
            return new IllegalArgumentException("function calls such as " + shown(e.toString()) + " aren't supported "
                    + "in a selector; " + nearest + ". " + SUPPORTED);
        }
        if (e instanceof CastExpression) {
            return new IllegalArgumentException("casts (" + shown(e.toString()) + ": CAST, ::, typed literals such as "
                    + "DATE '…') aren't supported: compare the field directly. Headers are always strings (seq = "
                    + "'5'); JSON numbers, and JSON text that is a number, read as numbers (amount > 5)");
        }
        if (e instanceof CaseExpression) {
            return new IllegalArgumentException("CASE … END isn't supported; write the cases with AND and OR, for "
                    + "example (tier = 'gold' AND amount > 100) OR (tier = 'silver' AND amount > 500)");
        }
        if (e instanceof ExistsExpression || e instanceof Select || e instanceof ParenthesedSelect
                || e instanceof AnyComparisonExpression) {
            return subquery();
        }
        if (e instanceof JdbcParameter || e instanceof JdbcNamedParameter || e instanceof UserVariable
                || e instanceof NumericBind) {
            return new IllegalArgumentException("parameters such as " + shown(e.toString()) + " (?, :name, $1, @var) "
                    + "aren't supported; write the value inline as a literal, for example status = 'failed'");
        }
        if (e instanceof RegExpMatchOperator || e instanceof SimilarToExpression) {
            return regex(e);
        }
        if (e instanceof IntervalExpression || e instanceof TimeKeyExpression) {
            return nowOutsideWindow(e);
        }
        if (e instanceof AnalyticExpression) {
            return aggregates();
        }
        return new IllegalArgumentException("the SQL construct " + shown(e.toString()) + " isn't supported in a "
                + "selector. " + SUPPORTED);
    }

    private static IllegalArgumentException reject(String message) {
        return new IllegalArgumentException(message);
    }

    private IllegalArgumentException unparseable(String text, Throwable e) {
        String detail = String.valueOf(e.getMessage());
        int cut = detail.indexOf("Was expecting");
        if (cut >= 0) {
            detail = detail.substring(0, cut);
        }
        detail = detail.replaceAll("\\s+", " ").strip();
        if (detail.length() > 200) {
            detail = detail.substring(0, 200) + "…";
        }
        String hint = text.contains("://") && !text.contains("\"") ? " Quote a full topic name, FROM "
                + "\"persistent://public/default/orders\", or use its short name (FROM orders)." : "";
        if (e instanceof StackOverflowError) {
            detail = "it is nested too deeply";
        }
        return new IllegalArgumentException("couldn't parse the SQL: " + detail + "." + hint + " Write a bare "
                + "condition (event_type = 'login' AND amount > 100) or a query such as " + EXAMPLE);
    }

    private String shown(String s) {
        String one = restore(s).replaceAll("\\s+", " ").strip();
        return one.length() > MAX_SHOWN ? one.substring(0, MAX_SHOWN) + "…" : one;
    }
}
