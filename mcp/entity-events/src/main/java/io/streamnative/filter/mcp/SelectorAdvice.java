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
import io.streamnative.filter.common.selector.SelectorParseException;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * What the server tells an agent about a selector beyond "valid" or "invalid": parse errors with where they
 * happened and how to write the syntax, "did you mean" for unknown names, comparisons that can't match as
 * written, and a resolver that evaluates a selector against a sampled message the way the filter would.
 *
 * <p>The goal is that an agent fixes a selector in one retry. Each message names the problem, the place, and
 * a corrected form where one is knowable, in plain sentences.
 *
 * <p>The comparison hints read the selector's tokens rather than the parser's tree: the tree is internal to
 * the selector package. The selector has already been parsed by then, so this scan only needs to spot
 * {@code field op literal} shapes, not judge syntax.
 */
final class SelectorAdvice {

    static final String SYNTAX_REMINDER = "Selector syntax is SQL-92 WHERE: strings go in single quotes ('login'), "
            + "with two single quotes for a quote inside one ('it''s'); the operators are = <> < > <= >= LIKE IN "
            + "BETWEEN IS NULL AND OR NOT; for example event_type LIKE '3ds_%'";

    private static final ObjectMapper JSON = new ObjectMapper();
    /** MessageBindings hides this KoP property and uses it to base64-decode the properties it names. */
    private static final String KOP_HEADER_FORMAT = "__ksn_internal_header_format";

    private SelectorAdvice() {
    }

    // ---------------------------------------------------------------- parse errors

    /**
     * A parse error the agent can act on: the parser's own detail, where in the selector it happened, the
     * common mistake behind it when one is recognisable, and a short syntax reminder.
     */
    static String parseError(String selector, SelectorParseException e) {
        String detail = String.valueOf(e.getMessage());
        int pos = e.position();
        // The exception appends its position to the message; it's said here in words instead.
        String suffix = " (at position " + pos + ")";
        if (pos >= 0 && detail.endsWith(suffix)) {
            detail = detail.substring(0, detail.length() - suffix.length());
        }
        // The end of input is an empty token, which the parser reports as found ''.
        detail = detail.replace("but found ''", "but the selector ends there")
                .replace("Unexpected token ''", "The selector ends too early");
        StringBuilder sb = new StringBuilder("selector isn't valid JMS selector syntax: ").append(detail);
        if (pos >= 0) {
            sb.append(where(selector, pos));
        }
        sb.append(". ");
        for (String hint : commonMistakes(selector)) {
            sb.append(hint).append(' ');
        }
        return sb.append(SYNTAX_REMINDER).toString();
    }

    private static String where(String selector, int pos) {
        if (pos == 0) {
            return ", at the start of the selector";
        }
        if (pos >= selector.length()) {
            return ", at the end of the selector";
        }
        String before = selector.substring(0, pos).stripTrailing();
        if (before.length() > 30) {
            before = "..." + before.substring(before.length() - 30);
        }
        return ", at character " + (pos + 1) + " (right after \"" + before + "\")";
    }

    /** Habits from other languages that the lexer rejects with a bare "unexpected character". */
    private static List<String> commonMistakes(String selector) {
        List<String> hints = new ArrayList<>();
        if (selector.indexOf('"') >= 0) {
            hints.add("Strings take single quotes, not double quotes.");
        }
        if (selector.contains("==")) {
            hints.add("Compare with =, not ==.");
        }
        if (selector.contains("!=")) {
            hints.add("Write not-equal as <>, not !=.");
        }
        if (selector.contains("&&") || selector.contains("||")) {
            hints.add("Join terms with AND and OR, not && and ||.");
        }
        return hints;
    }

    // ---------------------------------------------------------------- unknown fields

    /** The unknown-field error, with a "did you mean" for each name that resembles one the topic has. */
    static String unknownFields(String topic, List<String> unknown, Collection<String> known) {
        String shortName = Topics.shortName(topic);
        StringBuilder sb = new StringBuilder("selector names ").append(unknown.size() == 1 ? "a field" : "fields")
                .append(" that ").append(shortName).append(" doesn't have: ").append(String.join(", ", unknown))
                .append(". ");
        for (String name : unknown) {
            suggest(name, known).ifPresent(s -> sb.append("For ").append(name).append(", did you mean ")
                    .append(s).append("? "));
        }
        return sb.append("Call get_schema with topic \"").append(shortName).append("\" for the fields you can use")
                .toString();
    }

    /**
     * The known name {@code name} most likely meant: the same name in another case; the same name without
     * underscores ({@code key} for {@code __key__}, {@code eventtype} for {@code event_type}); the same last
     * segment of a dotted path ({@code tier} for {@code customer.tier}); else the closest by edit distance,
     * if it's close (1 edit for short names, 2 for longer).
     */
    static Optional<String> suggest(String name, Collection<String> known) {
        String lower = name.toLowerCase(Locale.ROOT);
        for (String k : known) {
            if (k.equalsIgnoreCase(name)) {
                return Optional.of(k);
            }
        }
        String bare = lower.replace("_", "");
        for (String k : known) {
            if (!bare.isEmpty() && k.toLowerCase(Locale.ROOT).replace("_", "").equals(bare)) {
                return Optional.of(k);
            }
        }
        String last = lastSegment(lower);
        for (String k : known) {
            if (lastSegment(k.toLowerCase(Locale.ROOT)).equals(last)) {
                return Optional.of(k);
            }
        }
        int limit = name.length() <= 4 ? 1 : 2;
        String best = null;
        int bestDistance = Integer.MAX_VALUE;
        for (String k : known) {
            int d = distance(lower, k.toLowerCase(Locale.ROOT));
            if (d <= limit && d < bestDistance) {
                best = k;
                bestDistance = d;
            }
        }
        return Optional.ofNullable(best);
    }

    private static String lastSegment(String path) {
        int dot = path.lastIndexOf('.');
        return dot < 0 ? path : path.substring(dot + 1);
    }

    /** Levenshtein distance. */
    static int distance(String a, String b) {
        int[] prev = new int[b.length() + 1];
        int[] cur = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            cur[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int sub = prev[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1);
                cur[j] = Math.min(sub, Math.min(prev[j] + 1, cur[j - 1] + 1));
            }
            int[] t = prev;
            prev = cur;
            cur = t;
        }
        return prev[b.length()];
    }

    // ---------------------------------------------------------------- comparisons

    enum Kind { IDENT, STRING, NUMBER, BOOLEAN, NULL, OP, KEYWORD, PUNCT }

    record Tok(Kind kind, String text, int start, int end) {
        boolean is(String s) {
            return text.equalsIgnoreCase(s) && (kind == Kind.OP || kind == Kind.KEYWORD || kind == Kind.PUNCT);
        }

        boolean literal() {
            return kind == Kind.STRING || kind == Kind.NUMBER || kind == Kind.BOOLEAN;
        }

        boolean arithmetic() {
            return kind == Kind.OP && "+-*/".contains(text);
        }
    }

    private static final Set<String> KEYWORDS = Set.of("AND", "OR", "NOT", "BETWEEN", "LIKE", "IN", "IS", "ESCAPE");
    private static final Set<String> COMPARE = Set.of("=", "<>", "<", ">", "<=", ">=");

    /**
     * A {@code field op literal} term. {@code op} is a comparison operator, {@code BETWEEN}, {@code LIKE} or
     * {@code IN}; {@code literal} is the (first) literal's kind and value ({@code null} for {@code IN}); and
     * {@code text} is the term as the agent wrote it.
     */
    record Term(String field, String op, boolean negated, Kind literal, String value, String text) {
    }

    /** The selector's tokens, scanned the way the selector lexer does (strings, numbers, dotted names). */
    static List<Tok> tokens(String s) {
        List<Tok> out = new ArrayList<>();
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            int start = i;
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '\'') {
                StringBuilder sb = new StringBuilder();
                i++;
                while (i < s.length()) {
                    if (s.charAt(i) == '\'') {
                        if (i + 1 < s.length() && s.charAt(i + 1) == '\'') {
                            sb.append('\'');
                            i += 2;
                            continue;
                        }
                        i++;
                        break;
                    }
                    sb.append(s.charAt(i++));
                }
                out.add(new Tok(Kind.STRING, sb.toString(), start, i));
            } else if (Character.isDigit(c) || (c == '.' && i + 1 < s.length() && Character.isDigit(s.charAt(i + 1)))) {
                while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '.'
                        || ((s.charAt(i) == '+' || s.charAt(i) == '-') && (s.charAt(i - 1) == 'e' || s.charAt(i - 1) == 'E')))) {
                    i++;
                }
                out.add(new Tok(Kind.NUMBER, s.substring(start, i), start, i));
            } else if (Character.isLetter(c) || c == '_' || c == '$') {
                while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_' || s.charAt(i) == '$'
                        || (s.charAt(i) == '.' && i + 1 < s.length()
                            && (Character.isLetter(s.charAt(i + 1)) || s.charAt(i + 1) == '_' || s.charAt(i + 1) == '$')))) {
                    i++;
                }
                String text = s.substring(start, i);
                String upper = text.toUpperCase(Locale.ROOT);
                Kind kind = text.indexOf('.') >= 0 ? Kind.IDENT
                        : upper.equals("TRUE") || upper.equals("FALSE") ? Kind.BOOLEAN
                        : upper.equals("NULL") ? Kind.NULL
                        : KEYWORDS.contains(upper) ? Kind.KEYWORD : Kind.IDENT;
                out.add(new Tok(kind, text, start, i));
            } else if ((c == '<' || c == '>') && i + 1 < s.length() && (s.charAt(i + 1) == '=' || (c == '<' && s.charAt(i + 1) == '>'))) {
                i += 2;
                out.add(new Tok(Kind.OP, s.substring(start, i), start, i));
            } else if ("=<>+-*/".indexOf(c) >= 0) {
                i++;
                out.add(new Tok(Kind.OP, String.valueOf(c), start, i));
            } else {
                i++;
                out.add(new Tok(Kind.PUNCT, String.valueOf(c), start, i));
            }
        }
        return out;
    }

    /** The selector's {@code field op literal} terms; terms inside arithmetic are left out. */
    static List<Term> terms(String selector) {
        List<Tok> t = tokens(selector);
        List<Term> out = new ArrayList<>();
        for (int i = 0; i < t.size(); i++) {
            Tok tok = t.get(i);
            boolean afterArithmetic = i > 0 && t.get(i - 1).arithmetic();
            if (tok.kind() == Kind.IDENT && !afterArithmetic && i + 1 < t.size()) {
                Tok next = t.get(i + 1);
                if (next.kind() == Kind.OP && COMPARE.contains(next.text())) {
                    int lit = literalAt(t, i + 2);
                    if (lit >= 0 && !arithmeticAt(t, lit + 1)) {
                        out.add(term(selector, tok.text(), next.text(), false, t, lit, tok.start(), t.get(lit).end()));
                    }
                    continue;
                }
                boolean negated = next.is("NOT");
                int j = negated ? i + 2 : i + 1;
                if (j >= t.size()) {
                    continue;
                }
                Tok op = t.get(j);
                if (op.is("BETWEEN")) {
                    int low = literalAt(t, j + 1);
                    if (low >= 0) {
                        int end = Math.min(t.size() - 1, low + 2 + (t.size() > low + 2 && t.get(low + 2).is("-") ? 1 : 0));
                        out.add(term(selector, tok.text(), "BETWEEN", negated, t, low, tok.start(), t.get(end).end()));
                    }
                } else if (op.is("LIKE") && j + 1 < t.size() && t.get(j + 1).kind() == Kind.STRING) {
                    out.add(term(selector, tok.text(), "LIKE", negated, t, j + 1, tok.start(), t.get(j + 1).end()));
                } else if (op.is("IN")) {
                    int end = j;
                    while (end + 1 < t.size() && !t.get(end).is(")")) {
                        end++;
                    }
                    out.add(new Term(tok.text(), "IN", negated, null, null,
                            selector.substring(tok.start(), t.get(end).end())));
                }
            } else if (tok.literal() && !afterArithmetic && i + 2 < t.size()
                    && t.get(i + 1).kind() == Kind.OP && COMPARE.contains(t.get(i + 1).text())
                    && t.get(i + 2).kind() == Kind.IDENT && !arithmeticAt(t, i + 3)) {
                // 5 < seq reads as seq > 5.
                out.add(term(selector, t.get(i + 2).text(), flip(t.get(i + 1).text()), false, t, i, tok.start(),
                        t.get(i + 2).end()));
            }
        }
        return out;
    }

    /** The index of a literal at {@code i} (a minus sign before a number is part of it), or -1. */
    private static int literalAt(List<Tok> t, int i) {
        if (i < t.size() && t.get(i).literal()) {
            return i;
        }
        if (i + 1 < t.size() && t.get(i).is("-") && t.get(i + 1).kind() == Kind.NUMBER) {
            return i + 1;
        }
        return -1;
    }

    private static boolean arithmeticAt(List<Tok> t, int i) {
        return i < t.size() && t.get(i).arithmetic();
    }

    private static Term term(String selector, String field, String op, boolean negated, List<Tok> t, int lit,
                             int from, int to) {
        Tok l = t.get(lit);
        String value = lit > 0 && t.get(lit - 1).is("-") && l.kind() == Kind.NUMBER ? "-" + l.text() : l.text();
        return new Term(field, op, negated, l.kind(), value, selector.substring(from, to));
    }

    private static String flip(String op) {
        return switch (op) {
            case "<" -> ">";
            case ">" -> "<";
            case "<=" -> ">=";
            case ">=" -> "<=";
            default -> op;
        };
    }

    /**
     * Whether the filter picks exact values: a key, or an {@code =} or (un-negated) {@code IN} on any field.
     * Without one, every matching event in the window is read and returned.
     */
    static boolean picksExactValues(String selector, String key) {
        if (key != null) {
            return true;
        }
        return terms(selector).stream().anyMatch(t -> t.op().equals("=") || (t.op().equals("IN") && !t.negated()));
    }

    /** {@code LIKE} terms whose pattern starts with {@code %}. */
    static List<Term> leadingWildcards(String selector) {
        return terms(selector).stream()
                .filter(t -> t.op().equals("LIKE") && t.value() != null && t.value().startsWith("%"))
                .toList();
    }

    // ---------------------------------------------------------------- type hints

    /** What a field holds when the filter evaluates it, and why, in words. */
    private record Holds(Kind kind, String why) {
    }

    /**
     * Comparisons that can't match as written because the field and the literal have different types. Under
     * the selector's value rules ({@code Values.compare}): a string never equals a number, so {@code =} is
     * false and {@code <>} true for every event; ordering ({@code < > <= >=}, {@code BETWEEN}) a string against
     * a number is unknown, which never matches; and {@code LIKE} and {@code IN} only match strings.
     */
    static List<String> typeHints(String selector, FieldCatalog.Fields fields) {
        Set<String> hints = new LinkedHashSet<>();
        for (Term t : terms(selector)) {
            Optional<FieldCatalog.Field> f = fields.field(t.field());
            if (f.isEmpty()) {
                continue;
            }
            Holds holds = holds(f.get(), fields.bodyFieldsFrom());
            if (holds == null) {
                continue;
            }
            String hint = hint(t, holds);
            if (hint != null) {
                hints.add(hint);
            }
        }
        return List.copyOf(hints);
    }

    private static Holds holds(FieldCatalog.Field f, String bodyFieldsFrom) {
        String name = f.name();
        switch (f.source()) {
            case "key":
                return new Holds(Kind.STRING, name + " is always a string");
            case "header":
                return new Holds(Kind.STRING, name + " is a header, and header values are always strings");
            case "metadata":
                return new Holds(Kind.NUMBER, name + " is a number (epoch ms)");
            case "body":
                if (f.type().startsWith("number")) {
                    return new Holds(Kind.NUMBER, name + " is a number in the message value");
                }
                if (f.type().equals("boolean")) {
                    return new Holds(Kind.BOOLEAN, name + " is a boolean in the message value");
                }
                // The filter reads a JSON string that parses as a number as that number (MessageBindings), so a
                // field of numeric text compares as a number. A structured (Avro) decode doesn't do this.
                if (!bodyFieldsFrom.contains("AVRO") && !f.examples().isEmpty()
                        && f.examples().stream().allMatch(SelectorAdvice::numeric)) {
                    return new Holds(Kind.NUMBER, name + " holds numbers written as text (for example \""
                            + f.examples().get(0) + "\"), and the filter reads such text in a JSON value as a number");
                }
                return new Holds(Kind.STRING, name + " is a string in the message value");
            default:
                return null;
        }
    }

    private static String hint(Term t, Holds holds) {
        boolean ordering = t.op().equals("BETWEEN") || (COMPARE.contains(t.op()) && !t.op().equals("=") && !t.op().equals("<>"));
        if (holds.kind() == Kind.STRING && t.literal() == Kind.NUMBER) {
            String quoted = t.field() + " " + t.op() + " '" + t.value() + "'";
            if (t.op().equals("=")) {
                return holds.why() + ", so " + t.text() + " is false for every event: a string never equals a number. "
                        + "Quote the value: " + quoted + ".";
            }
            if (t.op().equals("<>")) {
                return holds.why() + ", so " + t.text() + " is true for every event: a string never equals a number. "
                        + "Quote the value: " + quoted + ".";
            }
            if (ordering) {
                return holds.why() + ", so " + t.text() + " matches no event: comparing a string with a number by "
                        + "size is unknown, which never matches, and strings can only be compared with = <> LIKE and "
                        + "IN. Compare exact values instead, for example " + t.field() + " IN ('" + t.value() + "').";
            }
        }
        if (holds.kind() == Kind.NUMBER) {
            if (t.literal() == Kind.STRING) {
                String bare = t.field() + " " + t.op() + " " + (numeric(t.value()) ? t.value() : "<number>");
                if (t.op().equals("=")) {
                    return holds.why() + ", so " + t.text() + " is false for every event: a number never equals a "
                            + "string. Drop the quotes: " + bare + ".";
                }
                if (t.op().equals("<>")) {
                    return holds.why() + ", so " + t.text() + " is true for every event: a number never equals a "
                            + "string. Drop the quotes: " + bare + ".";
                }
                if (ordering) {
                    return holds.why() + ", so " + t.text() + " matches no event: comparing a number with a string "
                            + "by size is unknown, which never matches. Drop the quotes"
                            + (t.op().equals("BETWEEN") ? " around both bounds." : ": " + bare + ".");
                }
            }
            if (t.op().equals("LIKE") || t.op().equals("IN")) {
                return holds.why() + ", so " + t.text() + " matches no event: " + t.op() + " only matches strings. "
                        + "Use = or BETWEEN with unquoted numbers instead.";
            }
        }
        return null;
    }

    private static boolean numeric(String s) {
        try {
            Double.parseDouble(s);
            return !s.isBlank();
        } catch (NumberFormatException e) {
            return false;
        }
    }

    // ---------------------------------------------------------------- sample evaluation

    /**
     * A selector resolver over one sampled message, mirroring the filter's {@code MessageBindings.lookup}:
     * {@code __key__}; {@code __publish_time__} when the sample carries it (else {@code null}, unknown); then
     * properties, as strings (KoP's base64-flagged ones decoded, its format property hidden); then fields of
     * the value parsed as JSON by dotted path, where numbers stay numbers, booleans stay booleans and text
     * that parses as a number becomes that number.
     *
     * <p>The wire-format prefix is stripped before parsing, as {@link FieldCatalog#unframed} does. The filter
     * itself doesn't strip it, but the catalog offers no body fields for such topics, so a valid selector never
     * reaches a body field there.
     */
    static Function<String, Object> resolver(FieldCatalog.Sample sample) {
        JsonNode[] value = new JsonNode[1];
        boolean[] parsed = new boolean[1];
        return name -> {
            if ("__key__".equals(name)) {
                return sample.key();
            }
            if ("__publish_time__".equals(name)) {
                return sample.publishTime();
            }
            if ("__topic__".equals(name) || "__event_time__".equals(name) || KOP_HEADER_FORMAT.equals(name)) {
                return null;
            }
            Map<String, String> props = sample.properties() == null ? Map.of() : sample.properties();
            String raw = props.get(name);
            if (raw != null) {
                return base64Flagged(props).contains(name) ? decodedOrRaw(raw) : raw;
            }
            if (!parsed[0]) {
                parsed[0] = true;
                value[0] = json(sample.value());
            }
            return jsonField(value[0], name);
        };
    }

    private static JsonNode json(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            int from = FieldCatalog.unframed(bytes);
            JsonNode root = JSON.readTree(new String(bytes, from, bytes.length - from, StandardCharsets.UTF_8));
            return root != null && root.isObject() ? root : null;
        } catch (Exception e) {
            return null;
        }
    }

    private static Object jsonField(JsonNode root, String name) {
        JsonNode node = root;
        for (String segment : name.split("\\.")) {
            if (node == null || !node.isObject()) {
                return null;
            }
            node = node.get(segment);
        }
        if (node == null || node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        String text = node.asText();
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            return text;
        }
    }

    private static Set<String> base64Flagged(Map<String, String> props) {
        String spec = props.get(KOP_HEADER_FORMAT);
        if (spec == null) {
            return Set.of();
        }
        Set<String> flagged = new HashSet<>();
        try {
            JsonNode root = JSON.readTree(spec);
            if (root != null && root.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = root.fields();
                while (it.hasNext()) {
                    Map.Entry<String, JsonNode> e = it.next();
                    if (e.getValue().isTextual() && "base64".equalsIgnoreCase(e.getValue().asText())) {
                        flagged.add(e.getKey());
                    }
                }
            }
        } catch (Exception e) {
            return Set.of();
        }
        return flagged;
    }

    private static String decodedOrRaw(String raw) {
        try {
            byte[] bytes = Base64.getDecoder().decode(raw);
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (IllegalArgumentException | CharacterCodingException e) {
            return raw;
        }
    }

    /** A value as a selector would write it: strings quoted, numbers and booleans bare. */
    static String literal(Object value) {
        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof Double d && d == Math.rint(d) && !d.isInfinite() && Math.abs(d) < 1e15) {
            return String.valueOf(d.longValue());
        }
        return String.valueOf(value);
    }
}
