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

package io.streamnative.filter.common.selector;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Recursive-descent parser for the JMS message-selector grammar. Produces an {@link Expression} tree
 * and the set of identifier names it references; syntax errors raise {@link SelectorParseException}.
 *
 * <p>Precedence (low→high): {@code OR} · {@code AND} · {@code NOT} · comparison/{@code BETWEEN}/
 * {@code LIKE}/{@code IN}/{@code IS NULL} · additive · multiplicative · unary · primary.
 */
final class Parser {

    private final List<Token> tokens;
    private final Set<String> referencedNames = new LinkedHashSet<>();
    private int pos;

    Parser(List<Token> tokens) {
        this.tokens = tokens;
    }

    Set<String> referencedNames() {
        return referencedNames;
    }

    Expression parse() {
        Expression expr = parseOr();
        if (!check(Token.Type.EOF)) {
            throw error("Unexpected trailing input '" + peek().text() + "'", peek().position());
        }
        return expr;
    }

    // ---------------------------------------------------------------- grammar

    private Expression parseOr() {
        Expression left = parseAnd();
        while (match(Token.Type.OR)) {
            left = new Expression.Or(left, parseAnd());
        }
        return left;
    }

    private Expression parseAnd() {
        Expression left = parseNot();
        while (match(Token.Type.AND)) {
            left = new Expression.And(left, parseNot());
        }
        return left;
    }

    // NOT binds looser than comparison (standard SQL/JMS precedence): `NOT a = b` is `NOT (a = b)`.
    // This is the P4 deliberate fix of ActiveMQ's non-standard quirk (it placed NOT at the unary
    // level, making `NOT a = b` mean `(NOT a) = b`). It is a documented behavior change; see
    // docs/selector-parser-design.md.
    private Expression parseNot() {
        if (match(Token.Type.NOT)) {
            return new Expression.Not(parseNot());
        }
        return parsePredicate();
    }

    private Expression parsePredicate() {
        Expression left = parseAdditive();

        Token.Type op = peek().type();
        if (op == Token.Type.EQ || op == Token.Type.NEQ || op == Token.Type.LT
                || op == Token.Type.LTE || op == Token.Type.GT || op == Token.Type.GTE) {
            advance();
            return new Expression.Comparison(left, op, parseAdditive());
        }

        boolean negated = false;
        if (check(Token.Type.NOT)
                && (checkAhead(1, Token.Type.BETWEEN) || checkAhead(1, Token.Type.LIKE)
                    || checkAhead(1, Token.Type.IN))) {
            advance(); // NOT
            negated = true;
        }

        if (match(Token.Type.BETWEEN)) {
            Expression low = parseAdditive();
            expect(Token.Type.AND, "BETWEEN requires 'AND'");
            Expression high = parseAdditive();
            return new Expression.Between(left, low, high, negated);
        }
        if (match(Token.Type.LIKE)) {
            return finishLike(left, negated);
        }
        if (match(Token.Type.IN)) {
            return finishIn(left, negated);
        }
        if (match(Token.Type.IS)) {
            boolean isNot = match(Token.Type.NOT);
            expect(Token.Type.NULL, "IS requires NULL");
            return new Expression.IsNull(left, isNot);
        }
        return left; // bare value used as a predicate (coerced to truth by AND/OR/NOT)
    }

    private Expression parseAdditive() {
        Expression left = parseMultiplicative();
        while (check(Token.Type.PLUS) || check(Token.Type.MINUS)) {
            char op = advance().type() == Token.Type.PLUS ? '+' : '-';
            left = new Expression.Arithmetic(left, op, parseMultiplicative());
        }
        return left;
    }

    private Expression parseMultiplicative() {
        Expression left = parseUnary();
        while (check(Token.Type.STAR) || check(Token.Type.SLASH)) {
            char op = advance().type() == Token.Type.STAR ? '*' : '/';
            left = new Expression.Arithmetic(left, op, parseUnary());
        }
        return left;
    }

    private Expression parseUnary() {
        if (match(Token.Type.MINUS)) {
            return new Expression.Negate(parseUnary());
        }
        if (match(Token.Type.PLUS)) {
            return parseUnary();
        }
        return parsePrimary();
    }

    private Expression parsePrimary() {
        Token t = peek();
        switch (t.type()) {
            case LPAREN: {
                advance();
                Expression inner = parseOr();
                expect(Token.Type.RPAREN, "Expected ')'");
                return inner;
            }
            case STRING:
            case NUMBER:
            case BOOLEAN:
                advance();
                return new Expression.Literal(t.literal());
            case IDENTIFIER:
                advance();
                referencedNames.add(t.text());
                return new Expression.Identifier(t.text());
            default:
                throw error("Unexpected token '" + t.text() + "'", t.position());
        }
    }

    // ---------------------------------------------------------------- predicate helpers

    private Expression finishLike(Expression left, boolean negated) {
        Token pattern = expect(Token.Type.STRING, "LIKE requires a string pattern");
        Character escape = null;
        if (match(Token.Type.ESCAPE)) {
            Token esc = expect(Token.Type.STRING, "ESCAPE requires a string");
            String e = (String) esc.literal();
            if (e.length() != 1) {
                throw error("ESCAPE must be a single character", esc.position());
            }
            escape = e.charAt(0);
        }
        return new Expression.Like(left, likeToRegex((String) pattern.literal(), escape, pattern.position()), negated);
    }

    private Expression finishIn(Expression left, boolean negated) {
        expect(Token.Type.LPAREN, "IN requires '('");
        Set<String> values = new HashSet<>();
        do {
            Token s = expect(Token.Type.STRING, "IN list requires string literals");
            values.add((String) s.literal());
        } while (match(Token.Type.COMMA));
        expect(Token.Type.RPAREN, "IN list must end with ')'");
        return new Expression.In(left, values, negated);
    }

    /** Translate a SQL {@code LIKE} pattern to a regex: {@code %}→any run, {@code _}→any char. */
    private Pattern likeToRegex(String like, Character escape, int position) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < like.length(); i++) {
            char c = like.charAt(i);
            if (escape != null && c == escape) {
                i++;
                if (i >= like.length()) {
                    throw error("Dangling escape in LIKE pattern", position);
                }
                sb.append(Pattern.quote(String.valueOf(like.charAt(i))));
            } else if (c == '%') {
                sb.append(".*");
            } else if (c == '_') {
                sb.append('.');
            } else {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        return Pattern.compile(sb.toString(), Pattern.DOTALL);
    }

    // ---------------------------------------------------------------- token cursor

    private Token peek() {
        return tokens.get(pos);
    }

    private boolean check(Token.Type type) {
        return peek().type() == type;
    }

    private boolean checkAhead(int offset, Token.Type type) {
        int i = Math.min(pos + offset, tokens.size() - 1);
        return tokens.get(i).type() == type;
    }

    private Token advance() {
        Token t = tokens.get(pos);
        if (t.type() != Token.Type.EOF) {
            pos++;
        }
        return t;
    }

    private boolean match(Token.Type type) {
        if (check(type)) {
            advance();
            return true;
        }
        return false;
    }

    private Token expect(Token.Type type, String message) {
        if (check(type)) {
            return advance();
        }
        throw error(message + " but found '" + peek().text() + "'", peek().position());
    }

    private SelectorParseException error(String message, int position) {
        return new SelectorParseException(message, position);
    }
}
