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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AND / OR short-circuit left to right without changing three-valued results: the right operand is
 * skipped only when the left alone decides the outcome (FALSE for AND, TRUE for OR).
 */
class SelectorShortCircuitTest {

    private static final Boolean T = Boolean.TRUE;
    private static final Boolean F = Boolean.FALSE;
    private static final Boolean U = null; // UNKNOWN

    /** Explicit JMS / SQL three-valued truth table: {left, right, AND, OR}. */
    static Stream<Arguments> truthTable() {
        return Stream.of(
                Arguments.of(T, T, T, T),
                Arguments.of(T, F, F, T),
                Arguments.of(T, U, U, T),
                Arguments.of(F, T, F, T),
                Arguments.of(F, F, F, F),
                Arguments.of(F, U, F, U),
                Arguments.of(U, T, U, T),
                Arguments.of(U, F, F, U),
                Arguments.of(U, U, U, U));
    }

    /** Bind {@code name} to the operand; UNKNOWN is modelled as an absent name (resolves to null). */
    private static void bind(Map<String, Object> m, String name, Boolean v) {
        if (v != null) {
            m.put(name, v);
        }
    }

    private static Object eval(String expr, Boolean l, Boolean r) {
        Map<String, Object> m = new HashMap<>();
        bind(m, "l", l);
        bind(m, "r", r);
        return new Parser(new Lexer(expr).tokenize()).parse().evaluate(m::get);
    }

    @ParameterizedTest(name = "{0} AND {1} = {2}; {0} OR {1} = {3}")
    @MethodSource("truthTable")
    void andOrTruthTable(Boolean l, Boolean r, Boolean and, Boolean or) {
        // AST-level: the raw three-valued result (distinguishes FALSE from UNKNOWN).
        assertThat(eval("l AND r", l, r)).isEqualTo(and);
        assertThat(eval("l OR r", l, r)).isEqualTo(or);
        // The non-short-circuit combinators agree with the table (the pre-change behaviour).
        assertThat(Bool.and(l, r)).isEqualTo(and);
        assertThat(Bool.or(l, r)).isEqualTo(or);
        // NOT over the result: NOT UNKNOWN stays UNKNOWN, so FALSE vs UNKNOWN is observable via matches().
        Map<String, Object> m = new HashMap<>();
        bind(m, "l", l);
        bind(m, "r", r);
        assertThat(Selector.compile("NOT (l AND r)").matches(m::get)).isEqualTo(F.equals(and));
        assertThat(Selector.compile("NOT (l OR r)").matches(m::get)).isEqualTo(F.equals(or));
    }

    // ---------------------------------------------------------------- laziness

    /** Resolver that records every name it is asked for. */
    private static final class CountingResolver implements Function<String, Object> {
        private final Map<String, Object> values;
        private final List<String> resolved = new ArrayList<>();

        CountingResolver(Map<String, Object> values) {
            this.values = values;
        }

        @Override
        public Object apply(String name) {
            resolved.add(name);
            return values.get(name);
        }
    }

    @Test
    void andSkipsRightWhenLeftIsFalse() {
        CountingResolver r = new CountingResolver(Map.of("__key__", "b", "body.x", 1L));
        assertThat(Selector.compile("__key__ = 'a' AND body.x = 1").matches(r)).isFalse();
        assertThat(r.resolved).containsExactly("__key__");
    }

    @Test
    void andEvaluatesRightWhenLeftIsTrue() {
        CountingResolver r = new CountingResolver(Map.of("__key__", "a", "body.x", 1L));
        assertThat(Selector.compile("__key__ = 'a' AND body.x = 1").matches(r)).isTrue();
        assertThat(r.resolved).containsExactly("__key__", "body.x");
    }

    @Test
    void andEvaluatesRightWhenLeftIsUnknown() {
        // Missing __key__ → UNKNOWN; UNKNOWN AND FALSE = FALSE, so the right must be consulted.
        CountingResolver r = new CountingResolver(Map.of("body.x", 2L));
        assertThat(Selector.compile("__key__ = 'a' AND body.x = 1").matches(r)).isFalse();
        assertThat(r.resolved).containsExactly("__key__", "body.x");
    }

    @Test
    void orSkipsRightWhenLeftIsTrue() {
        CountingResolver r = new CountingResolver(Map.of("__key__", "a", "body.x", 2L));
        assertThat(Selector.compile("__key__ = 'a' OR body.x = 1").matches(r)).isTrue();
        assertThat(r.resolved).containsExactly("__key__");
    }

    @Test
    void orEvaluatesRightWhenLeftIsFalse() {
        CountingResolver r = new CountingResolver(Map.of("__key__", "b", "body.x", 1L));
        assertThat(Selector.compile("__key__ = 'a' OR body.x = 1").matches(r)).isTrue();
        assertThat(r.resolved).containsExactly("__key__", "body.x");
    }

    @Test
    void orEvaluatesRightWhenLeftIsUnknown() {
        // Missing __key__ → UNKNOWN; UNKNOWN OR TRUE = TRUE, so the right must be consulted.
        CountingResolver r = new CountingResolver(Map.of("body.x", 1L));
        assertThat(Selector.compile("__key__ = 'a' OR body.x = 1").matches(r)).isTrue();
        assertThat(r.resolved).containsExactly("__key__", "body.x");
    }

    @Test
    void shortCircuitNestsLeftToRight() {
        // (FALSE AND …) OR (TRUE OR …): only the deciding terms are resolved.
        CountingResolver r = new CountingResolver(Map.of("__key__", "b", "region", "US"));
        assertThat(Selector.compile(
                "(__key__ = 'a' AND body.x = 1) OR (region = 'US' OR body.y = 2)").matches(r)).isTrue();
        assertThat(r.resolved).containsExactly("__key__", "region");
    }
}
