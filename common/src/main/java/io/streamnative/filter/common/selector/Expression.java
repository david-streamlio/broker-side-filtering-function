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

import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A compiled selector AST node. {@link #evaluate} returns a value ({@link String} / {@link Number} /
 * {@link Boolean} / {@code null}) for value nodes, and a truth value ({@link Boolean} or {@code null}
 * = UNKNOWN) for predicate nodes. Boolean operators coerce their operands via {@link Bool#truth},
 * so a parenthesized group can hold either a value or a predicate uniformly.
 */
sealed interface Expression {

    Object evaluate(Function<String, Object> resolver);

    // ---------------------------------------------------------------- value nodes

    record Literal(Object value) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return value;
        }
    }

    record Identifier(String name) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return resolver.apply(name);
        }
    }

    record Arithmetic(Expression left, char op, Expression right) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return Values.arithmetic(left.evaluate(resolver), right.evaluate(resolver), op);
        }
    }

    record Negate(Expression operand) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return Values.negate(operand.evaluate(resolver));
        }
    }

    // ---------------------------------------------------------------- predicate nodes

    record And(Expression left, Expression right) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            // Short-circuit: FALSE AND x = FALSE for every x (incl. UNKNOWN), so the right side --
            // and any name resolution it would trigger, e.g. a message-value decode -- is skipped.
            Boolean l = Bool.truth(left.evaluate(resolver));
            if (Boolean.FALSE.equals(l)) {
                return Boolean.FALSE;
            }
            return Bool.and(l, Bool.truth(right.evaluate(resolver)));
        }
    }

    record Or(Expression left, Expression right) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            // Short-circuit: TRUE OR x = TRUE for every x (incl. UNKNOWN). UNKNOWN still needs the
            // right side (UNKNOWN OR TRUE = TRUE), as does FALSE.
            Boolean l = Bool.truth(left.evaluate(resolver));
            if (Boolean.TRUE.equals(l)) {
                return Boolean.TRUE;
            }
            return Bool.or(l, Bool.truth(right.evaluate(resolver)));
        }
    }

    record Not(Expression operand) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return Bool.not(Bool.truth(operand.evaluate(resolver)));
        }
    }

    record Comparison(Expression left, Token.Type op, Expression right) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return Values.compare(left.evaluate(resolver), right.evaluate(resolver), op);
        }
    }

    record Between(Expression value, Expression low, Expression high, boolean negated) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            Object v = value.evaluate(resolver);
            Boolean in = Bool.and(
                    Values.compare(v, low.evaluate(resolver), Token.Type.GTE),
                    Values.compare(v, high.evaluate(resolver), Token.Type.LTE));
            return negated ? Bool.not(in) : in;
        }
    }

    record Like(Expression value, Pattern pattern, boolean negated) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            Object v = value.evaluate(resolver);
            if (!(v instanceof String s)) {
                return null; // non-string or null → UNKNOWN
            }
            Boolean matched = pattern.matcher(s).matches();
            return negated ? Bool.not(matched) : matched;
        }
    }

    record In(Expression value, Set<String> values, boolean negated) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            Object v = value.evaluate(resolver);
            if (!(v instanceof String s)) {
                return null; // null or non-string → UNKNOWN
            }
            Boolean present = values.contains(s);
            return negated ? Bool.not(present) : present;
        }
    }

    record IsNull(Expression value, boolean negated) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            boolean isNull = value.evaluate(resolver) == null;
            return negated != isNull; // IS NULL: TRUE when null; IS NOT NULL: TRUE when present
        }
    }
}
