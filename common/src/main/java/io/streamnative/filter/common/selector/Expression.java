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
            return Bool.and(Bool.truth(left.evaluate(resolver)), Bool.truth(right.evaluate(resolver)));
        }
    }

    record Or(Expression left, Expression right) implements Expression {
        @Override
        public Object evaluate(Function<String, Object> resolver) {
            return Bool.or(Bool.truth(left.evaluate(resolver)), Bool.truth(right.evaluate(resolver)));
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
