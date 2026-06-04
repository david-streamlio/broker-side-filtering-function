package io.streamnative.filter.common.selector;

/**
 * Comparison and arithmetic helpers implementing JMS selector value semantics. Operands are the
 * {@link String} / {@link Number} / {@link Boolean} / {@code null} values produced by the resolver
 * and by literals. Comparisons return {@link Boolean#TRUE}/{@link Boolean#FALSE} or {@code null}
 * (UNKNOWN).
 */
final class Values {

    private Values() {
    }

    /**
     * Compare two operands under JMS rules. Numeric↔numeric compares by magnitude (with widening);
     * String↔String and Boolean↔Boolean support only {@code =}/{@code <>}; any {@code null} operand,
     * cross-type pair, or ordering comparison on non-numerics yields {@code null} (UNKNOWN).
     */
    static Boolean compare(Object left, Object right, Token.Type op) {
        if (left == null || right == null) {
            return null; // UNKNOWN when either operand is absent
        }
        if (left instanceof Number ln && right instanceof Number rn) {
            // Numeric widening: compare by magnitude (100L == 100.0d).
            return matches(Double.compare(ln.doubleValue(), rn.doubleValue()), op);
        }
        // At least one non-numeric, both non-null. Equality is definite (matching ActiveMQ): a
        // cross-type `=` is FALSE and `<>` is TRUE — not UNKNOWN. Ordering on non-numerics is not
        // defined (ActiveMQ raises, which the caller treats as no-match), so it is UNKNOWN here.
        return switch (op) {
            case EQ -> left.equals(right);
            case NEQ -> !left.equals(right);
            default -> null;
        };
    }

    private static Boolean matches(int cmp, Token.Type op) {
        return switch (op) {
            case EQ -> cmp == 0;
            case NEQ -> cmp != 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            default -> null;
        };
    }

    /**
     * Evaluate {@code left op right} arithmetically. Non-numeric or {@code null} operands yield
     * {@code null} (making an enclosing comparison UNKNOWN). Exact operands stay {@link Long}; any
     * approximate operand promotes to {@link Double}. Division by zero yields {@code null}.
     */
    static Object arithmetic(Object left, Object right, char op) {
        if (!(left instanceof Number ln) || !(right instanceof Number rn)) {
            return null;
        }
        if (isExact(left) && isExact(right)) {
            long a = ln.longValue();
            long b = rn.longValue();
            return switch (op) {
                case '+' -> a + b;
                case '-' -> a - b;
                case '*' -> a * b;
                case '/' -> b == 0 ? null : a / b;
                default -> null;
            };
        }
        double a = ln.doubleValue();
        double b = rn.doubleValue();
        return switch (op) {
            case '+' -> a + b;
            case '-' -> a - b;
            case '*' -> a * b;
            case '/' -> b == 0.0 ? null : a / b;
            default -> null;
        };
    }

    static Object negate(Object value) {
        if (value instanceof Long l) {
            return -l;
        }
        if (value instanceof Number n) {
            return -n.doubleValue();
        }
        return null;
    }

    private static boolean isExact(Object o) {
        return o instanceof Long || o instanceof Integer || o instanceof Short || o instanceof Byte;
    }
}
