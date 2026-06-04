package io.streamnative.filter.common.selector;

/**
 * Three-valued logic over {@link Boolean} with {@code null} as UNKNOWN, matching the JMS
 * message-selector truth tables. A selector matches a record only when the root expression evaluates
 * to {@link Boolean#TRUE}; {@code FALSE} and {@code null} both mean "no match".
 */
final class Bool {

    private Bool() {
    }

    /** Coerce an evaluated value to a truth value: a {@link Boolean} stays, anything else is UNKNOWN. */
    static Boolean truth(Object value) {
        return value instanceof Boolean b ? b : null;
    }

    static Boolean and(Boolean a, Boolean b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
            return Boolean.FALSE;
        }
        if (a == null || b == null) {
            return null;
        }
        return Boolean.TRUE;
    }

    static Boolean or(Boolean a, Boolean b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
            return Boolean.TRUE;
        }
        if (a == null || b == null) {
            return null;
        }
        return Boolean.FALSE;
    }

    static Boolean not(Boolean a) {
        return a == null ? null : !a;
    }
}
