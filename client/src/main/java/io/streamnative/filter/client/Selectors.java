package io.streamnative.filter.client;

import java.util.Collection;
import java.util.Iterator;

/**
 * Helpers that compose JMS-selector fragments. Source-topic constraints are added by the Session
 * so the user's selector evaluates only against messages from the destination(s) they asked for.
 */
final class Selectors {

    private Selectors() {}

    /** Escapes a single value for inclusion as a JMS-selector string literal. */
    static String literal(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Selector literal must not be null");
        }
        // JMS selector strings are single-quoted; inner quotes are escaped by doubling.
        return "'" + value.replace("'", "''") + "'";
    }

    /** Builds {@code __topic__ = 'value'}. */
    static String topicEquals(String topic) {
        return MessageBindingNames.TOPIC + " = " + literal(topic);
    }

    /** Builds {@code __topic__ IN ('a','b','c')}. */
    static String topicIn(Collection<String> topics) {
        if (topics == null || topics.isEmpty()) {
            throw new IllegalArgumentException("At least one source topic is required");
        }
        if (topics.size() == 1) {
            return topicEquals(topics.iterator().next());
        }
        StringBuilder sb = new StringBuilder(MessageBindingNames.TOPIC).append(" IN (");
        Iterator<String> it = topics.iterator();
        while (it.hasNext()) {
            sb.append(literal(it.next()));
            if (it.hasNext()) sb.append(',');
        }
        sb.append(')');
        return sb.toString();
    }

    /**
     * Combine a source-topic constraint with an optional user selector. The user's selector is
     * wrapped in parentheses to preserve operator precedence.
     */
    static String combine(String sourceConstraint, String userSelector) {
        if (userSelector == null || userSelector.isBlank()) {
            return sourceConstraint;
        }
        return sourceConstraint + " AND (" + userSelector + ")";
    }

    /** Names that the function exposes to selectors. Kept in sync with the function's MessageBindings. */
    static final class MessageBindingNames {
        static final String KEY   = "__key__";
        static final String TOPIC = "__topic__";

        private MessageBindingNames() {}
    }
}
