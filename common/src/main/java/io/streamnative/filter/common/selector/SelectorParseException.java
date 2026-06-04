package io.streamnative.filter.common.selector;

/**
 * Thrown when a selector expression is syntactically invalid. Replaces the {@code javax.jms.JMSException}
 * previously surfaced by the ActiveMQ-based parser, so callers no longer depend on the JMS API.
 */
public final class SelectorParseException extends RuntimeException {

    /** Character offset into the expression where parsing failed, or {@code -1} if not available. */
    private final int position;

    public SelectorParseException(String message, int position) {
        super(position >= 0 ? message + " (at position " + position + ")" : message);
        this.position = position;
    }

    public SelectorParseException(String message) {
        this(message, -1);
    }

    /** Character offset of the error, or {@code -1} if unknown. */
    public int position() {
        return position;
    }
}
