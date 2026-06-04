package io.streamnative.filter.client;

import io.streamnative.filter.common.selector.Selector;
import io.streamnative.filter.common.selector.SelectorParseException;

/**
 * JMS-selector helpers used by the client SDK. Since the orchestrator routes by input topic
 * (filter functions are spawned per-topic), the SDK no longer composes {@code __topic__ = '...'}
 * fragments into the user's selector — it publishes the user selector verbatim. The only
 * remaining helper is client-side syntax validation against the same selector grammar the filter
 * function uses, so malformed selectors fail at the SDK boundary instead of being silently dropped
 * as bad queries server-side.
 */
final class Selectors {

    private Selectors() {}

    /**
     * Validate a user-supplied selector against the same selector grammar the function uses.
     * {@code null} or blank is allowed (means "no user-side filter"). Throws
     * {@link IllegalArgumentException} otherwise; the thrown exception's {@code getCause()} is the
     * underlying {@link SelectorParseException}.
     */
    static void validate(String userSelector) {
        if (userSelector == null || userSelector.isBlank()) {
            return;
        }
        try {
            Selector.compile(userSelector);
        } catch (SelectorParseException e) {
            throw new IllegalArgumentException(
                    "Invalid selector syntax: " + e.getMessage() + " (expression: " + userSelector + ")", e);
        }
    }
}
