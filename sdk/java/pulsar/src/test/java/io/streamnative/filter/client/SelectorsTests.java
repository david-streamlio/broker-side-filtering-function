package io.streamnative.filter.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Selectors only retains client-side syntax validation. Composition helpers (topicEquals,
 * topicIn, combine, literal, MessageBindingNames) were removed in the orchestrator migration —
 * the orchestrator routes by input topic so the SDK publishes the user's selector verbatim.
 */
class SelectorsTests {

    @Test
    void validateAcceptsNullAndBlank() {
        // null and blank are valid — they mean "no user-side filter; the orchestrator will
        // translate to a JMS-selector identity downstream".
        assertThatCode(() -> Selectors.validate(null)).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("   ")).doesNotThrowAnyException();
    }

    @Test
    void validateAcceptsValidJmsSelectors() {
        assertThatCode(() -> Selectors.validate("region = 'US'")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("__key__ IN ('AAPL','MSFT')")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("price > 100 AND region = 'US'")).doesNotThrowAnyException();
    }

    @Test
    void validateRejectsMalformedSelector() {
        assertThatThrownBy(() -> Selectors.validate("THIS IS NOT @@## VALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid selector syntax");
    }

    @Test
    void validateRejectsUnbalancedQuotes() {
        assertThatThrownBy(() -> Selectors.validate("region = 'US"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
