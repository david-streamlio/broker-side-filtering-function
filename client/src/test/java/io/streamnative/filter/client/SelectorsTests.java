package io.streamnative.filter.client;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectorsTests {

    @Test
    void literalQuotesValue() {
        assertThat(Selectors.literal("US")).isEqualTo("'US'");
    }

    @Test
    void literalEscapesEmbeddedSingleQuotes() {
        assertThat(Selectors.literal("O'Reilly")).isEqualTo("'O''Reilly'");
    }

    @Test
    void literalRejectsNull() {
        assertThatThrownBy(() -> Selectors.literal(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topicEqualsRenders() {
        assertThat(Selectors.topicEquals("persistent://t/n/x"))
                .isEqualTo("__topic__ = 'persistent://t/n/x'");
    }

    @Test
    void topicInWithSingleElementCollapsesToEquals() {
        assertThat(Selectors.topicIn(List.of("a"))).isEqualTo("__topic__ = 'a'");
    }

    @Test
    void topicInWithMultipleElementsRendersList() {
        assertThat(Selectors.topicIn(List.of("a", "b", "c")))
                .isEqualTo("__topic__ IN ('a','b','c')");
    }

    @Test
    void topicInRejectsEmpty() {
        assertThatThrownBy(() -> Selectors.topicIn(List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void combineWrapsUserSelectorInParens() {
        String combined = Selectors.combine("__topic__ = 'a'", "region = 'US'");
        assertThat(combined).isEqualTo("__topic__ = 'a' AND (region = 'US')");
    }

    @Test
    void combineSkipsBlankUserSelector() {
        assertThat(Selectors.combine("__topic__ = 'a'", null)).isEqualTo("__topic__ = 'a'");
        assertThat(Selectors.combine("__topic__ = 'a'", "")).isEqualTo("__topic__ = 'a'");
        assertThat(Selectors.combine("__topic__ = 'a'", "   ")).isEqualTo("__topic__ = 'a'");
    }
}
