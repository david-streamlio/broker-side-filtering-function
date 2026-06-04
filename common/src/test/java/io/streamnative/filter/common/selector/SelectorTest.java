package io.streamnative.filter.common.selector;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectorTest {

    private static Function<String, Object> bindings(Map<String, Object> map) {
        return map::get;
    }

    private static boolean match(String selector, Map<String, Object> map) {
        return Selector.compile(selector).matches(bindings(map));
    }

    // ---------------------------------------------------------------- basics

    @Test
    void stringEquality() {
        assertThat(match("region = 'US'", Map.of("region", "US"))).isTrue();
        assertThat(match("region = 'US'", Map.of("region", "UK"))).isFalse();
    }

    @Test
    void missingIdentifierIsNoMatch() {
        assertThat(match("region = 'US'", Map.of())).isFalse();
    }

    @Test
    void numericComparison() {
        assertThat(match("price > 100", Map.of("price", 150L))).isTrue();
        assertThat(match("price > 100", Map.of("price", 50L))).isFalse();
        assertThat(match("price >= 100", Map.of("price", 100L))).isTrue();
        assertThat(match("price <= 99.5", Map.of("price", 99.5d))).isTrue();
        assertThat(match("price <> 100", Map.of("price", 101L))).isTrue();
    }

    @Test
    void numericComparisonAgainstStringIsUnknown() {
        // JMS quirk: numeric op on a String operand → UNKNOWN → no match.
        assertThat(match("price > 100", Map.of("price", "150.5"))).isFalse();
    }

    @Test
    void booleanIdentifierAndLiterals() {
        assertThat(match("active", Map.of("active", true))).isTrue();
        assertThat(match("active", Map.of("active", false))).isFalse();
        assertThat(match("active = TRUE", Map.of("active", true))).isTrue();
        assertThat(match("active = FALSE", Map.of("active", true))).isFalse();
    }

    // ---------------------------------------------------------------- logical / 3VL

    @Test
    void andOrNot() {
        Map<String, Object> m = Map.of("region", "US", "tier", "GOLD");
        assertThat(match("region = 'US' AND tier = 'GOLD'", m)).isTrue();
        assertThat(match("region = 'US' AND tier = 'SILVER'", m)).isFalse();
        assertThat(match("region = 'EU' OR tier = 'GOLD'", m)).isTrue();
        assertThat(match("NOT (region = 'EU')", m)).isTrue();
    }

    @Test
    void threeValuedLogic() {
        Map<String, Object> m = Map.of("region", "US"); // 'tier' absent
        // UNKNOWN AND TRUE → UNKNOWN → no match
        assertThat(match("tier = 'GOLD' AND region = 'US'", m)).isFalse();
        // UNKNOWN OR TRUE → TRUE → match
        assertThat(match("tier = 'GOLD' OR region = 'US'", m)).isTrue();
        // NOT UNKNOWN → UNKNOWN → no match (parens needed — see notBindsTighterThanComparison)
        assertThat(match("NOT (tier = 'GOLD')", m)).isFalse();
    }

    @Test
    void notBindsTighterThanComparison() {
        // ActiveMQ quirk we replicate for parity: NOT is unary, so `NOT a = b` is `(NOT a) = b`.
        // NOT of a (non-boolean) string is UNKNOWN, and UNKNOWN = 'US' is UNKNOWN → no match, even
        // when region differs from 'US'. Use NOT (a = b) for real negation.
        assertThat(match("NOT region = 'US'", Map.of("region", "UK"))).isFalse();
        assertThat(match("NOT (region = 'US')", Map.of("region", "UK"))).isTrue();
    }

    // ---------------------------------------------------------------- LIKE

    @Test
    void likePercentAndUnderscore() {
        assertThat(match("t LIKE 'persistent://public/trades/%'",
                Map.of("t", "persistent://public/trades/AAPL"))).isTrue();
        assertThat(match("t LIKE '%trades%'", Map.of("t", "x/quotes/y"))).isFalse();
        assertThat(match("code LIKE 'A_C'", Map.of("code", "ABC"))).isTrue();
        assertThat(match("code LIKE 'A_C'", Map.of("code", "ABBC"))).isFalse();
    }

    @Test
    void likeEscape() {
        assertThat(match("v LIKE '50!%' ESCAPE '!'", Map.of("v", "50%"))).isTrue();
        assertThat(match("v LIKE '50!%' ESCAPE '!'", Map.of("v", "5000"))).isFalse();
    }

    @Test
    void notLike() {
        assertThat(match("t NOT LIKE '%trades%'", Map.of("t", "x/quotes/y"))).isTrue();
    }

    // ---------------------------------------------------------------- IN / BETWEEN / IS NULL

    @Test
    void inList() {
        assertThat(match("region IN ('US','UK','DE')", Map.of("region", "DE"))).isTrue();
        assertThat(match("region IN ('US','UK','DE')", Map.of("region", "JP"))).isFalse();
        assertThat(match("region NOT IN ('US','UK')", Map.of("region", "JP"))).isTrue();
    }

    @Test
    void between() {
        assertThat(match("price BETWEEN 100 AND 200", Map.of("price", 150L))).isTrue();
        assertThat(match("price BETWEEN 100 AND 200", Map.of("price", 250L))).isFalse();
        assertThat(match("price NOT BETWEEN 100 AND 200", Map.of("price", 250L))).isTrue();
    }

    @Test
    void isNull() {
        assertThat(match("__key__ IS NULL", Map.of())).isTrue();
        assertThat(match("__key__ IS NULL", Map.of("__key__", "AAPL"))).isFalse();
        assertThat(match("__key__ IS NOT NULL", Map.of("__key__", "AAPL"))).isTrue();
    }

    // ---------------------------------------------------------------- arithmetic / precedence / parens

    @Test
    void arithmeticAndPrecedence() {
        assertThat(match("a + b * 2 = 11", Map.of("a", 5L, "b", 3L))).isTrue();   // 5 + 6
        assertThat(match("(a + b) * 2 = 16", Map.of("a", 5L, "b", 3L))).isTrue(); // 8 * 2
        assertThat(match("-a = 0 - 5", Map.of("a", 5L))).isTrue();
    }

    @Test
    void parenthesizedBooleanGroup() {
        Map<String, Object> m = Map.of("a", "1", "b", "2", "c", "3");
        assertThat(match("(a = '1' OR b = 'x') AND c = '3'", m)).isTrue();
        assertThat(match("a = '1' OR (b = 'x' AND c = 'x')", m)).isTrue();
    }

    // ---------------------------------------------------------------- literals / referenced names

    @Test
    void escapedQuoteInStringLiteral() {
        assertThat(match("name = 'O''Reilly'", Map.of("name", "O'Reilly"))).isTrue();
    }

    @Test
    void referencedNamesFromAst() {
        Selector s = Selector.compile(
                "region = 'foo' AND status IN ('ACTIVE','PENDING') OR __key__ IS NOT NULL");
        assertThat(s.referencedNames()).containsExactlyInAnyOrder("region", "status", "__key__");
    }

    @Test
    void referencedNamesIgnoreQuotedLiterals() {
        assertThat(Selector.compile("name = 'O''Reilly'").referencedNames()).containsExactly("name");
    }

    // ---------------------------------------------------------------- parse errors

    @Test
    void emptyOrBlankThrows() {
        assertThatThrownBy(() -> Selector.compile("")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("   ")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile(null)).isInstanceOf(SelectorParseException.class);
    }

    @Test
    void malformedThrows() {
        assertThatThrownBy(() -> Selector.compile("region =")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("region = 'US' AND")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("region 'US'")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("(a = 1")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("name = 'unterminated")).isInstanceOf(SelectorParseException.class);
        assertThatThrownBy(() -> Selector.compile("@@bad@@")).isInstanceOf(SelectorParseException.class);
    }
}
