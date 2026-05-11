package io.streamnative.filter;

import javax.jms.JMSException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectorEvaluatorTests {

    private static MessageBindings bindings(String key, String topic, Map<String, String> props) {
        return new MessageBindings(key, topic, props);
    }

    @Test
    void compilesValidExpression() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("region = 'US'");
        assertThat(e.getExpression()).isEqualTo("region = 'US'");
    }

    @Test
    void malformedExpressionThrows() {
        assertThatThrownBy(() -> SelectorEvaluator.compile("region = "))
                .isInstanceOf(JMSException.class);
    }

    @Test
    void emptyExpressionThrows() {
        assertThatThrownBy(() -> SelectorEvaluator.compile(""))
                .isInstanceOf(JMSException.class);
        assertThatThrownBy(() -> SelectorEvaluator.compile(null))
                .isInstanceOf(JMSException.class);
        assertThatThrownBy(() -> SelectorEvaluator.compile("   "))
                .isInstanceOf(JMSException.class);
    }

    @Test
    void matchesByKey() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ = 'AAPL'");
        assertThat(e.matches(bindings("AAPL", null, Map.of()))).isTrue();
        assertThat(e.matches(bindings("MSFT", null, Map.of()))).isFalse();
    }

    @Test
    void matchesByTopicLike() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__topic__ LIKE 'persistent://public/trades/%'");
        assertThat(e.matches(bindings(null, "persistent://public/trades/AAPL", Map.of()))).isTrue();
        assertThat(e.matches(bindings(null, "persistent://public/quotes/AAPL", Map.of()))).isFalse();
    }

    @Test
    void matchesByUserProperty() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("region = 'US'");
        assertThat(e.matches(bindings(null, null, Map.of("region", "US")))).isTrue();
        assertThat(e.matches(bindings(null, null, Map.of("region", "UK")))).isFalse();
    }

    @Test
    void stringEqualityOnStringProperty() throws Exception {
        // Pulsar user properties are always String. Numeric comparisons against string-typed
        // properties evaluate to UNKNOWN per JMS selector semantics. Authors should rely on
        // string equality / LIKE / IN for property filtering.
        SelectorEvaluator e = SelectorEvaluator.compile("price = '150.5'");
        assertThat(e.matches(bindings(null, null, Map.of("price", "150.5")))).isTrue();
        assertThat(e.matches(bindings(null, null, Map.of("price", "50.0")))).isFalse();
    }

    @Test
    void numericComparisonAgainstStringPropertyIsUnknown() throws Exception {
        // Documents the JMS-selector quirk: String > Number => UNKNOWN => no match.
        SelectorEvaluator e = SelectorEvaluator.compile("price > 100");
        assertThat(e.matches(bindings(null, null, Map.of("price", "150.5")))).isFalse();
    }

    @Test
    void matchesAndCombination() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile(
                "__topic__ LIKE '%trades%' AND region = 'US' AND marketcap_bucket = 'LARGE'");
        Map<String, String> props = new HashMap<>();
        props.put("region", "US");
        props.put("marketcap_bucket", "LARGE");
        assertThat(e.matches(bindings(null, "persistent://t/n/trades", props))).isTrue();
        props.put("marketcap_bucket", "SMALL");
        assertThat(e.matches(bindings(null, "persistent://t/n/trades", props))).isFalse();
    }

    @Test
    void matchesInList() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("region IN ('US','UK','DE')");
        assertThat(e.matches(bindings(null, null, Map.of("region", "DE")))).isTrue();
        assertThat(e.matches(bindings(null, null, Map.of("region", "JP")))).isFalse();
    }

    @Test
    void missingPropertyEvaluatesFalse() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("region = 'US'");
        assertThat(e.matches(bindings(null, null, Map.of()))).isFalse();
    }

    @Test
    void isNullSelector() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ IS NULL");
        assertThat(e.matches(bindings(null, "any", Map.of()))).isTrue();
        assertThat(e.matches(bindings("present", "any", Map.of()))).isFalse();
    }

    // ===== referencedNames / referencesOnlyKey optimization detection =====

    @Test
    void referencesOnlyKeyForKeyOnlySelector() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ = 'AAPL'");
        assertThat(e.referencesOnlyKey()).isTrue();
        assertThat(e.referencesTopic()).isFalse();
        assertThat(e.referencesUserProperties()).isFalse();
        assertThat(e.getReferencedNames()).containsExactly("__key__");
    }

    @Test
    void referencesOnlyKeyForKeyInList() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ IN ('AAPL','MSFT','GOOGL')");
        assertThat(e.referencesOnlyKey()).isTrue();
    }

    @Test
    void referencesOnlyKeyIsFalseWhenTopicReferenced() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ = 'AAPL' AND __topic__ LIKE '%trades%'");
        assertThat(e.referencesOnlyKey()).isFalse();
        assertThat(e.referencesTopic()).isTrue();
        assertThat(e.referencesUserProperties()).isFalse();
    }

    @Test
    void referencesOnlyKeyIsFalseWhenUserPropertyReferenced() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ = 'AAPL' AND region = 'US'");
        assertThat(e.referencesOnlyKey()).isFalse();
        assertThat(e.referencesTopic()).isFalse();
        assertThat(e.referencesUserProperties()).isTrue();
        assertThat(e.getReferencedNames()).containsExactlyInAnyOrder("__key__", "region");
    }

    @Test
    void identifierExtractorIgnoresKeywordsAndQuotedLiterals() {
        // The literal 'foo' must not be picked up as a binding even though it would otherwise
        // match the identifier pattern.
        assertThat(SelectorEvaluator.extractReferencedNames(
                "region = 'foo' AND status IN ('ACTIVE','PENDING') OR __key__ IS NOT NULL"))
                .containsExactlyInAnyOrder("region", "status", "__key__");
    }

    @Test
    void identifierExtractorHandlesEscapedQuotesInLiterals() {
        // Within 'O''Reilly', the inner '' is the escaped single quote — Reilly is NOT a binding.
        assertThat(SelectorEvaluator.extractReferencedNames("name = 'O''Reilly'"))
                .containsExactly("name");
    }

    @Test
    void keyOnlyMatchDoesNotResolveTopicOrProperties() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__key__ = 'AAPL'");
        MessageBindings b = new MessageBindings("AAPL", "any-topic", Map.of("ignored", "x"));
        assertThat(e.matches(b)).isTrue();
        assertThat(b.keyResolved()).isTrue();
        assertThat(b.topicResolved()).isFalse();
        assertThat(b.propertiesResolved()).isFalse();
    }

    @Test
    void topicSelectorResolvesTopicButNotProperties() throws Exception {
        SelectorEvaluator e = SelectorEvaluator.compile("__topic__ LIKE '%trades%'");
        MessageBindings b = new MessageBindings("AAPL", "persistent://t/n/trades", Map.of("region", "US"));
        assertThat(e.matches(b)).isTrue();
        assertThat(b.topicResolved()).isTrue();
        assertThat(b.propertiesResolved()).isFalse();
    }

    @Test
    void selectorEngineFailureDuringMatchReturnsFalse() throws Exception {
        // Safety net: if the underlying selector engine ever throws during evaluation (e.g.
        // because a selector references a name whose typed accessor inside the engine cannot
        // tolerate the binding we return), our wrapper catches the exception and the query
        // simply does not match. One bad query must not poison the fan-out.
        SelectorEvaluator e = SelectorEvaluator.compile("JMSPriority = 5");
        assertThat(e.matches(bindings("k", "t", Map.of()))).isFalse();
    }
}
