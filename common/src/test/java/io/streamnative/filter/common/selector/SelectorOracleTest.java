package io.streamnative.filter.common.selector;

import io.streamnative.pulsar.jms.selectors.SelectorSupport;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential test: every (selector, bindings) pair must evaluate identically in the homegrown
 * {@link Selector} and the ActiveMQ-based {@link SelectorSupport} it replaces. This is the parity
 * gate — it must stay green before the production call sites switch over (P2). The JMS oracle is a
 * test-scope dependency that goes away in P3.
 */
class SelectorOracleTest {

    private static final List<String> SELECTORS = List.of(
            "region = 'US'",
            "region <> 'US'",
            "price > 100",
            "price >= 100",
            "price < 100",
            "price <= 100",
            "price = 100",
            "price <> 100",
            "region = 'US' AND tier = 'GOLD'",
            "region = 'US' OR tier = 'GOLD'",
            "NOT region = 'US'",
            "NOT (region = 'US' AND tier = 'GOLD')",
            "region IN ('US','UK','DE')",
            "region NOT IN ('US','UK')",
            "price BETWEEN 100 AND 200",
            "price NOT BETWEEN 100 AND 200",
            "region IS NULL",
            "region IS NOT NULL",
            "t LIKE '%trades%'",
            "t LIKE 'persistent://public/trades/%'",
            "code LIKE 'A_C'",
            "v LIKE '50!%' ESCAPE '!'",
            "t NOT LIKE '%trades%'",
            "(region = 'US' OR region = 'UK') AND tier = 'GOLD'",
            "price + 10 > 100",
            "price * 2 >= 200",
            "active = TRUE",
            "active",
            "region = 'US' AND price > 100 AND tier IN ('GOLD','PLATINUM')",
            "name = 'O''Reilly'",
            "price > 100 OR region IS NULL",
            "tier = 'GOLD' AND price BETWEEN 100 AND 200");

    private static List<Map<String, Object>> bindingSets() {
        List<Map<String, Object>> out = new ArrayList<>();
        out.add(new HashMap<>());                                              // all absent
        out.add(map("region", "US"));
        out.add(map("region", "UK", "tier", "GOLD"));
        out.add(map("region", "US", "tier", "GOLD", "price", 150L, "active", true,
                "t", "persistent://public/trades/AAPL", "code", "ABC", "v", "50%", "name", "O'Reilly"));
        out.add(map("region", "JP", "tier", "SILVER", "price", 50L, "active", false,
                "t", "x/quotes/y", "code", "ABBC", "v", "5000"));
        out.add(map("price", "150.5"));                                        // numeric-as-string
        out.add(map("price", 250.0d, "region", "DE"));
        out.add(map("region", null, "price", 100L));                           // explicit null binding
        return out;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void parityWithJmsOracleAcrossCorpus() throws Exception {
        List<String> mismatches = new ArrayList<>();
        for (String selector : SELECTORS) {
            SelectorSupport oracle = SelectorSupport.build(selector, true);
            Selector ours = Selector.compile(selector);
            assertThat(oracle).as("oracle compiled: %s", selector).isNotNull();

            for (Map<String, Object> b : bindingSets()) {
                Function<String, Object> resolver = b::get;
                boolean oracleResult;
                try {
                    oracleResult = oracle.matches(resolver);
                } catch (JMSException | RuntimeException e) {
                    // SelectorEvaluator.matches catches JMSException | RuntimeException → "no match"
                    // (e.g. ActiveMQ throws RuntimeException for arithmetic on a String). Mirror it.
                    oracleResult = false;
                }
                boolean ourResult = ours.matches(resolver);
                if (oracleResult != ourResult) {
                    mismatches.add(String.format("[%s] bindings=%s  oracle=%s ours=%s",
                            selector, b, oracleResult, ourResult));
                }
            }
        }
        assertThat(mismatches).as("differential mismatches vs JMS oracle").isEmpty();
    }
}
