package io.streamnative.filter.common.selector;

import io.streamnative.pulsar.jms.selectors.SelectorSupport;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Differential test: every (selector, bindings) pair must evaluate identically in the homegrown
 * {@link Selector} and the ActiveMQ-based {@link SelectorSupport} it replaces. This is the parity
 * gate that must stay green before the production call sites switch over (P2). The JMS oracle is a
 * test-scope dependency that goes away in P3.
 *
 * <p>Two layers: a curated corpus of edge cases ({@link #parityAcrossCuratedCorpus()}), and a
 * seeded property-based sweep that generates thousands of compound selectors and randomized bindings
 * ({@link #parityAcrossRandomizedSweep()}).
 */
class SelectorOracleTest {

    // ---------------------------------------------------------------- curated corpus

    private static final List<String> SELECTORS = List.of(
            // equality / inequality across types
            "region = 'US'", "region <> 'US'",
            "price = 100", "price <> 100", "price = 100.0", "price <> 0",
            "active = TRUE", "active = FALSE", "active <> TRUE",
            // ordering (numeric only)
            "price > 100", "price >= 100", "price < 100", "price <= 100",
            "price > 99.5", "price >= -5", "price < 1000000",
            // numeric-vs-string: equality is definite (so NOT flips), matching ActiveMQ. Ordering
            // under NOT on a String is an intentional documented divergence (see design doc).
            "price > 100 OR price <> 100", "NOT (price = 100)",
            // logical + precedence
            "region = 'US' AND tier = 'GOLD'",
            "region = 'US' OR tier = 'GOLD'",
            "region = 'US' OR tier = 'GOLD' AND price > 100",
            "(region = 'US' OR region = 'UK') AND tier = 'GOLD'",
            "NOT (region = 'US')", "NOT (region = 'US' AND tier = 'GOLD')",
            "NOT region = 'US'",                         // ActiveMQ NOT-precedence quirk
            "NOT region = 'US' OR tier = 'GOLD'",
            // IN
            "region IN ('US','UK','DE')", "region NOT IN ('US','UK')", "tier IN ('GOLD')",
            // BETWEEN
            "price BETWEEN 100 AND 200", "price NOT BETWEEN 100 AND 200",
            "price BETWEEN 200 AND 100", "price BETWEEN 100 AND 100",
            // IS NULL
            "region IS NULL", "region IS NOT NULL", "price IS NULL OR price > 50",
            // LIKE (incl. _, %, escape, regex-special chars)
            "t LIKE '%trades%'", "t LIKE 'persistent://public/trades/%'",
            "code LIKE 'A_C'", "code LIKE '_B_'", "t NOT LIKE '%quotes%'",
            "v LIKE '50!%' ESCAPE '!'", "path LIKE 'a.b%'", "path LIKE 'a%b%c'",
            // arithmetic
            "price + 10 > 100", "price * 2 >= 200", "price - 50 < 0", "(price + price) = 300",
            // literals / escapes / keyword case-insensitivity
            "name = 'O''Reilly'", "region = 'US' and tier = 'GOLD'", "region in ('US','UK')",
            "price between 100 and 200", "region is not null",
            // combined
            "region = 'US' AND price > 100 AND tier IN ('GOLD','PLATINUM')",
            "tier = 'GOLD' AND price BETWEEN 100 AND 200 OR region IS NULL");

    private static List<Map<String, Object>> bindingSets() {
        List<Map<String, Object>> out = new ArrayList<>();
        out.add(new HashMap<>());                                              // all absent
        out.add(map("region", "US"));
        out.add(map("region", "UK", "tier", "GOLD"));
        out.add(map("region", "US", "tier", "GOLD", "price", 150L, "active", true,
                "t", "persistent://public/trades/AAPL", "code", "ABC", "v", "50%",
                "name", "O'Reilly", "path", "a.b.c"));
        out.add(map("region", "JP", "tier", "SILVER", "price", 50L, "active", false,
                "t", "x/quotes/y", "code", "ABBC", "v", "5000", "path", "axbyc"));
        out.add(map("price", "150.5"));                                        // numeric-as-string
        out.add(map("price", 250.0d, "region", "DE"));
        out.add(map("price", 100));                                            // Integer, not Long
        out.add(map("price", -5L, "region", "US"));
        out.add(map("region", null, "price", 100L));                           // explicit null binding
        return out;
    }

    @Test
    void parityAcrossCuratedCorpus() throws Exception {
        List<String> mismatches = new ArrayList<>();
        for (String selector : SELECTORS) {
            SelectorSupport oracle = SelectorSupport.build(selector, true);
            Selector ours = Selector.compile(selector);
            assertThat(oracle).as("oracle compiled: %s", selector).isNotNull();
            for (Map<String, Object> b : bindingSets()) {
                compareInto(mismatches, selector, oracle, ours, b);
            }
        }
        assertThat(mismatches).as("curated differential mismatches").isEmpty();
    }

    // ---------------------------------------------------------------- randomized sweep

    @Test
    void parityAcrossRandomizedSweep() {
        Random rnd = new Random(20260604L); // fixed seed → reproducible
        List<String> mismatches = new ArrayList<>();
        int compiledPairs = 0;

        for (int i = 0; i < 3000 && mismatches.size() < 25; i++) {
            String selector = genExpr(rnd, 0);

            SelectorSupport oracle = null;
            boolean oracleOk = true;
            try {
                oracle = SelectorSupport.build(selector, true);
            } catch (Throwable e) {
                oracleOk = false;
            }
            Selector ours = null;
            boolean oursOk = true;
            try {
                ours = Selector.compile(selector);
            } catch (Throwable e) {
                oursOk = false;
            }

            if (oracleOk != oursOk) {
                mismatches.add(String.format("COMPILE diverge: [%s] oracle=%s ours=%s",
                        selector, oracleOk, oursOk));
                continue;
            }
            if (!oracleOk) {
                continue; // both rejected — fine
            }
            compiledPairs++;
            for (int j = 0; j < 6; j++) {
                compareInto(mismatches, selector, oracle, ours, genBindings(rnd));
            }
        }

        assertThat(mismatches).as("randomized differential mismatches").isEmpty();
        assertThat(compiledPairs).as("sweep should exercise many compiled selectors").isGreaterThan(500);
    }

    // ---------------------------------------------------------------- shared comparison

    private static void compareInto(List<String> mismatches, String selector,
                                    SelectorSupport oracle, Selector ours, Map<String, Object> b) {
        Function<String, Object> resolver = b::get;
        boolean oracleResult;
        try {
            oracleResult = oracle.matches(resolver);
        } catch (JMSException | RuntimeException e) {
            // SelectorEvaluator.matches catches JMSException | RuntimeException → "no match".
            oracleResult = false;
        }
        boolean ourResult = ours.matches(resolver);
        if (oracleResult != ourResult) {
            mismatches.add(String.format("[%s] bindings=%s  oracle=%s ours=%s",
                    selector, b, oracleResult, ourResult));
        }
    }

    // ---------------------------------------------------------------- generators

    private static final String[] REGIONS = {"US", "UK", "DE", "JP"};
    private static final String[] TIERS = {"GOLD", "SILVER", "PLATINUM"};

    /** A compound boolean expression: atoms combined with AND/OR, grouped, and negated. */
    private static String genExpr(Random rnd, int depth) {
        if (depth >= 3 || rnd.nextInt(100) < 45) {
            return genAtom(rnd);
        }
        int choice = rnd.nextInt(4);
        return switch (choice) {
            case 0 -> "(" + genExpr(rnd, depth + 1) + " AND " + genExpr(rnd, depth + 1) + ")";
            case 1 -> "(" + genExpr(rnd, depth + 1) + " OR " + genExpr(rnd, depth + 1) + ")";
            case 2 -> "NOT (" + genExpr(rnd, depth + 1) + ")";
            default -> "(" + genExpr(rnd, depth + 1) + ")";
        };
    }

    /** A single valid predicate; literals are drawn from the same pools the bindings use. */
    private static String genAtom(Random rnd) {
        return switch (rnd.nextInt(12)) {
            case 0 -> "region = '" + pick(rnd, REGIONS) + "'";
            case 1 -> "region <> '" + pick(rnd, REGIONS) + "'";
            case 2 -> "region IN ('" + pick(rnd, REGIONS) + "','" + pick(rnd, REGIONS) + "')";
            case 3 -> "region IS " + (rnd.nextBoolean() ? "NULL" : "NOT NULL");
            case 4 -> "price " + pickOp(rnd) + " " + rnd.nextInt(300);
            case 5 -> "price " + pickOp(rnd) + " " + (rnd.nextInt(30000) / 100.0);
            case 6 -> "price BETWEEN " + rnd.nextInt(150) + " AND " + (150 + rnd.nextInt(150));
            case 7 -> "tier IN ('" + pick(rnd, TIERS) + "','" + pick(rnd, TIERS) + "')";
            case 8 -> "active = " + (rnd.nextBoolean() ? "TRUE" : "FALSE");
            case 9 -> "t LIKE '%" + pick(rnd, new String[]{"trades", "quotes", "public"}) + "%'";
            case 10 -> "code LIKE '" + pick(rnd, new String[]{"A_C", "_B_", "AB%"}) + "'";
            default -> "price + " + rnd.nextInt(50) + " " + pickOp(rnd) + " " + rnd.nextInt(300);
        };
    }

    private static Map<String, Object> genBindings(Random rnd) {
        Map<String, Object> b = new HashMap<>();
        if (rnd.nextInt(100) < 80) {
            b.put("region", pick(rnd, REGIONS));
        }
        if (rnd.nextInt(100) < 70) {
            b.put("tier", pick(rnd, TIERS));
        }
        if (rnd.nextInt(100) < 80) {
            // Numeric only: arithmetic on a String binding is a pathological case where ActiveMQ's
            // ad-hoc string->number coercion (and exceptions) is intentionally not replicated. The
            // String-vs-numeric *comparison* edge is covered separately in the curated corpus.
            b.put("price", switch (rnd.nextInt(3)) {
                case 0 -> (long) rnd.nextInt(400) - 50;
                case 1 -> (rnd.nextInt(40000) - 5000) / 100.0;
                default -> rnd.nextInt(400);                 // Integer
            });
        }
        if (rnd.nextInt(100) < 60) {
            b.put("active", rnd.nextBoolean());
        }
        if (rnd.nextInt(100) < 60) {
            b.put("t", pick(rnd, new String[]{
                    "persistent://public/trades/AAPL", "x/quotes/y", "public/data"}));
        }
        if (rnd.nextInt(100) < 50) {
            b.put("code", pick(rnd, new String[]{"ABC", "ABBC", "ABX", "XBY"}));
        }
        return b;
    }

    private static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    private static String pick(Random rnd, String[] pool) {
        return pool[rnd.nextInt(pool.length)];
    }

    private static String pickOp(Random rnd) {
        return new String[]{"=", "<>", ">", ">=", "<", "<="}[rnd.nextInt(6)];
    }
}
