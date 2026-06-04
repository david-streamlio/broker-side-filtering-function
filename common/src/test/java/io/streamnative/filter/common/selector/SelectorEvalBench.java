package io.streamnative.filter.common.selector;

import io.streamnative.pulsar.jms.selectors.SelectorSupport;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Bench A (handoff §6) — selector evaluation throughput: homegrown {@link Selector} vs the
 * {@code pulsar-jms} {@link SelectorSupport} it replaces, compile-once / eval-many over a corpus of
 * realistic selectors. Confirms the homegrown engine is no slower on the hot path (it has no JMS
 * {@code Message} abstraction and evaluates straight against a {@code name -> Object} resolver — the
 * same resolver shape used by the differential oracle in {@link SelectorOracleTest}).
 *
 * <p>Not a unit test: the {@code *Bench} name matches neither surefire's ({@code *Test}/{@code *Tests})
 * nor failsafe's ({@code *IT}) default include patterns, so it never runs in a normal build and does
 * not count toward the module's test total. Run it explicitly:
 * <pre>{@code
 *   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
 *   mvn -pl common -am test -Dtest=SelectorEvalBench -DfailIfNoTests=false
 *   # tune: -Dbench.iters=1000000 -Dbench.warmup=100000
 * }</pre>
 */
class SelectorEvalBench {

    private static final long WARMUP = Long.getLong("bench.warmup", 50_000L);
    private static final long ITERS = Long.getLong("bench.iters", 500_000L);

    /** Realistic selectors — the patterns the demo and tests actually register. */
    private static final String[] SELECTORS = {
            "region = 'US'",
            "region = 'US' AND tier = 'GOLD'",
            "price > 100 AND price < 500",
            "region IN ('US','UK','DE')",
            "t LIKE 'persistent://public/trades/%'",
            "price BETWEEN 100 AND 200 OR region IS NULL",
            "region = 'US' AND price > 100 AND tier IN ('GOLD','PLATINUM')",
    };

    /** Properly-typed bindings: a String region/tier/topic and a numeric price (Number, not String). */
    private static Map<String, Object> bindings() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("region", "US");
        m.put("tier", "GOLD");
        m.put("price", 150L);
        m.put("t", "persistent://public/trades/AAPL");
        return m;
    }

    // Accumulator read at the end so the JIT cannot eliminate the matches() calls as dead code.
    private long sink;

    @Test
    void benchmark() throws Exception {
        Map<String, Object> b = bindings();
        Function<String, Object> resolver = b::get;

        System.out.printf("%n=== Bench A: selector eval (ns/op) — warmup=%d iters=%d, JDK %s ===%n",
                WARMUP, ITERS, System.getProperty("java.version"));
        System.out.printf("%-58s %12s %12s %9s%n", "selector", "homegrown", "pulsar-jms", "speedup");

        double sumHome = 0, sumJms = 0;
        for (String s : SELECTORS) {
            Selector ours = Selector.compile(s);
            SelectorSupport jms = SelectorSupport.build(s, true);

            double homeNs = timeEval(() -> ours.matches(resolver));
            double jmsNs = timeEval(() -> jmsMatch(jms, resolver));
            sumHome += homeNs;
            sumJms += jmsNs;

            System.out.printf("%-58s %12.1f %12.1f %8.2fx%n",
                    s.length() > 58 ? s.substring(0, 55) + "..." : s, homeNs, jmsNs, jmsNs / homeNs);
        }
        System.out.printf("%-58s %12.1f %12.1f %8.2fx%n", "-- mean --",
                sumHome / SELECTORS.length, sumJms / SELECTORS.length, sumJms / sumHome);

        benchCompile();

        // Touch the sink so it is observably live.
        System.out.printf("(sink=%d)%n", sink);
    }

    /** Compile cost: parse+build the AST / JMS support object, once-per-query in production. */
    private void benchCompile() throws Exception {
        long n = Math.min(ITERS, 100_000L);
        System.out.printf("%n=== Bench A: selector compile (ns/op) — iters=%d ===%n", n);
        System.out.printf("%-58s %12s %12s%n", "selector", "homegrown", "pulsar-jms");
        for (String s : SELECTORS) {
            double homeNs = timeCompile(n, () -> sink += Selector.compile(s).referencedNames().size());
            double jmsNs = timeCompile(n, () -> sink += compileJms(s));
            System.out.printf("%-58s %12.1f %12.1f%n",
                    s.length() > 58 ? s.substring(0, 55) + "..." : s, homeNs, jmsNs);
        }
    }

    // ---- timing helpers (manual nanoTime; warmup then measure) ----

    private double timeEval(BoolOp op) {
        for (long i = 0; i < WARMUP; i++) {
            if (op.run()) sink++;
        }
        long start = System.nanoTime();
        for (long i = 0; i < ITERS; i++) {
            if (op.run()) sink++;
        }
        return (System.nanoTime() - start) / (double) ITERS;
    }

    private double timeCompile(long iters, Runnable op) {
        for (long i = 0; i < Math.min(iters, 2_000L); i++) op.run();
        long start = System.nanoTime();
        for (long i = 0; i < iters; i++) op.run();
        return (System.nanoTime() - start) / (double) iters;
    }

    private static boolean jmsMatch(SelectorSupport jms, Function<String, Object> resolver) {
        try {
            return jms.matches(resolver);
        } catch (JMSException | RuntimeException e) {
            return false;
        }
    }

    private static int compileJms(String s) {
        try {
            return SelectorSupport.build(s, true) == null ? 0 : 1;
        } catch (JMSException | RuntimeException e) {
            return 0;
        }
    }

    @FunctionalInterface
    private interface BoolOp {
        boolean run();
    }
}
