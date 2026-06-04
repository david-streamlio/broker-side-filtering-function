package io.streamnative.filter;

import io.streamnative.pulsar.jms.selectors.SelectorSupport;
import javax.jms.JMSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SelectorEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorEvaluator.class);

    /** JMS selector reserved words (uppercased). Identifiers matching one of these are not bindings. */
    private static final Set<String> JMS_KEYWORDS = Set.of(
            "AND", "OR", "NOT", "BETWEEN", "LIKE", "IN", "IS", "NULL", "TRUE", "FALSE", "ESCAPE");

    /**
     * Matches a single-quoted JMS string literal (with doubled-quote escape) or a JVM-ish
     * identifier. Quoted regions are matched in their entirety so the identifier alternation
     * never picks anything up from inside them.
     */
    private static final Pattern TOKEN = Pattern.compile(
            "'(?:[^']|'')*'|[A-Za-z_$][A-Za-z0-9_$]*");

    private final SelectorSupport support;
    private final String expression;
    private final Set<String> referencedNames;

    private SelectorEvaluator(SelectorSupport support, String expression, Set<String> referencedNames) {
        this.support = support;
        this.expression = expression;
        this.referencedNames = referencedNames;
    }

    public static SelectorEvaluator compile(String expression) throws JMSException {
        if (expression == null || expression.isBlank()) {
            throw new JMSException("Selector expression is empty");
        }
        SelectorSupport support = SelectorSupport.build(expression, true);
        if (support == null) {
            throw new JMSException("Selector expression is empty");
        }
        return new SelectorEvaluator(support, expression, extractReferencedNames(expression));
    }

    public boolean matches(MessageBindings bindings) {
        return matches(bindings, null);
    }

    /**
     * Evaluate the selector, resolving value-field references through {@code decoder} when one is
     * present (a query with a structured value schema). When {@code decoder} is {@code null},
     * value-field references fall back to best-effort JSON parsing — identical to {@link #matches(MessageBindings)}.
     */
    public boolean matches(MessageBindings bindings, ValueDecoder decoder) {
        try {
            return decoder == null
                    ? support.matches(bindings::lookup)
                    : support.matches(name -> bindings.lookup(name, decoder));
        } catch (JMSException | RuntimeException e) {
            // Logged at DEBUG, not WARN: per-message failures on a single bad selector would
            // otherwise flood logs. The "nothing matched" outcome is already visible via the
            // filter.messages.dropped metric; enable DEBUG on this logger when diagnosing why
            // a specific selector never produces output.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Selector evaluation failed for expression [{}]: {}", expression, e.toString());
            }
            return false;
        }
    }

    public String getExpression() {
        return expression;
    }

    /** All non-keyword identifiers referenced by the selector — i.e. potential binding names. */
    public Set<String> getReferencedNames() {
        return referencedNames;
    }

    /**
     * Returns {@code true} iff the selector references only the {@code __key__} binding (no
     * topic, no user properties). Used by callers to enable a fast path where {@code record
     * .getTopicName()} and {@code record.getProperties()} are never resolved.
     */
    public boolean referencesOnlyKey() {
        return referencedNames.size() == 1
                && referencedNames.contains(MessageBindings.KEY_BINDING);
    }

    /** True iff the selector references {@code __topic__}. */
    public boolean referencesTopic() {
        return referencedNames.contains(MessageBindings.TOPIC_BINDING);
    }

    /** True iff the selector references at least one user property (not __key__ or __topic__). */
    public boolean referencesUserProperties() {
        for (String n : referencedNames) {
            if (!MessageBindings.KEY_BINDING.equals(n) && !MessageBindings.TOPIC_BINDING.equals(n)) {
                return true;
            }
        }
        return false;
    }

    static Set<String> extractReferencedNames(String expression) {
        Set<String> names = new HashSet<>();
        Matcher m = TOKEN.matcher(expression);
        while (m.find()) {
            String tok = m.group();
            if (tok.charAt(0) == '\'') {
                continue; // quoted string literal
            }
            String upper = tok.toUpperCase();
            if (JMS_KEYWORDS.contains(upper)) {
                continue;
            }
            names.add(tok);
        }
        return Collections.unmodifiableSet(names);
    }
}
