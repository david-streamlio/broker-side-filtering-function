package io.streamnative.filter.common.selector;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

/**
 * A compiled JMS message selector. Parse once with {@link #compile(String)}, then evaluate many
 * times with {@link #matches(Function)} against a binding resolver ({@code name → value}, where the
 * value is a {@link String} / {@link Number} / {@link Boolean} / {@code null}).
 *
 * <p>This is a self-contained replacement for the ActiveMQ-based JMS selector parser — same grammar
 * and three-valued semantics, no {@code javax.jms} dependency. A record matches only when the
 * expression evaluates to {@code TRUE} (UNKNOWN and FALSE both mean "no match").
 */
public final class Selector {

    private final String expression;
    private final Expression ast;
    private final Set<String> referencedNames;

    private Selector(String expression, Expression ast, Set<String> referencedNames) {
        this.expression = expression;
        this.ast = ast;
        this.referencedNames = referencedNames;
    }

    /**
     * Parse {@code expression} into a compiled selector.
     *
     * @throws SelectorParseException if the expression is empty/blank or syntactically invalid
     */
    public static Selector compile(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new SelectorParseException("Selector expression is empty");
        }
        Parser parser = new Parser(new Lexer(expression).tokenize());
        Expression ast = parser.parse();
        return new Selector(expression, ast,
                Collections.unmodifiableSet(parser.referencedNames()));
    }

    /** Evaluate against {@code resolver}; {@code true} only when the expression is definitely TRUE. */
    public boolean matches(Function<String, Object> resolver) {
        return Boolean.TRUE.equals(ast.evaluate(resolver));
    }

    /** The original expression text. */
    public String expression() {
        return expression;
    }

    /** All identifier names referenced by the expression (insertion order). */
    public Set<String> referencedNames() {
        return referencedNames;
    }
}
