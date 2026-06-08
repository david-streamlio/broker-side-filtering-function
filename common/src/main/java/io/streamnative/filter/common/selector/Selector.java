/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
