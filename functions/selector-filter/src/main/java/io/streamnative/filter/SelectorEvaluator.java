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

package io.streamnative.filter;

import io.streamnative.filter.common.selector.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * Thin wrapper over the homegrown {@link Selector} engine. Compiles a JMS-selector expression once
 * and evaluates it per message against a {@link MessageBindings} resolver. The set of referenced
 * identifier names comes straight from the compiled AST ({@link Selector#referencedNames()}), which
 * backs the {@code referencesOnly*} fast-path checks.
 */
public final class SelectorEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorEvaluator.class);

    private final Selector selector;
    private final String expression;
    private final Set<String> referencedNames;

    private SelectorEvaluator(Selector selector) {
        this.selector = selector;
        this.expression = selector.expression();
        this.referencedNames = selector.referencedNames();
    }

    /**
     * Compile {@code expression}.
     *
     * @throws io.streamnative.filter.common.selector.SelectorParseException if the expression is
     *         empty/blank or syntactically invalid
     */
    public static SelectorEvaluator compile(String expression) {
        return new SelectorEvaluator(Selector.compile(expression));
    }

    public boolean matches(MessageBindings bindings) {
        return matches(bindings, null);
    }

    /**
     * Evaluate the selector, resolving value-field references through {@code decoder} when one is
     * present (a query with a structured value schema). When {@code decoder} is {@code null},
     * value-field references fall back to best-effort JSON parsing — identical to
     * {@link #matches(MessageBindings)}.
     */
    public boolean matches(MessageBindings bindings, ValueDecoder decoder) {
        try {
            return decoder == null
                    ? selector.matches(bindings::lookup)
                    : selector.matches(name -> bindings.lookup(name, decoder));
        } catch (RuntimeException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Selector evaluation failed for expression [{}]: {}", expression, e.toString());
            }
            return false;
        }
    }

    public String getExpression() {
        return expression;
    }

    /** All identifier names referenced by the selector — i.e. potential binding names. */
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

    /** True iff the selector references at least one user property (not a built-in {@code __x__} binding). */
    public boolean referencesUserProperties() {
        for (String n : referencedNames) {
            if (!MessageBindings.KEY_BINDING.equals(n) && !MessageBindings.TOPIC_BINDING.equals(n)
                    && !MessageBindings.PUBLISH_TIME_BINDING.equals(n)
                    && !MessageBindings.EVENT_TIME_BINDING.equals(n)) {
                return true;
            }
        }
        return false;
    }
}
