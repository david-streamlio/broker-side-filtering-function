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

package io.streamnative.filter.mcp;

/**
 * A thin SQL-to-JMS normalizer for agent-written selectors. A JMS selector is a SQL-92 {@code WHERE} clause,
 * so agents write it fluently, but two SQL habits aren't JMS: {@code !=} for not-equal, and quoted
 * identifiers ({@code "event_type"}, or MySQL's {@code `event_type`}). This rewrites just those, outside
 * single-quoted string literals, and leaves everything else to the parser and the field check.
 *
 * <p>A quoted name loses its quotes only if the selector grammar can name it unquoted; a name it can't
 * (a hyphen, a space, a keyword) is rejected here with a plain reason, because no selector could filter
 * on that field anyway.
 *
 * <p>Only where a field name can stand: right after a comparison operator, {@code LIKE}, {@code IN},
 * {@code BETWEEN}, an opening parenthesis or a comma, double quotes are far more often a string written
 * with the wrong quotes ({@code event_type = "login"}) than a field. Those are left for the parser, whose
 * error explains that strings go in single quotes.
 */
final class SelectorNormalizer {

    private SelectorNormalizer() {
    }

    private static final java.util.regex.Pattern VALUE_KEYWORD =
            java.util.regex.Pattern.compile("(?i).*\\b(LIKE|IN|BETWEEN|ESCAPE)\\s*$", java.util.regex.Pattern.DOTALL);

    /** Whether what's been written so far ends where a value, not a field name, comes next. */
    private static boolean valuePosition(CharSequence written) {
        int j = written.length() - 1;
        while (j >= 0 && Character.isWhitespace(written.charAt(j))) {
            j--;
        }
        if (j < 0) {
            return false;
        }
        char last = written.charAt(j);
        if ("=<>(,+-*/".indexOf(last) >= 0) {
            return true;
        }
        return VALUE_KEYWORD.matcher(written).matches();
    }

    /**
     * The selector with {@code !=} as {@code <>} and quoted identifiers unquoted. An unterminated quote is
     * left as it is, for the parser to report with its position.
     *
     * @throws IllegalArgumentException if a quoted identifier names a field no selector can express
     */
    static String normalize(String selector) {
        StringBuilder out = new StringBuilder(selector.length());
        int i = 0;
        int n = selector.length();
        while (i < n) {
            char c = selector.charAt(i);
            if (c == '\'') {
                // A string literal, copied as is; '' inside it is an escaped quote, not its end.
                int end = i + 1;
                while (end < n) {
                    if (selector.charAt(end) == '\'') {
                        if (end + 1 < n && selector.charAt(end + 1) == '\'') {
                            end += 2;
                            continue;
                        }
                        break;
                    }
                    end++;
                }
                int stop = Math.min(end + 1, n);
                out.append(selector, i, stop);
                i = stop;
            } else if ((c == '"' || c == '`') && valuePosition(out)) {
                // A value in the wrong quotes: copy it whole, closing quote included, for the parser to report.
                int end = selector.indexOf(c, i + 1);
                int stop = end < 0 ? n : end + 1;
                out.append(selector, i, stop);
                i = stop;
            } else if (c == '"' || c == '`') {
                int end = selector.indexOf(c, i + 1);
                if (end < 0) {
                    out.append(selector, i, n); // unterminated: let the parser say where
                    break;
                }
                String name = selector.substring(i + 1, end);
                if (!FieldCatalog.nameable(name)) {
                    throw new IllegalArgumentException("the quoted field name " + c + name + c + " can't be used in a "
                            + "selector: field names must be plain identifiers (letters, digits, _ and $, dots for "
                            + "nested fields) and not keywords. Call get_schema for the fields you can filter on");
                }
                out.append(name);
                i = end + 1;
            } else if (c == '!' && i + 1 < n && selector.charAt(i + 1) == '=') {
                out.append("<>");
                i += 2;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }
}
