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

/**
 * Thrown when a selector expression is syntactically invalid. Replaces the {@code javax.jms.JMSException}
 * previously surfaced by the ActiveMQ-based parser, so callers no longer depend on the JMS API.
 */
public final class SelectorParseException extends RuntimeException {

    /** Character offset into the expression where parsing failed, or {@code -1} if not available. */
    private final int position;

    public SelectorParseException(String message, int position) {
        super(position >= 0 ? message + " (at position " + position + ")" : message);
        this.position = position;
    }

    public SelectorParseException(String message) {
        this(message, -1);
    }

    /** Character offset of the error, or {@code -1} if unknown. */
    public int position() {
        return position;
    }
}
