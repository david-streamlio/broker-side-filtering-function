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

import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A validated {@code get_entity_events} request: an entity key, how far back to read, and how many
 * events to return.
 *
 * <p>Agent input is treated as input. The key must match {@link #KEY}, which admits no quotes or
 * spaces, so it can't change the selector it's placed in. A lookback beyond the server's maximum is
 * rejected rather than shortened, because a shorter window answers a different question; a
 * {@code max_events} above the maximum is lowered to it, and the result says how many events matched.
 */
record EntityQuery(String key, Duration lookback, int maxEvents) {

    /** Letters, digits and {@code . _ : -}, up to 128 characters. */
    static final Pattern KEY = Pattern.compile("[A-Za-z0-9._:-]{1,128}");
    static final Duration DEFAULT_LOOKBACK = Duration.ofHours(1);
    static final int DEFAULT_MAX_EVENTS = 50;

    private static final Pattern DURATION = Pattern.compile("(\\d+)\\s*([smhd])");

    /** What one call may ask for: set by the server's operator, not by the agent. */
    record Limits(Duration maxLookback, int maxEvents, Duration timeout) {
    }

    /**
     * Validate tool arguments against {@code limits}.
     *
     * @throws IllegalArgumentException with a message the agent can act on
     */
    static EntityQuery parse(Map<String, Object> args, Limits limits) {
        Object rawKey = args == null ? null : args.get("key");
        if (!(rawKey instanceof String key) || !KEY.matcher(key).matches()) {
            throw new IllegalArgumentException("key must be 1-128 letters, digits or . _ : - (got "
                    + (rawKey == null ? "nothing" : "\"" + rawKey + "\"") + ")");
        }

        Duration lookback = DEFAULT_LOOKBACK.compareTo(limits.maxLookback()) > 0 ? limits.maxLookback() : DEFAULT_LOOKBACK;
        Object rawLookback = args.get("lookback");
        if (rawLookback != null) {
            lookback = parseDuration(String.valueOf(rawLookback));
        }
        if (lookback.compareTo(limits.maxLookback()) > 0) {
            throw new IllegalArgumentException("lookback " + rawLookback + " is longer than this server allows ("
                    + format(limits.maxLookback()) + "); ask for a shorter window");
        }

        int maxEvents = DEFAULT_MAX_EVENTS;
        Object rawMax = args.get("max_events");
        if (rawMax != null) {
            maxEvents = parseCount(rawMax);
        }
        return new EntityQuery(key, lookback, Math.min(maxEvents, limits.maxEvents()));
    }

    /** The selector for this key. Safe to build by concatenation: {@link #KEY} admits no quotes. */
    String selector() {
        return "__key__ = '" + key + "'";
    }

    /** {@code "90s"}, {@code "15m"}, {@code "24h"}, {@code "1d"}. */
    static Duration parseDuration(String text) {
        Matcher m = DURATION.matcher(text.trim().toLowerCase());
        if (!m.matches()) {
            throw new IllegalArgumentException("lookback must look like 90s, 15m, 6h or 1d (got \"" + text + "\")");
        }
        long n = Long.parseLong(m.group(1));
        Duration d = switch (m.group(2)) {
            case "s" -> Duration.ofSeconds(n);
            case "m" -> Duration.ofMinutes(n);
            case "h" -> Duration.ofHours(n);
            default -> Duration.ofDays(n);
        };
        if (d.isZero()) {
            throw new IllegalArgumentException("lookback must be longer than zero");
        }
        return d;
    }

    /** Hours up to two days ({@code "24h"}, not {@code "1d"}), then days. */
    static String format(Duration d) {
        long s = d.toSeconds();
        if (s % 86_400 == 0 && s >= 2 * 86_400) {
            return s / 86_400 + "d";
        }
        if (s % 3_600 == 0) {
            return s / 3_600 + "h";
        }
        return s % 60 == 0 ? s / 60 + "m" : s + "s";
    }

    private static int parseCount(Object raw) {
        long n;
        try {
            n = raw instanceof Number num ? num.longValue() : Long.parseLong(String.valueOf(raw).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("max_events must be a whole number (got \"" + raw + "\")");
        }
        if (n < 1) {
            throw new IllegalArgumentException("max_events must be at least 1");
        }
        return (int) Math.min(n, Integer.MAX_VALUE);
    }
}
