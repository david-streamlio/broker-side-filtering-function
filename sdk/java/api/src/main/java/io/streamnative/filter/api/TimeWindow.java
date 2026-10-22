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

package io.streamnative.filter.api;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Which part of a topic's log a filtered consumer reads: some history, live events, or both.
 *
 * <table>
 *   <caption>Factories</caption>
 *   <tr><th></th><th>Bounded: read history, then complete</th><th>Open-ended: history, then keep streaming</th></tr>
 *   <tr><td>From the beginning</td><td>{@link #all()}</td><td>{@link #allThenLive()}</td></tr>
 *   <tr><td>From a lookback or time</td><td>{@link #last(long, TimeUnit)}, {@link #between(Instant, Instant)}</td>
 *       <td>{@link #since(long, TimeUnit)}, {@link #since(Instant)}</td></tr>
 *   <tr><td>From now</td><td>(nothing to read)</td><td>{@link #liveOnly()}</td></tr>
 * </table>
 *
 * <p>Times are on the log's clock: the broker-entry timestamp when the broker appends one, else the
 * producer publish time. Not event time. Relative windows ({@code last}, {@code since(amount, unit)})
 * are resolved on the client's clock when the consumer subscribes. "Now" for a bounded window is the
 * last message present when the server registers the query.
 *
 * <p>"From the beginning" reads everything retained. Unlike a window that starts before the oldest
 * retained message, it is never reported as truncated.
 */
public final class TimeWindow {

    private enum Kind { ALL, ALL_THEN_LIVE, LIVE_ONLY, LAST, BETWEEN, SINCE_LOOKBACK, SINCE_TIME }

    /** {@code fromMs = 0} on the wire means "from the beginning" (see {@link #resolve}). */
    private static final long FROM_BEGINNING = 0L;

    private final Kind kind;
    private final long fromOrLookbackMs;
    private final long toMs;

    private TimeWindow(Kind kind, long fromOrLookbackMs, long toMs) {
        this.kind = kind;
        this.fromOrLookbackMs = fromOrLookbackMs;
        this.toMs = toMs;
    }

    /** Everything retained in the topic, then complete. A full scan: the most expensive window. */
    public static TimeWindow all() {
        return new TimeWindow(Kind.ALL, 0, 0);
    }

    /** Everything retained in the topic, then keep streaming new events. */
    public static TimeWindow allThenLive() {
        return new TimeWindow(Kind.ALL_THEN_LIVE, 0, 0);
    }

    /** Only new events, from now on: a live filtered consumer, caught up from the start. */
    public static TimeWindow liveOnly() {
        return new TimeWindow(Kind.LIVE_ONLY, 0, 0);
    }

    /** The last {@code amount} {@code unit}s up to now, then complete. */
    public static TimeWindow last(long amount, TimeUnit unit) {
        return new TimeWindow(Kind.LAST, lookbackMs(amount, unit), 0);
    }

    /** The last {@code lookback} up to now, then complete. */
    public static TimeWindow last(Duration lookback) {
        return new TimeWindow(Kind.LAST, lookbackMs(lookback), 0);
    }

    /** History between two instants (inclusive), then complete. {@code to} may be in the future. */
    public static TimeWindow between(Instant from, Instant to) {
        long fromMs = Objects.requireNonNull(from, "from").toEpochMilli();
        long untilMs = Objects.requireNonNull(to, "to").toEpochMilli();
        if (fromMs < 0 || untilMs <= fromMs) {
            throw new IllegalArgumentException("between requires 0 <= from < to, got [" + from + ", " + to + "]");
        }
        return new TimeWindow(Kind.BETWEEN, fromMs, untilMs);
    }

    /** History from {@code amount} {@code unit}s ago, then keep streaming new events. */
    public static TimeWindow since(long amount, TimeUnit unit) {
        return new TimeWindow(Kind.SINCE_LOOKBACK, lookbackMs(amount, unit), 0);
    }

    /** History from {@code lookback} ago, then keep streaming new events. */
    public static TimeWindow since(Duration lookback) {
        return new TimeWindow(Kind.SINCE_LOOKBACK, lookbackMs(lookback), 0);
    }

    /** History from an instant, then keep streaming new events. */
    public static TimeWindow since(Instant from) {
        long fromMs = Objects.requireNonNull(from, "from").toEpochMilli();
        if (fromMs < 0) {
            throw new IllegalArgumentException("since requires a non-negative instant, got " + from);
        }
        return new TimeWindow(Kind.SINCE_TIME, fromMs, 0);
    }

    /** True when the consumer keeps streaming after history; it never completes. */
    public boolean isOpenEnded() {
        return kind == Kind.ALL_THEN_LIVE || kind == Kind.LIVE_ONLY
                || kind == Kind.SINCE_LOOKBACK || kind == Kind.SINCE_TIME;
    }

    /** True for {@link #liveOnly()}: no history to read; it runs as a live filtered query. */
    public boolean isLiveOnly() {
        return kind == Kind.LIVE_ONLY;
    }

    /**
     * Absolute bounds sent to the server, resolved at {@code nowMs} (epoch ms, the client's clock).
     * {@code toMs} is {@code null} for open-ended windows. {@code fromMs = 0} means "from the
     * beginning", so a relative lookback reaching before the epoch is clamped to 1.
     *
     * @throws IllegalStateException for {@link #liveOnly()}, which has no history to bound
     */
    public Bounds resolve(long nowMs) {
        return switch (kind) {
            case ALL -> new Bounds(FROM_BEGINNING, nowMs);
            case ALL_THEN_LIVE -> new Bounds(FROM_BEGINNING, null);
            case LAST -> new Bounds(Math.max(1L, nowMs - fromOrLookbackMs), nowMs);
            case BETWEEN -> new Bounds(fromOrLookbackMs, toMs);
            case SINCE_LOOKBACK -> new Bounds(Math.max(1L, nowMs - fromOrLookbackMs), null);
            case SINCE_TIME -> new Bounds(fromOrLookbackMs, null);
            case LIVE_ONLY -> throw new IllegalStateException("liveOnly() reads no history");
        };
    }

    /** Absolute window bounds in epoch ms; {@code toMs} {@code null} when open-ended. */
    public record Bounds(long fromMs, Long toMs) {
    }

    private static long lookbackMs(long amount, TimeUnit unit) {
        Objects.requireNonNull(unit, "unit");
        if (amount <= 0) {
            throw new IllegalArgumentException("lookback must be positive, got " + amount + " " + unit);
        }
        return unit.toMillis(amount);
    }

    private static long lookbackMs(Duration lookback) {
        Objects.requireNonNull(lookback, "lookback");
        if (lookback.isNegative() || lookback.isZero()) {
            throw new IllegalArgumentException("lookback must be positive, got " + lookback);
        }
        return lookback.toMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof TimeWindow that)) return false;
        return kind == that.kind && fromOrLookbackMs == that.fromOrLookbackMs && toMs == that.toMs;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, fromOrLookbackMs, toMs);
    }

    @Override
    public String toString() {
        return switch (kind) {
            case ALL -> "TimeWindow.all()";
            case ALL_THEN_LIVE -> "TimeWindow.allThenLive()";
            case LIVE_ONLY -> "TimeWindow.liveOnly()";
            case LAST -> "TimeWindow.last(" + Duration.ofMillis(fromOrLookbackMs) + ")";
            case BETWEEN -> "TimeWindow.between(" + Instant.ofEpochMilli(fromOrLookbackMs) + ", "
                    + Instant.ofEpochMilli(toMs) + ")";
            case SINCE_LOOKBACK -> "TimeWindow.since(" + Duration.ofMillis(fromOrLookbackMs) + ")";
            case SINCE_TIME -> "TimeWindow.since(" + Instant.ofEpochMilli(fromOrLookbackMs) + ")";
        };
    }
}
