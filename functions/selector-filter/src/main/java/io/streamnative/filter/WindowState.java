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

import io.streamnative.filter.common.LogPosition;
import io.streamnative.filter.common.WindowSpec;
import io.streamnative.filter.common.WindowStats;
import java.util.function.LongSupplier;

/**
 * Progress of one windowed query through its time window.
 *
 * <p>The function's subscription was positioned at the window start, so records arrive in log
 * order from (just before) {@code fromMs}. {@link #admit} classifies each record.
 *
 * <ul>
 *   <li><b>Bounded</b> ({@code toMs} set): the function evaluates {@link Admission#IN_WINDOW} /
 *       {@link Admission#LAST_IN_WINDOW} records and writes the completion record when the window
 *       ends. After that every record is {@link Admission#DONE} and is dropped.</li>
 *   <li><b>Open-ended</b> ({@code toMs} {@code null}): the window never ends. At the end of history
 *       (the captured end-of-log position) the function writes a caught-up record once
 *       ({@link Admission#LAST_OF_HISTORY} / {@link Admission#FIRST_AFTER_HISTORY}), then keeps
 *       evaluating live records.</li>
 * </ul>
 *
 * <p>Mutated only on the function's process thread, except {@link #reopen()} and
 * {@link #reopenCaughtUp()}, which a failed marker send calls so the redelivered record writes it again.
 */
final class WindowState {

    enum Admission {
        /** Before {@code fromMs}: the cursor reset lands at entry granularity, so a few arrive. */
        BEFORE_WINDOW,
        IN_WINDOW,
        /** The last record at or before the end-of-log position: evaluate it, then complete. */
        LAST_IN_WINDOW,
        /** Past {@code toMs} or past the end-of-log position: complete without evaluating it. */
        PAST_WINDOW,
        /** The window already completed. */
        DONE,
        /** Open-ended: the last historical record. Evaluate it, then write the caught-up record. */
        LAST_OF_HISTORY,
        /** Open-ended: a record after the end of history, which isn't marked yet. Write the caught-up record, then evaluate it. */
        FIRST_AFTER_HISTORY
    }

    private final WindowSpec spec;
    private final LongSupplier clock;
    private final long startedAtMs;

    private long scanned;
    private long matched;
    private long bytesScanned;
    private Long firstScannedMs;
    private Long lastScannedMs;
    private WindowStats.EndedBy endedBy;
    private volatile boolean done;
    private volatile boolean caughtUp;

    WindowState(WindowSpec spec, LongSupplier clock) {
        this.spec = spec;
        this.clock = clock;
        this.startedAtMs = clock.getAsLong();
    }

    WindowSpec spec() {
        return spec;
    }

    /**
     * Classify a record by its log-clock time ({@code 0} when unknown) and position ({@code null}
     * when unknown, e.g. in unit tests without a message).
     */
    Admission admit(long logTimeMs, LogPosition position) {
        if (done) {
            return Admission.DONE;
        }
        if (logTimeMs > 0 && logTimeMs < spec.getFromMs()) {
            return Admission.BEFORE_WINDOW;
        }
        if (spec.isOpenEnded()) {
            return admitOpenEnded(position);
        }
        if (logTimeMs > spec.getToMs()) {
            endedBy = WindowStats.EndedBy.WINDOW_END;
            return Admission.PAST_WINDOW;
        }
        if (spec.hasEndPosition() && position != null) {
            int cmp = position.compareTo(spec.getEndLedgerId(), spec.getEndEntryId());
            if (cmp > 0) {
                endedBy = WindowStats.EndedBy.LOG_END;
                return Admission.PAST_WINDOW;
            }
            if (cmp == 0 && position.isLastInEntry()) {
                endedBy = WindowStats.EndedBy.LOG_END;
                return Admission.LAST_IN_WINDOW;
            }
        }
        return Admission.IN_WINDOW;
    }

    private Admission admitOpenEnded(LogPosition position) {
        if (!caughtUp && spec.hasEndPosition() && position != null) {
            int cmp = position.compareTo(spec.getEndLedgerId(), spec.getEndEntryId());
            if (cmp == 0 && position.isLastInEntry()) {
                return Admission.LAST_OF_HISTORY;
            }
            if (cmp > 0) {
                return Admission.FIRST_AFTER_HISTORY;
            }
        }
        return Admission.IN_WINDOW;
    }

    void recordScanned(long logTimeMs, int bytes) {
        scanned++;
        bytesScanned += bytes;
        if (logTimeMs > 0) {
            if (firstScannedMs == null) {
                firstScannedMs = logTimeMs;
            }
            lastScannedMs = logTimeMs;
        }
    }

    void recordMatched() {
        matched++;
    }

    /** Mark the window done and return its completion stats. */
    WindowStats complete(String queryId) {
        done = true;
        return stats(queryId, endedBy);
    }

    /** Open-ended: mark the end of history delivered and return the stats for the historical part. */
    WindowStats caughtUp(String queryId) {
        caughtUp = true;
        return stats(queryId, WindowStats.EndedBy.LOG_END);
    }

    private WindowStats stats(String queryId, WindowStats.EndedBy ended) {
        return new WindowStats(queryId, spec.getFromMs(), spec.getToMs(), scanned, matched, bytesScanned,
                firstScannedMs, lastScannedMs, clock.getAsLong() - startedAtMs, ended,
                spec.isTruncatedByRetention() ? WindowStats.Status.TRUNCATED_BY_RETENTION
                                              : WindowStats.Status.COMPLETE);
    }

    /** Undo {@link #complete} after a failed completion send, so the window can complete again. */
    void reopen() {
        done = false;
    }

    /** Undo {@link #caughtUp} after a failed caught-up send, so it is written again. */
    void reopenCaughtUp() {
        caughtUp = false;
    }

    boolean isDone() {
        return done;
    }

    boolean isCaughtUp() {
        return caughtUp;
    }
}
