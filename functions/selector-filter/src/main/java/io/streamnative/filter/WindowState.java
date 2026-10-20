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

import io.streamnative.filter.common.WindowSpec;
import io.streamnative.filter.common.WindowStats;
import java.util.function.LongSupplier;

/**
 * Progress of one windowed query through its time window.
 *
 * <p>The function's subscription was positioned at the window start, so records arrive in log
 * order from (just before) {@code fromMs}. {@link #admit} classifies each record; the function
 * evaluates the selector only for {@link Admission#IN_WINDOW} / {@link Admission#LAST_IN_WINDOW}
 * records and writes the completion record when the window ends. After that every record is
 * {@link Admission#DONE} and is dropped.
 *
 * <p>Mutated only on the function's process thread, except {@link #reopen()}, which a failed
 * completion send calls so the redelivered record completes the window again.
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
        DONE
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
    Admission admit(long logTimeMs, MessagePosition position) {
        if (done) {
            return Admission.DONE;
        }
        if (logTimeMs > 0 && logTimeMs < spec.getFromMs()) {
            return Admission.BEFORE_WINDOW;
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
        return new WindowStats(queryId, spec.getFromMs(), spec.getToMs(), scanned, matched, bytesScanned,
                firstScannedMs, lastScannedMs, clock.getAsLong() - startedAtMs, endedBy,
                spec.isTruncatedByRetention() ? WindowStats.Status.TRUNCATED_BY_RETENTION
                                              : WindowStats.Status.COMPLETE);
    }

    /** Undo {@link #complete} after a failed completion send, so the window can complete again. */
    void reopen() {
        done = false;
    }

    boolean isDone() {
        return done;
    }
}
