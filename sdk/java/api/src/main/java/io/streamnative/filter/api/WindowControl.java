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

import io.streamnative.filter.common.WindowStats;
import java.util.concurrent.CompletableFuture;

/**
 * Progress of a consumer built with {@code .window(TimeWindow)}, from either SDK. Cast the consumer to
 * this interface, as with {@code FilterControl}.
 *
 * <p>The server marks the end of a window's history with a record the consumer never returns as data:
 * a completion record for a bounded window (nothing follows it), or a caught-up record for an
 * open-ended one (live events follow it). Until then, an empty receive or poll only means nothing has
 * arrived yet:
 *
 * <pre>{@code
 * // bounded: TimeWindow.last(1, TimeUnit.HOURS)
 * WindowControl window = (WindowControl) consumer;
 * while (!window.isComplete()) {
 *     Message<byte[]> m = consumer.receive(1, TimeUnit.SECONDS);
 *     if (m != null) { handle(m); consumer.acknowledge(m); }
 * }
 * WindowStats stats = window.stats();   // scanned, matched, how the window ended, status
 *
 * // open-ended: TimeWindow.since(1, TimeUnit.HOURS)
 * window.caughtUp().thenAccept(stats -> log.info("history delivered; now live"));
 * }</pre>
 *
 * <p>Results can contain duplicates if a server-side filter restarted mid-window (at-least-once);
 * dedupe on the {@code __src_msgid__} property / header.
 */
public interface WindowControl {

    /** True for an open-ended window ({@code since}, {@code allThenLive}, {@code liveOnly}): it never completes. */
    boolean isOpenEnded();

    /**
     * True once the window's history has been delivered: at the completion record (bounded), at the
     * caught-up record (open-ended), or immediately for {@code liveOnly()}.
     */
    boolean isCaughtUp();

    /** True once a bounded window's completion record has arrived; nothing follows it. Always false when open-ended. */
    boolean isComplete();

    /**
     * The server's stats for the window's history, or {@code null} until {@link #isCaughtUp()}. Also
     * {@code null} for {@code liveOnly()}, and if the stats couldn't be read.
     */
    WindowStats stats();

    /** Completes with {@link #stats()} when the window's history has been delivered. */
    CompletableFuture<WindowStats> caughtUp();
}
