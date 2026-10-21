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

package io.streamnative.filter.api.internal;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <b>Internal API, not for application use; it may change without notice.</b> Window progress shared by
 * the Pulsar and Kafka windowed consumers: recognizes the completion and caught-up records and holds
 * their stats. Public only so both SDKs can use it; applications use {@link WindowControl}.
 */
public final class WindowProgress implements WindowControl {

    private static final Logger LOG = LoggerFactory.getLogger(WindowProgress.class);
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** What a record is, from the window's point of view. */
    public enum Marker {
        /** An ordinary result. */
        NONE,
        /** The bounded window's completion record: nothing follows. */
        COMPLETE,
        /** The open-ended window's caught-up record: live results follow. */
        CAUGHT_UP
    }

    private final boolean openEnded;
    private final boolean liveOnly;
    private final CompletableFuture<WindowStats> caughtUp = new CompletableFuture<>();
    private volatile boolean complete;

    private WindowProgress(boolean openEnded, boolean liveOnly) {
        this.openEnded = openEnded;
        this.liveOnly = liveOnly;
    }

    /** Progress of a windowed query that reads history ({@code openEnded}: keeps streaming after it). */
    public static WindowProgress forWindow(boolean openEnded) {
        return new WindowProgress(openEnded, false);
    }

    /** Progress of a {@code liveOnly()} consumer: no history, so caught up from the start. */
    public static WindowProgress liveOnly() {
        WindowProgress p = new WindowProgress(true, true);
        p.caughtUp.complete(null);
        return p;
    }

    /** True for a {@code liveOnly()} consumer, which runs as a live query (its selector can change). */
    public boolean isLiveOnly() {
        return liveOnly;
    }

    /** Classify a record from its properties / headers; {@code property} looks one up by name. */
    public static Marker markerOf(Function<String, String> property) {
        if ("true".equals(property.apply(WindowProtocol.COMPLETE_PROPERTY))) {
            return Marker.COMPLETE;
        }
        if ("true".equals(property.apply(WindowProtocol.CAUGHT_UP_PROPERTY))) {
            return Marker.CAUGHT_UP;
        }
        return Marker.NONE;
    }

    /**
     * Record a marker with its {@link WindowProtocol#STATS_PROPERTY} (JSON {@link WindowStats}, or
     * {@code null} if absent). Idempotent: a duplicate marker (a filter restarted after writing it)
     * keeps the first stats.
     */
    public void onMarker(Marker marker, String statsJson) {
        if (marker == Marker.NONE) {
            return;
        }
        WindowStats stats = parse(statsJson);
        if (marker == Marker.COMPLETE) {
            complete = true;
        }
        if (caughtUp.complete(stats)) {
            LOG.info("Window {}: {}", marker == Marker.COMPLETE ? "complete" : "caught up", stats);
        }
    }

    private static WindowStats parse(String statsJson) {
        if (statsJson == null || statsJson.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.readValue(statsJson, WindowStats.class);
        } catch (Exception e) {
            LOG.warn("Unreadable window stats: {}", e.toString());
            return null;
        }
    }

    @Override public boolean isOpenEnded()                     { return openEnded; }
    @Override public boolean isCaughtUp()                      { return caughtUp.isDone(); }
    @Override public boolean isComplete()                      { return complete; }
    @Override public WindowStats stats()                       { return caughtUp.getNow(null); }
    @Override public CompletableFuture<WindowStats> caughtUp() { return caughtUp; }
}
