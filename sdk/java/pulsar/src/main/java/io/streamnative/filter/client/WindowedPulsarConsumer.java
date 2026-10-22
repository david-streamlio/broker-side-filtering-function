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

package io.streamnative.filter.client;

import io.streamnative.filter.api.WindowControl;
import io.streamnative.filter.api.internal.WindowProgress;
import io.streamnative.filter.api.internal.WindowProgress.Marker;
import io.streamnative.filter.common.WindowProtocol;
import io.streamnative.filter.common.WindowStats;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * A {@link FilteringPulsarConsumer} built with {@code .window(TimeWindow)}: every receive path drops
 * the window's marker records, acknowledges them, and reports progress through {@link WindowControl}.
 *
 * <ul>
 *   <li>{@code receive(timeout, unit)} returns {@code null} when it consumes a marker, as on a
 *       timeout.</li>
 *   <li>Bounded window: once complete, blocking {@code receive()} and {@code receiveAsync()} fail with
 *       {@link IllegalStateException}; no further message will ever arrive.</li>
 *   <li>Open-ended window: the caught-up record is skipped and the receive carries on with live
 *       results.</li>
 *   <li>Batch receives return the batch without markers.</li>
 * </ul>
 *
 * <p>A windowed query's selector is fixed at subscribe time; {@code liveOnly()} consumers run as live
 * queries and keep {@link #setSelector}.
 */
final class WindowedPulsarConsumer<V> extends FilteringPulsarConsumer<V> implements WindowControl {

    private final WindowProgress progress;

    WindowedPulsarConsumer(Session session, PrivateQuery query, Consumer<V> delegate, String userSelector,
                           WindowProgress progress) {
        super(session, query, delegate, userSelector);
        this.progress = progress;
    }

    // ------------------------------------------------------------------ WindowControl

    @Override public boolean isOpenEnded()                     { return progress.isOpenEnded(); }
    @Override public boolean isCaughtUp()                      { return progress.isCaughtUp(); }
    @Override public boolean isComplete()                      { return progress.isComplete(); }
    @Override public WindowStats stats()                       { return progress.stats(); }
    @Override public CompletableFuture<WindowStats> caughtUp() { return progress.caughtUp(); }

    // ------------------------------------------------------------------ FilterControl

    @Override
    public synchronized void setSelector(String selector) {
        if (!progress.isLiveOnly()) {
            throw new UnsupportedOperationException("A windowed query's selector is fixed at subscribe time");
        }
        super.setSelector(selector);
    }

    // ------------------------------------------------------------------ receive

    @Override
    public Message<V> receive() throws PulsarClientException {
        while (true) {
            failIfComplete();
            Message<V> m = delegate.receive();
            if (consumeMarker(m) == Marker.NONE) {
                return m;
            }
        }
    }

    @Override
    public Message<V> receive(int timeout, TimeUnit unit) throws PulsarClientException {
        if (isComplete()) {
            return null;
        }
        Message<V> m = delegate.receive(timeout, unit);
        return consumeMarker(m) == Marker.NONE ? m : null;
    }

    @Override
    public CompletableFuture<Message<V>> receiveAsync() {
        if (isComplete()) {
            return CompletableFuture.failedFuture(windowComplete());
        }
        return delegate.receiveAsync().thenCompose(m -> switch (consumeMarker(m)) {
            case NONE -> CompletableFuture.completedFuture(m);
            case COMPLETE -> CompletableFuture.failedFuture(windowComplete());
            case CAUGHT_UP -> receiveAsync();
        });
    }

    @Override
    public Messages<V> batchReceive() throws PulsarClientException {
        return withoutMarkers(delegate.batchReceive());
    }

    @Override
    public CompletableFuture<Messages<V>> batchReceiveAsync() {
        return delegate.batchReceiveAsync().thenApply(this::withoutMarkers);
    }

    // ------------------------------------------------------------------ helpers

    /** Classify {@code m}; a marker is acknowledged and recorded, never returned as data. */
    private Marker consumeMarker(Message<V> m) {
        if (m == null) {
            return Marker.NONE;
        }
        Marker marker = WindowProgress.markerOf(m::getProperty);
        if (marker != Marker.NONE) {
            progress.onMarker(marker, m.getProperty(WindowProtocol.STATS_PROPERTY));
            delegate.acknowledgeAsync(m.getMessageId());
        }
        return marker;
    }

    private Messages<V> withoutMarkers(Messages<V> batch) {
        List<Message<V>> kept = new ArrayList<>(batch.size());
        for (Message<V> m : batch) {
            if (consumeMarker(m) == Marker.NONE) {
                kept.add(m);
            }
        }
        return kept.size() == batch.size() ? batch : new ListMessages<>(kept);
    }

    private void failIfComplete() {
        if (isComplete()) {
            throw windowComplete();
        }
    }

    private IllegalStateException windowComplete() {
        return new IllegalStateException("Window complete for queryId=" + queryId() + "; no more messages");
    }

    /** A batch with the markers removed. */
    private record ListMessages<T>(List<Message<T>> messages) implements Messages<T> {
        @Override public int size()                      { return messages.size(); }
        @Override public Iterator<Message<T>> iterator() { return messages.iterator(); }
    }
}
