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

import java.util.List;

/**
 * Dynamic-selector operations exposed by the consumers both SDKs return from {@code subscribe()}.
 *
 * <p>The returned consumer (Pulsar {@code Consumer} or Kafka {@code Consumer}) also implements this
 * interface; callers cast when they want to inspect or mutate the active selector:
 *
 * <pre>{@code
 * Consumer<byte[]> c = session.newConsumer().topic(...).selector("a = 'x'").subscribe();
 * ((FilterControl) c).setSelector("a = 'y'");
 * }</pre>
 *
 * <p>Single-selector model: each consumer has exactly one user-side selector at a time.
 * {@link #setSelector(String)} replaces it. The underlying {@code queryId} is stable for the
 * lifetime of the consumer — the function module performs in-place hot-reload when the same
 * key is re-published to the control topic.
 */
public interface FilterControl {

    /**
     * Replace the active user-side selector. The session re-composes the effective selector as
     * {@code <topic-constraint> AND (<selector>)} and republishes the query on the same
     * {@link #queryId()} key, prompting the server-side filter to swap in the new compiled expression.
     *
     * @param selector a JMS selector expression; must be non-blank
     * @throws IllegalArgumentException if {@code selector} is null, blank or malformed; the previous
     *                                  selector stays active
     * @throws FilterException          if the change couldn't be delivered to the server
     * @throws UnsupportedOperationException for a windowed query, whose selector is fixed
     */
    void setSelector(String selector);

    /**
     * The current user-side selector (the portion AFTER the topic constraint). May be
     * {@code null} when no user-side filter is active.
     */
    String currentSelector();

    /**
     * Stable identifier for this consumer's registered query. Same value before and after
     * {@link #setSelector(String)}.
     */
    String queryId();

    /** Source topics this consumer is filtering, in builder-supplied order. */
    List<String> sourceTopics();

    /** Private output topic the consumer reads (for Kafka clients, the topic name under KoP's namespace). */
    String privateTopic();
}
