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

package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.WindowSpec;

/**
 * Admin-side setup of a windowed query, before its dedicated filter instance starts: positions the
 * query's subscription at the window start and captures the window's bounds. A seam so the
 * orchestrator's windowed REGISTER can be unit-tested without a broker.
 */
interface WindowPreparer {

    /**
     * Create {@code subscription} on {@code sourceTopic} positioned at {@code fromMs} (reusing it if a
     * redelivered REGISTER already created it), then capture the topic's last message position and
     * whether the window starts before the earliest retained message. For an empty window the
     * subscription is removed again, since no filter instance will read it.
     *
     * @throws IllegalArgumentException if the topic doesn't exist or can't be read as one ordered log
     *         (more than one partition): a permanent rejection of the request
     */
    WindowPlan prepare(String sourceTopic, String subscription, long fromMs, long toMs) throws Exception;

    /**
     * @param spec  the window as the filter function receives it (bounds, end position, truncation)
     * @param empty nothing exists at or after {@code fromMs}: the orchestrator writes the completion
     *              record itself instead of spawning a filter that would never see a record
     */
    record WindowPlan(WindowSpec spec, boolean empty) {
    }
}
