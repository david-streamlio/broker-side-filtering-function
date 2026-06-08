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

package io.streamnative.filter.common;

/**
 * Lifecycle command on a {@link FilterRequest}, derived from the SDK call site.
 *
 * <ul>
 *   <li>{@link #REGISTER} — emitted by SDK subscribe(). Orchestrator either spawns a new
 *       per-input-topic filter function or reuses an existing one; registers the query and routes
 *       a QueryMessage onto the matching filter function's control topic. Idempotent on broker
 *       redelivery iff all FilterRequest fields match the existing registry entry for the same
 *       queryId; rejected if any field differs.</li>
 *   <li>{@link #MODIFY} — emitted by SDK setSelector / add-update-selector methods. Orchestrator
 *       republishes the QueryMessage with the new selector on the same queryId key — the filter
 *       function hot-reloads its compiled expression via TableView semantics.</li>
 *   <li>{@link #UNREGISTER} — emitted by consumer close() or SDK delete-selector. Orchestrator
 *       tombstones the queryId on every per-function control topic the registration was fanned
 *       out to. Idempotent on unknown queryId (silent success).</li>
 *   <li>{@link #HEARTBEAT} — emitted periodically by an SDK Session for all of its live
 *       queryIds. The body's {@code queryIds} list is authoritative; the message key is
 *       arbitrary. Orchestrator stamps {@code lastSeen} on each named QueryEntry; the sweeper
 *       synthesizes UNREGISTER for any queryId whose {@code lastSeen} is older than
 *       {@code heartbeat-max-stale-seconds}. Unknown queryIds in the list are silently ignored
 *       (probably already-tombstoned by a prior UNREGISTER).</li>
 * </ul>
 */
public enum Command {
    REGISTER,
    MODIFY,
    UNREGISTER,
    HEARTBEAT
}
