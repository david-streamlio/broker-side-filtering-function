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

import java.util.List;
import java.util.Objects;

/**
 * Immutable handle for a query registered with the orchestrator. Returned from
 * {@link Session#registerPrivateQuery(List, String)} (and its overloads); passed back to
 * {@link Session#updatePrivateQuery(PrivateQuery, String)} and
 * {@link Session#retirePrivateQuery(PrivateQuery)} for lifecycle operations.
 *
 * <p>Both the Pulsar SDK ({@link FilterableConsumerBuilder}) and the Kafka SDK in the
 * {@code client-kafka} module use this as the contract between the registration layer (owned by
 * {@link Session}) and the per-transport consumer wrappers.
 */
public final class PrivateQuery {

    private final String queryId;
    private final String privateTopic;
    private final List<String> sourceTopics;

    PrivateQuery(String queryId, String privateTopic, List<String> sourceTopics) {
        this.queryId      = Objects.requireNonNull(queryId, "queryId");
        this.privateTopic = Objects.requireNonNull(privateTopic, "privateTopic");
        this.sourceTopics = List.copyOf(sourceTopics);
    }

    /**
     * Rebuild a {@code PrivateQuery} handle from values previously persisted by the caller
     * (e.g., in a database). Intended for stateful agents that survive process restarts and
     * need to call {@link Session#updatePrivateQuery} or {@link Session#retirePrivateQuery}
     * on a queryId that was originally returned by an earlier process.
     *
     * <p>The SDK does not validate the queryId against the orchestrator — the orchestrator
     * silently no-ops on UNREGISTER for unknown queryIds and silently routes MODIFY to
     * matching filter-function instances. The caller is responsible for supplying values
     * that match what {@link Session#registerPrivateQuery} originally returned.
     */
    public static PrivateQuery fromPersistedState(String queryId,
                                                  String privateTopic,
                                                  List<String> sourceTopics) {
        return new PrivateQuery(queryId, privateTopic, sourceTopics);
    }

    /** Stable identifier; same value used to publish, update, and retract the query. */
    public String queryId() {
        return queryId;
    }

    /**
     * Fully-qualified Pulsar topic the function writes matching messages to. Per-transport
     * wrappers subscribe to this topic (or its KoP-mapped Kafka name).
     */
    public String privateTopic() {
        return privateTopic;
    }

    /** The list of source topics the user requested, in builder-supplied order. */
    public List<String> sourceTopics() {
        return sourceTopics;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PrivateQuery that)) return false;
        return queryId.equals(that.queryId)
                && privateTopic.equals(that.privateTopic)
                && sourceTopics.equals(that.sourceTopics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, privateTopic, sourceTopics);
    }

    @Override
    public String toString() {
        return "PrivateQuery{queryId='" + queryId + "', privateTopic='" + privateTopic
                + "', sourceTopics=" + sourceTopics + "}";
    }
}
