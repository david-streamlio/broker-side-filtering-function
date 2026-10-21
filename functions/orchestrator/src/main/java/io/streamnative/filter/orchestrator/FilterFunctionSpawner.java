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

import io.streamnative.filter.common.FilterRequest;

/**
 * Strategy for materializing a per-input-topic filter-function instance. Phase 4 supplies a
 * production implementation backed by {@code PulsarAdmin.functions().createFunction(...)} that
 * spawns the function from a pre-uploaded Pulsar package URL and configures the per-function
 * control topic. Phase 3 uses a no-op stub for unit testing the orchestrator's routing logic.
 *
 * <p>Returns a {@link RegistryEntry} describing the materialized function (its name + its control
 * topic). The orchestrator writes this entry to the registry store after a successful spawn.
 *
 * <p>Implementations MUST be idempotent: invoking with the same {@code registryKey} when the
 * function is already running is a no-op and should return a {@link RegistryEntry} consistent with
 * the running instance.
 */
interface FilterFunctionSpawner {

    /**
     * Materialize a filter-function instance for the given registry key (input topic FQN or
     * topicsPattern).
     *
     * @param registryKey the input topic FQN or topicsPattern that this filter function is bound to
     * @param req         the originating FilterRequest (provides input topics, pattern, serializers,
     *                    clientType — orchestrator uses these to populate the registry entry)
     * @param cfg         orchestrator configuration (package URL, tenant/namespace, parallelism…)
     */
    RegistryEntry spawn(String registryKey, FilterRequest req, OrchestratorConfig cfg) throws Exception;

    /**
     * Spawn a dedicated filter function for one windowed query: input {@code sourceTopic}, on
     * {@code subscription} (already positioned at the window start), parallelism 1 so the output keeps
     * log order. {@code beforeDeploy} runs once the control topic exists and before the function is
     * created: the orchestrator publishes the windowed {@code QueryMessage} there, so the function
     * preloads it. A function that started without its query would ack and drop the whole backlog
     * (spike S1).
     */
    RegistryEntry spawnWindowed(String queryId, String sourceTopic, String subscription, FilterRequest req,
                                OrchestratorConfig cfg, ControlTopicAction beforeDeploy) throws Exception;

    /** Work done with a freshly prepared control topic. */
    @FunctionalInterface
    interface ControlTopicAction {
        void accept(String controlTopic) throws Exception;
    }
}
