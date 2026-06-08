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
 * SDK family that produced a {@link FilterRequest}. Recorded by the orchestrator in the per-input-topic
 * registry entry. v1 has no per-type behavior; reserved for future routing decisions (e.g., output-topic
 * namespace selection for {@link #KAFKA} clients that require KoP-mapped namespaces).
 */
public enum ClientType {
    /** Standard Apache Pulsar Java client. */
    PULSAR,

    /** Apache Kafka client pointed at a KoP-enabled Pulsar broker. */
    KAFKA
}
