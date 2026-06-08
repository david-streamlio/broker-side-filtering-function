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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Objects;

/**
 * Fluent builder for {@link Session}.
 *
 * <p>Two ways to provide Pulsar handles:
 * <ul>
 *   <li>Pass connection URLs and let the builder construct {@link PulsarClient} and
 *       {@link PulsarAdmin}. The session then owns and closes them.</li>
 *   <li>Pass existing {@link PulsarClient}/{@link PulsarAdmin} instances. The session uses but
 *       does NOT close them — caller retains ownership.</li>
 * </ul>
 *
 * <p>If no admin handle (or admin URL) is provided, per-topic policies cannot be applied. The
 * session will log a warning and degrade gracefully: private topics still work, but operators
 * must rely on namespace defaults for TTL/retention/inactive-cleanup.
 */
public final class SessionBuilder {

    private String serviceUrl;
    private String adminUrl;
    private PulsarClient pulsarClient;
    private PulsarAdmin pulsarAdmin;

    private String controlTopic;
    private String privateTopicTenant    = "public";
    private String privateTopicNamespace = "filtered";
    private PrivateTopicPolicy privateTopicPolicy = PrivateTopicPolicy.DEFAULT;
    /** Heartbeat publish interval. {@code 0} disables. Default 30s matches the orchestrator default
     *  heartbeat-max-stale-seconds=90 (three missed heartbeats → declared dead). */
    private long heartbeatIntervalSeconds = 30L;

    SessionBuilder() {}

    public SessionBuilder serviceUrl(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        return this;
    }

    public SessionBuilder adminUrl(String adminUrl) {
        this.adminUrl = adminUrl;
        return this;
    }

    public SessionBuilder pulsarClient(PulsarClient client) {
        this.pulsarClient = client;
        return this;
    }

    public SessionBuilder pulsarAdmin(PulsarAdmin admin) {
        this.pulsarAdmin = admin;
        return this;
    }

    public SessionBuilder controlTopic(String controlTopic) {
        this.controlTopic = controlTopic;
        return this;
    }

    public SessionBuilder privateTopicTenant(String tenant) {
        this.privateTopicTenant = tenant;
        return this;
    }

    public SessionBuilder privateTopicNamespace(String ns) {
        this.privateTopicNamespace = ns;
        return this;
    }

    public SessionBuilder privateTopicPolicy(PrivateTopicPolicy policy) {
        this.privateTopicPolicy = policy == null ? PrivateTopicPolicy.DEFAULT : policy;
        return this;
    }

    /**
     * Set the heartbeat publish interval (in seconds). The Session sends one HEARTBEAT message
     * per interval listing all of its currently-live queryIds; the orchestrator uses these to
     * detect SDK consumers that died without calling close(). Set to {@code 0} to disable.
     * Defaults to {@code 30}, matching the orchestrator default {@code heartbeat-max-stale-seconds=90}
     * (three missed heartbeats before a queryId is declared dead).
     */
    public SessionBuilder heartbeatIntervalSeconds(long seconds) {
        this.heartbeatIntervalSeconds = Math.max(0L, seconds);
        return this;
    }

    /** Convenience: turn off heartbeats entirely. Equivalent to {@code heartbeatIntervalSeconds(0)}. */
    public SessionBuilder disableHeartbeat() {
        this.heartbeatIntervalSeconds = 0L;
        return this;
    }

    public Session build() throws PulsarClientException {
        Objects.requireNonNull(controlTopic, "controlTopic is required");
        if (pulsarClient == null && (serviceUrl == null || serviceUrl.isBlank())) {
            throw new IllegalStateException("Provide either serviceUrl or pulsarClient");
        }
        Objects.requireNonNull(privateTopicTenant, "privateTopicTenant is required");
        Objects.requireNonNull(privateTopicNamespace, "privateTopicNamespace is required");

        boolean ownsClient = false;
        PulsarClient client = pulsarClient;
        if (client == null) {
            client = PulsarClient.builder().serviceUrl(serviceUrl).build();
            ownsClient = true;
        }

        boolean ownsAdmin = false;
        PulsarAdmin admin = pulsarAdmin;
        if (admin == null && adminUrl != null && !adminUrl.isBlank()) {
            try {
                admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
                ownsAdmin = true;
            } catch (PulsarClientException e) {
                if (ownsClient) {
                    try { client.close(); } catch (Exception ignored) {}
                }
                throw e;
            }
        }

        return new Session(client, ownsClient, admin, ownsAdmin,
                controlTopic, privateTopicTenant, privateTopicNamespace, privateTopicPolicy,
                heartbeatIntervalSeconds);
    }
}
