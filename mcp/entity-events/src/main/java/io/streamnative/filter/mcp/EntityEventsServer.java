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

package io.streamnative.filter.mcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.json.jackson2.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.streamnative.filter.client.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * An MCP server over stdio with two tools: {@code get_entity_events} (see {@link EntityEventsTool}), one
 * entity's events over a recent window, and {@code watch_entity_events} (see {@link EntityWatchTool}), its
 * history and then its new events as they happen.
 *
 * <p>Configured by environment variables; the defaults point at the time-windowed read demo's broker
 * ({@code make window-demo}):
 *
 * <table>
 *   <caption>Environment</caption>
 *   <tr><td>{@code PULSAR_SERVICE_URL}</td><td>{@code pulsar://localhost:16650}</td></tr>
 *   <tr><td>{@code PULSAR_ADMIN_URL}</td><td>{@code http://localhost:18080}</td></tr>
 *   <tr><td>{@code FILTER_CONTROL_TOPIC}</td><td>{@code persistent://public/default/filter-requests}</td></tr>
 *   <tr><td>{@code PRIVATE_TOPIC_NAMESPACE}</td><td>{@code public/default}</td></tr>
 *   <tr><td>{@code ENTITY_TOPIC}</td><td>{@code persistent://public/default/orders}: the only topic the tool reads</td></tr>
 *   <tr><td>{@code MAX_LOOKBACK}</td><td>{@code 24h}</td></tr>
 *   <tr><td>{@code CALL_TIMEOUT}</td><td>{@code 60s}</td></tr>
 *   <tr><td>{@code MAX_WATCHES}</td><td>{@code 5}: open watches at once (each holds a filter)</td></tr>
 *   <tr><td>{@code WATCH_IDLE_TIMEOUT}</td><td>{@code 5m}: a watch with no call this long is closed</td></tr>
 *   <tr><td>{@code WATCH_MAX_WAIT}</td><td>{@code 60s}: the longest one watch call waits</td></tr>
 * </table>
 *
 * <p>stdout carries the MCP protocol, so everything else logs to stderr.
 */
public final class EntityEventsServer {

    private static final Logger LOG = LoggerFactory.getLogger(EntityEventsServer.class);

    private EntityEventsServer() {
    }

    public static void main(String[] args) throws Exception {
        String topic = env("ENTITY_TOPIC", "persistent://public/default/orders");
        EntityQuery.Limits limits = new EntityQuery.Limits(
                EntityQuery.parseDuration(env("MAX_LOOKBACK", "24h")),
                EntityQuery.parseDuration(env("CALL_TIMEOUT", "60s")));
        LazySession sessions = new LazySession();
        EntityEventsTool tool = new EntityEventsTool(sessions, topic, limits, Clock.systemUTC());
        EntityWatchTool watch = new EntityWatchTool(sessions, topic, limits, new EntityWatchTool.WatchLimits(
                Integer.parseInt(env("MAX_WATCHES", "5")),
                EntityQuery.parseDuration(env("WATCH_IDLE_TIMEOUT", "5m")),
                EntityQuery.parseDuration(env("WATCH_MAX_WAIT", "60s"))), Clock.systemUTC());

        McpSyncServer server = McpServer.sync(new StdioServerTransportProvider(new JacksonMcpJsonMapper(new ObjectMapper())))
                .serverInfo("entity-events", "1.0.0")
                .instructions("Use get_entity_events to see what happened to one entity (for example a customer or order ID) "
                        + "recently. Use watch_entity_events instead when things may keep changing while you help: "
                        + "it returns the history, then each call with its watch_id returns what happened since. "
                        + "Check the result's status before saying an entity had no events.")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
                .toolCall(tool.definition(), (exchange, request) -> tool.call(request.arguments()))
                .toolCall(watch.definition(), (exchange, request) -> watch.call(request.arguments()))
                .build();
        LOG.info("entity-events MCP server: topic {}, max lookback {}, timeout {}", topic,
                EntityQuery.format(limits.maxLookback()), EntityQuery.format(limits.timeout()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.closeGracefully();
            watch.close();
            sessions.close();
        }));
        new CountDownLatch(1).await(); // the stdio transport serves on its own threads until the process ends
    }

    /**
     * The filter SDK session, opened on the first call and reused after. Opening it lazily lets the agent
     * start this server before the broker is up; a failed open is retried on the next call.
     */
    static final class LazySession implements Supplier<Session> {
        private Session session;

        @Override
        public synchronized Session get() {
            if (session == null) {
                try {
                    session = Session.builder()
                            .serviceUrl(env("PULSAR_SERVICE_URL", "pulsar://localhost:16650"))
                            .adminUrl(env("PULSAR_ADMIN_URL", "http://localhost:18080"))
                            .controlTopic(env("FILTER_CONTROL_TOPIC", "persistent://public/default/filter-requests"))
                            .privateTopicTenant(env("PRIVATE_TOPIC_NAMESPACE", "public/default").split("/", 2)[0])
                            .privateTopicNamespace(env("PRIVATE_TOPIC_NAMESPACE", "public/default").split("/", 2)[1])
                            .build();
                } catch (Exception e) {
                    throw new IllegalStateException("couldn't connect to the broker: " + e.getMessage(), e);
                }
            }
            return session;
        }

        synchronized void close() {
            if (session != null) {
                session.close();
            }
        }
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }
}
