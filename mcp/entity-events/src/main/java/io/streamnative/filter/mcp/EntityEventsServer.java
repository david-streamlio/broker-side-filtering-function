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
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/**
 * An MCP server over stdio with five tools, in the shape of Confluent RTCE's {@code listTopics},
 * {@code getMetadata} and {@code queryData}: {@code list_topics} and {@code get_schema} (see
 * {@link TopicTools}) say which topics the agent may read and which fields a selector can name;
 * {@code check_selector} (see {@link SelectorCheckTool}) tries a selector on recent events without a scan;
 * {@code get_entity_events} (see {@link EntityEventsTool}) returns events over a recent window, and
 * {@code watch_entity_events} (see {@link EntityWatchTool}) the history and then new events as they happen.
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
 *   <tr><td>{@code ENTITY_TOPICS}</td><td>unset: comma-separated topics the tools may read (the allowlist); the first is the default</td></tr>
 *   <tr><td>{@code ENTITY_TOPIC}</td><td>{@code persistent://public/default/orders}: the one topic, when {@code ENTITY_TOPICS} is unset</td></tr>
 *   <tr><td>{@code SCHEMA_SAMPLE}</td><td>{@code 25}: recent messages read to learn a topic's headers (and value fields, without a registered schema)</td></tr>
 *   <tr><td>{@code SCHEMA_CACHE_TTL}</td><td>{@code 5m}: how long a topic's fields are cached</td></tr>
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
        Topics topics = Topics.fromConfig(System.getenv("ENTITY_TOPICS"),
                env("ENTITY_TOPIC", "persistent://public/default/orders"));
        EntityQuery.Limits limits = new EntityQuery.Limits(
                EntityQuery.parseDuration(env("MAX_LOOKBACK", "24h")),
                EntityQuery.parseDuration(env("CALL_TIMEOUT", "60s")));
        Clock clock = Clock.systemUTC();
        LazySession sessions = new LazySession();
        LazyAdmin admin = new LazyAdmin();
        int sampleSize = Integer.parseInt(env("SCHEMA_SAMPLE", "25"));
        Duration schemaTtl = EntityQuery.parseDuration(env("SCHEMA_CACHE_TTL", "5m"));
        FieldCatalog catalog = new FieldCatalog(BrokerInspector.fromEnv(admin), sampleSize, schemaTtl, clock);
        TopicTools topicTools = new TopicTools(topics, catalog);
        SelectorCheckTool check = new SelectorCheckTool(topics, catalog);
        EntityEventsTool tool = new EntityEventsTool(sessions, topics, catalog, limits, clock);
        EntityWatchTool watch = new EntityWatchTool(sessions, topics, catalog, limits, new EntityWatchTool.WatchLimits(
                Integer.parseInt(env("MAX_WATCHES", "5")),
                EntityQuery.parseDuration(env("WATCH_IDLE_TIMEOUT", "5m")),
                EntityQuery.parseDuration(env("WATCH_MAX_WAIT", "60s"))), clock);

        McpSyncServer server = McpServer.sync(new StdioServerTransportProvider(new JacksonMcpJsonMapper(new ObjectMapper())))
                .serverInfo("entity-events", "1.0.0")
                .instructions("Use get_entity_events to see what happened to one entity (for example a customer or order ID) "
                        + "recently. Use watch_entity_events instead when things may keep changing while you help: "
                        + "it returns the history, then each call with its watch_id returns what happened since. "
                        + "To filter on more than an entity's key, first call list_topics to see the topics you can "
                        + "read and get_schema to see the fields a selector on one can name, then pass a selector "
                        + "built from those fields: a SQL WHERE clause without the WHERE (the SQL-92 subset JMS "
                        + "selectors use), for example event_type = 'login' AND amount > 100. Or pass a whole SQL "
                        + "query as sql instead, RTCE-style: SELECT * FROM orders WHERE event_type = 'login' AND "
                        + "ts > now() - INTERVAL '15' MINUTE LIMIT 50; the server translates it to a selector, "
                        + "topic, lookback and max_events, and says what it ran. Check a selector with check_selector before a long "
                        + "lookback: it says whether the selector is valid and tries it on a few recent events "
                        + "without starting a scan. "
                        + "Check the result's status before saying an entity had no events.")
                .capabilities(McpSchema.ServerCapabilities.builder().tools(false).build())
                .toolCall(topicTools.listTopicsDefinition(), (exchange, request) -> topicTools.listTopics(request.arguments()))
                .toolCall(topicTools.getSchemaDefinition(), (exchange, request) -> topicTools.getSchema(request.arguments()))
                .toolCall(check.definition(), (exchange, request) -> check.call(request.arguments()))
                .toolCall(tool.definition(), (exchange, request) -> tool.call(request.arguments()))
                .toolCall(watch.definition(), (exchange, request) -> watch.call(request.arguments()))
                .build();
        LOG.info("entity-events MCP server: topics {}, max lookback {}, timeout {}, schema sample {}, schema cache {}",
                topics.all(), EntityQuery.format(limits.maxLookback()), EntityQuery.format(limits.timeout()),
                sampleSize, EntityQuery.format(schemaTtl));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.closeGracefully();
            watch.close();
            sessions.close();
            admin.close();
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

    /**
     * The Pulsar admin client {@code get_schema} reads topics through, created on first use like
     * {@link LazySession}; a failed create is retried on the next call.
     */
    static final class LazyAdmin implements Supplier<PulsarAdmin> {
        private PulsarAdmin admin;

        @Override
        public synchronized PulsarAdmin get() {
            if (admin == null) {
                try {
                    admin = PulsarAdmin.builder().serviceHttpUrl(env("PULSAR_ADMIN_URL", "http://localhost:18080")).build();
                } catch (Exception e) {
                    throw new IllegalStateException("couldn't connect to the broker's admin API: " + e.getMessage(), e);
                }
            }
            return admin;
        }

        synchronized void close() {
            if (admin != null) {
                admin.close();
            }
        }
    }

    private static String env(String name, String def) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? def : v;
    }
}
