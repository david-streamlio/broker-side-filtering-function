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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The discovery tools, in the shape of Confluent RTCE's {@code listTopics} and {@code getMetadata}:
 * {@code list_topics} names the topics this server may read (its allowlist, see {@link Topics}), and
 * {@code get_schema} lists the fields a selector on one of them can name (see {@link FieldCatalog}), so an
 * agent can write a JMS selector for {@code get_entity_events} or {@code watch_entity_events}.
 */
final class TopicTools {

    static final String LIST_TOPICS = "list_topics";
    static final String GET_SCHEMA = "get_schema";

    private static final Logger LOG = LoggerFactory.getLogger(TopicTools.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    /** A name a selector can use bare: a JMS identifier, possibly dotted. */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_$][A-Za-z0-9_$.]*");

    static final String HOW_TO_FILTER = "Pass a JMS selector as the selector argument of get_entity_events or "
            + "watch_entity_events. It uses SQL-92 WHERE syntax: = <> < > <= >= IN LIKE BETWEEN IS NULL AND OR NOT, "
            + "with strings in single quotes. Use only the field names listed here. Prefer key and header fields: "
            + "they are read without decoding the message value. The time window is the lookback argument, not a "
            + "selector term. Before a long lookback, try the selector with check_selector. To write a whole SQL query "
            + "instead (SELECT * FROM <topic> WHERE … LIMIT n), pass it as sql.";

    private final Topics topics;
    private final FieldCatalog catalog;

    TopicTools(Topics topics, FieldCatalog catalog) {
        this.topics = topics;
        this.catalog = catalog;
    }

    /** {@code list_topics} as the agent sees it. */
    McpSchema.Tool listTopicsDefinition() {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of());
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(LIST_TOPICS)
                .title("List topics")
                .description("Lists the topics this server can read, each by its short name and full name. Pass either "
                        + "as topic to get_schema, get_entity_events or watch_entity_events.")
                .inputSchema(schema)
                .build();
    }

    McpSchema.CallToolResult listTopics(Map<String, Object> args) {
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("topics", topics.describe());
        r.put("next", "call get_schema with a topic to see the fields you can filter on");
        return ok(r);
    }

    /** {@code get_schema} as the agent sees it. */
    McpSchema.Tool getSchemaDefinition() {
        Map<String, Object> topic = Map.of("type", "string",
                "description", "The topic, by its short or full name from list_topics. Default: "
                        + Topics.shortName(topics.first()) + ".");
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", Map.of("topic", topic));
        schema.put("additionalProperties", false);
        return McpSchema.Tool.builder()
                .name(GET_SCHEMA)
                .title("Topic schema")
                .description("Lists the fields a selector on a topic can name: the event key, message headers (with "
                        + "example values) and fields of the message value (dotted paths for nested ones), with what "
                        + "each costs to filter on. Call it before passing a selector to get_entity_events or "
                        + "watch_entity_events; a selector may use only these names.")
                .inputSchema(schema)
                .build();
    }

    McpSchema.CallToolResult getSchema(Map<String, Object> args) {
        String topic;
        FieldCatalog.Fields fields;
        try {
            topic = topics.resolve(args == null ? null : args.get("topic"));
            fields = catalog.fields(topic);
        } catch (IllegalArgumentException | IllegalStateException e) {
            return error(e.getMessage());
        }
        List<Map<String, Object>> described = new ArrayList<>();
        fields.fields().forEach(f -> described.add(f.describe()));
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("topic", topic);
        r.put("sampled", fields.sampled());
        r.put("body_fields_from", fields.bodyFieldsFrom());
        r.put("fields", described);
        r.put("how_to_filter", HOW_TO_FILTER);
        r.put("example", example(fields.fields()));
        LOG.info("{} topic={}: {} fields from {} sampled", GET_SCHEMA, topic, described.size(), fields.sampled());
        return ok(r);
    }

    /** A selector built from the topic's own fields: the key, and the first header with an example value. */
    static String example(List<FieldCatalog.Field> fields) {
        String key = "__key__ = '<id>'";
        for (FieldCatalog.Field f : fields) {
            if ("header".equals(f.source()) && !f.examples().isEmpty() && IDENTIFIER.matcher(f.name()).matches()) {
                return key + " AND " + f.name() + " = '" + f.examples().get(0).replace("'", "''") + "'";
            }
        }
        return key;
    }

    private static McpSchema.CallToolResult ok(Map<String, Object> result) {
        try {
            return McpSchema.CallToolResult.builder()
                    .addTextContent(JSON.writeValueAsString(result))
                    .structuredContent(result)
                    .build();
        } catch (JsonProcessingException e) {
            return error("couldn't write the result: " + e.getMessage());
        }
    }

    private static McpSchema.CallToolResult error(String message) {
        return McpSchema.CallToolResult.builder().addTextContent(message).isError(true).build();
    }
}
