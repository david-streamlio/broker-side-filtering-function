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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * What {@link SqlTranslator} made of an agent's SQL: the selector its {@code WHERE} becomes, and the tool
 * arguments the statement around it sets. Every part is {@code null} when the SQL didn't say it.
 *
 * @param sql       the SQL as the agent wrote it
 * @param topic     the full topic name {@code FROM} named, checked against the allowlist
 * @param selector  the JMS selector, or {@code null} when the {@code WHERE} held only the time window (or nothing)
 * @param lookback  from a top-level {@code <time> > now() - INTERVAL …} term
 * @param maxEvents from {@code LIMIT n}
 * @param columns   the fields {@code SELECT} names (each event is trimmed to them, plus time and key), or
 *                  {@code null} for {@code SELECT *}, a bare condition, or no column list
 * @param notes     plain sentences: what each part of the SQL became
 */
record SqlTranslation(String sql, String topic, String selector, Duration lookback, Integer maxEvents,
                      List<String> columns, List<String> notes) {

    SqlTranslation {
        columns = columns == null ? null : List.copyOf(columns);
        notes = notes == null ? List.of() : List.copyOf(notes);
    }

    /** What the result echoes: the generated selector and each argument the SQL set. */
    Map<String, Object> describe() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("selector", selector);
        if (topic != null) {
            m.put("topic", topic);
        }
        if (lookback != null) {
            m.put("lookback", EntityQuery.format(lookback));
        }
        if (maxEvents != null) {
            m.put("max_events", maxEvents);
        }
        if (columns != null) {
            m.put("columns", columns);
        }
        m.put("notes", notes);
        return m;
    }

    /**
     * An event trimmed to {@link #columns}: {@code time} and {@code key} always, then the named properties,
     * and the named value fields (dotted paths) in the value's own structure. Unchanged without a column list.
     */
    @SuppressWarnings("unchecked")
    Map<String, Object> project(Map<String, Object> event) {
        if (columns == null) {
            return event;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("time", event.get("time"));
        out.put("key", event.get("key"));
        Map<String, String> properties = new TreeMap<>();
        Object props = event.get("properties");
        if (props instanceof Map<?, ?> p) {
            for (String c : columns) {
                Object v = p.get(c);
                if (v != null) {
                    properties.put(c, String.valueOf(v));
                }
            }
        }
        out.put("properties", properties);
        Object value = event.get("value");
        if (value instanceof JsonNode node && node.isObject()) {
            ObjectNode kept = JsonNodeFactory.instance.objectNode();
            for (String c : columns) {
                copyPath(node, kept, c.split("\\."), 0);
            }
            out.put("value", kept);
        } else {
            out.put("value", null); // text or no value: it has no fields to keep
        }
        return out;
    }

    private static void copyPath(JsonNode from, ObjectNode to, String[] path, int i) {
        JsonNode child = from.get(path[i]);
        if (child == null) {
            return;
        }
        if (i == path.length - 1) {
            to.set(path[i], child.deepCopy());
            return;
        }
        if (!child.isObject()) {
            return;
        }
        JsonNode existing = to.get(path[i]);
        ObjectNode next = existing instanceof ObjectNode o ? o : to.putObject(path[i]);
        copyPath(child, next, path, i + 1);
    }
}
