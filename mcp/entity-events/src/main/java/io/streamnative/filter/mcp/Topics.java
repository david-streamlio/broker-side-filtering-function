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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The topics this server may read: an allowlist set by the operator ({@code ENTITY_TOPICS}, falling back
 * to {@code ENTITY_TOPIC}). The agent can name a topic by its full name or by its last path segment
 * ({@code session_events}); anything else is rejected, so agent text never picks an arbitrary topic.
 */
final class Topics {

    private final List<String> topics;

    private Topics(List<String> topics) {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("at least one topic is required");
        }
        this.topics = List.copyOf(topics);
    }

    static Topics of(String... topics) {
        return new Topics(Arrays.asList(topics));
    }

    /** {@code list} is comma-separated; blank means {@code single} alone. */
    static Topics fromConfig(String list, String single) {
        List<String> out = new ArrayList<>();
        if (list != null) {
            for (String t : list.split(",")) {
                if (!t.isBlank() && !out.contains(t.trim())) {
                    out.add(t.trim());
                }
            }
        }
        if (out.isEmpty()) {
            out.add(single);
        }
        return new Topics(out);
    }

    List<String> all() {
        return topics;
    }

    String first() {
        return topics.get(0);
    }

    /** The last path segment: {@code persistent://public/default/session_events} → {@code session_events}. */
    static String shortName(String topic) {
        int slash = topic.lastIndexOf('/');
        return slash < 0 ? topic : topic.substring(slash + 1);
    }

    /**
     * The topic a call names, or the first topic when it names none.
     *
     * @throws IllegalArgumentException with a message the agent can act on
     */
    String resolve(Object raw) {
        if (raw == null || String.valueOf(raw).isBlank()) {
            return first();
        }
        String asked = String.valueOf(raw).trim();
        for (String t : topics) {
            if (t.equals(asked) || shortName(t).equals(asked)) {
                return t;
            }
        }
        throw new IllegalArgumentException("topic \"" + asked + "\" isn't one this server reads; call list_topics for the "
                + "topics you can use (" + String.join(", ", topics.stream().map(Topics::shortName).toList()) + ")");
    }

    /** What {@code list_topics} returns: each topic's short and full name. */
    List<Map<String, Object>> describe() {
        List<Map<String, Object>> out = new ArrayList<>();
        for (String t : topics) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", shortName(t));
            m.put("topic", t);
            out.add(m);
        }
        return out;
    }
}
