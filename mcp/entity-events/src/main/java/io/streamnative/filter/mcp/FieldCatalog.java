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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.selector.Selector;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * What a selector can filter on, per topic: the {@code get_schema} answer, and the allowlist a selector's
 * identifiers are checked against before any filter starts.
 *
 * <p>The fields follow the filter's own resolution order ({@code MessageBindings.lookup}): the built-in
 * {@code __key__} and {@code __publish_time__}, then message properties (Kafka headers, via KoP), then
 * fields of the message value. A property shadows a value field of the same name, so such a name is
 * listed once, as a header. Value fields come from the topic's registered schema when it has an Avro or
 * JSON one, else from parsing recent values as JSON; nested fields use dotted paths. Headers carry no
 * schema, so they are learned from recent messages, with a few example values for each.
 *
 * <p>Results are cached per topic for {@code ttl}: the catalog is read on {@code get_schema} and on calls
 * that pass a selector, not on every event.
 */
final class FieldCatalog {

    /**
     * One recent message, as the catalog needs it. {@code publishTime} (epoch ms on the log's clock) is
     * {@code null} when the inspector doesn't supply it; {@code check_selector} then can't try a
     * {@code __publish_time__} term on the sample.
     */
    record Sample(String key, Map<String, String> properties, byte[] value, Long publishTime) {

        Sample(String key, Map<String, String> properties, byte[] value) {
            this(key, properties, value, null);
        }
    }

    /** A topic's registered value schema: its type name ({@code AVRO}, {@code JSON}, …) and definition. */
    record RegisteredSchema(String type, String definition) {
    }

    /** Where the catalog reads from: the broker in production, a stub in tests. */
    interface Inspector {
        /** Up to {@code n} of the topic's most recent messages, newest first; fewer if it holds fewer. */
        List<Sample> recent(String topic, int n) throws Exception;

        Optional<RegisteredSchema> schema(String topic) throws Exception;
    }

    /** A field a selector can name. {@code source} is {@code key}, {@code metadata}, {@code header} or {@code body}. */
    record Field(String name, String source, String type, List<String> examples) {
        String cost() {
            return "body".equals(source) ? "decodes the message value once per event"
                    : "read from message metadata, without decoding the value";
        }

        Map<String, Object> describe() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", name);
            m.put("source", source);
            m.put("type", type);
            m.put("cost", cost());
            if (!examples.isEmpty()) {
                m.put("examples", examples);
            }
            return m;
        }
    }

    /**
     * A topic's fields, where its value fields came from, and the recent messages they were learned from.
     * The samples are kept so {@code check_selector} can try a selector on them without reading the topic again.
     */
    record Fields(String topic, List<Field> fields, int sampled, String bodyFieldsFrom, List<Sample> samples) {

        /** Fields without the samples they came from. */
        Fields(String topic, List<Field> fields, int sampled, String bodyFieldsFrom) {
            this(topic, fields, sampled, bodyFieldsFrom, List.of());
        }

        /**
         * Fields known only by name (a caller that has no catalog): the built-ins keep their source, every
         * other name's source and type are {@code unknown}, so nothing is inferred about them.
         */
        static Fields ofNames(String topic, Set<String> names) {
            List<Field> fields = new ArrayList<>();
            for (String name : names) {
                fields.add(BUILT_INS.stream().filter(b -> b.name().equals(name)).findFirst()
                        .orElse(new Field(name, "unknown", "unknown", List.of())));
            }
            return new Fields(topic, List.copyOf(fields), 0, "unknown");
        }

        Set<String> names() {
            Set<String> names = new LinkedHashSet<>();
            fields.forEach(f -> names.add(f.name()));
            return names;
        }

        Optional<Field> field(String name) {
            return fields.stream().filter(f -> f.name().equals(name)).findFirst();
        }
    }

    static final List<Field> BUILT_INS = List.of(
            new Field("__key__", "key", "string", List.of()),
            new Field("__publish_time__", "metadata", "number (epoch ms, when the broker received the event)", List.of()));

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_EXAMPLES = 5;
    private static final int MAX_EXAMPLE_LENGTH = 40;
    private static final int MAX_DEPTH = 3;

    private final Inspector inspector;
    private final int sampleSize;
    private final Duration ttl;
    private final Clock clock;
    private final Map<String, Cached> cache = new ConcurrentHashMap<>();

    private record Cached(Fields fields, long at) {
    }

    FieldCatalog(Inspector inspector, int sampleSize, Duration ttl, Clock clock) {
        this.inspector = inspector;
        this.sampleSize = sampleSize;
        this.ttl = ttl;
        this.clock = clock;
    }

    /**
     * The topic's fields, from the cache when fresh.
     *
     * @throws IllegalStateException if the broker can't be read
     */
    Fields fields(String topic) {
        long now = clock.millis();
        Cached c = cache.get(topic);
        if (c != null && now - c.at() < ttl.toMillis()) {
            return c.fields();
        }
        Fields f;
        try {
            f = build(topic, inspector.schema(topic), inspector.recent(topic, sampleSize));
        } catch (Exception e) {
            throw new IllegalStateException("couldn't read the fields of " + topic + ": " + e.getMessage(), e);
        }
        cache.put(topic, new Cached(f, now));
        return f;
    }

    /** The recent messages the topic's fields were learned from, newest first (from the cache when fresh). */
    List<Sample> samples(String topic) {
        return fields(topic).samples();
    }

    /** The names a selector on {@code topic} may use. */
    Set<String> names(String topic) {
        return fields(topic).names();
    }

    static Fields build(String topic, Optional<RegisteredSchema> schema, List<Sample> samples) {
        List<Field> fields = new ArrayList<>(BUILT_INS);

        // Headers, in the order first seen; the filter's own (__…) properties are internal.
        Map<String, LinkedHashSet<String>> headers = new LinkedHashMap<>();
        for (Sample s : samples) {
            s.properties().forEach((k, v) -> {
                if (!k.startsWith("__")) {
                    addExample(headers.computeIfAbsent(k, x -> new LinkedHashSet<>()), v);
                }
            });
        }
        // A header whose name the selector grammar can't express (x-trace, a keyword like "order") can't be
        // filtered on, so it isn't offered.
        headers.forEach((name, examples) -> {
            if (nameable(name)) {
                fields.add(new Field(name, "header", "string", List.copyOf(examples)));
            }
        });

        // Value fields: the registered schema when it describes a record, else the sampled values.
        Map<String, String> types = new LinkedHashMap<>();
        Map<String, LinkedHashSet<String>> examples = new LinkedHashMap<>();
        boolean framed = false;
        for (Sample s : samples) {
            framed |= s.value() != null && unframed(s.value()) > 0;
            JsonNode value = parse(s.value());
            if (value != null && value.isObject()) {
                collect(value, "", 0, types, examples);
            }
        }
        String from = "none";
        Map<String, String> declared = schema.map(FieldCatalog::schemaFields).orElse(Map.of());
        if (!declared.isEmpty()) {
            types = new LinkedHashMap<>(declared);
            from = "registered " + schema.get().type() + " schema";
        } else if (!types.isEmpty()) {
            from = "sampled JSON values";
        }
        if (framed) {
            // A Kafka serializer's wire-format prefix: the filter reads the raw value as JSON and doesn't strip it
            // (MessageBindings), so a selector on these body fields would never match. Offer key and headers only.
            return new Fields(topic, List.copyOf(fields), samples.size(), "none: values are in Kafka serializer wire "
                    + "format (" + from + "), which the filter can't read yet; filter on the key and headers",
                    List.copyOf(samples));
        }
        for (Map.Entry<String, String> e : types.entrySet()) {
            if (!nameable(e.getKey())) {
                continue;
            }
            if (!headers.containsKey(e.getKey())) { // a property of the same name shadows the value field
                LinkedHashSet<String> ex = examples.getOrDefault(e.getKey(), new LinkedHashSet<>());
                fields.add(new Field(e.getKey(), "body", e.getValue(), List.copyOf(ex)));
            }
        }
        return new Fields(topic, List.copyOf(fields), samples.size(), from, List.copyOf(samples));
    }

    /** Scalar fields of a JSON object, by dotted path; arrays can't be named in a selector, so they're left out. */
    private static void collect(JsonNode node, String prefix, int depth, Map<String, String> types,
                                Map<String, LinkedHashSet<String>> examples) {
        Iterator<Map.Entry<String, JsonNode>> it = node.fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            if (e.getKey().startsWith("_")) {
                continue; // padding and private fields
            }
            String path = prefix + e.getKey();
            JsonNode v = e.getValue();
            if (v.isObject()) {
                if (depth + 1 < MAX_DEPTH) {
                    collect(v, path + ".", depth + 1, types, examples);
                }
            } else if (v.isTextual() || v.isNumber() || v.isBoolean()) {
                types.putIfAbsent(path, v.isTextual() ? "string" : v.isNumber() ? "number" : "boolean");
                addExample(examples.computeIfAbsent(path, x -> new LinkedHashSet<>()), v.asText());
            } else if (v.isNull()) {
                types.putIfAbsent(path, "string");
            }
        }
    }

    /**
     * The scalar fields a schema definition declares: an Avro-style record (Pulsar's AVRO and JSON schemas,
     * a Kafka Schema Registry's AVRO schema) or a JSON Schema object with {@code properties} (a Kafka Schema
     * Registry's JSON schema).
     */
    static Map<String, String> schemaFields(RegisteredSchema schema) {
        Map<String, String> out = new LinkedHashMap<>();
        try {
            JsonNode root = JSON.readTree(schema.definition());
            if (root != null && "record".equals(root.path("type").asText())) {
                record(root, "", 0, out);
            } else if (root != null && root.path("properties").isObject()) {
                properties(root, "", 0, out);
            }
        } catch (Exception e) {
            return Map.of();
        }
        return out;
    }

    /** A JSON Schema object's scalar properties; {@code type} may be a list such as {@code ["string", "null"]}. */
    private static void properties(JsonNode obj, String prefix, int depth, Map<String, String> out) {
        Iterator<Map.Entry<String, JsonNode>> it = obj.path("properties").fields();
        while (it.hasNext()) {
            Map.Entry<String, JsonNode> e = it.next();
            String path = prefix + e.getKey();
            JsonNode prop = e.getValue();
            String t = nonNull(prop.path("type")).asText();
            if ("object".equals(t) || (t.isEmpty() && prop.path("properties").isObject())) {
                if (depth + 1 < MAX_DEPTH) {
                    properties(prop, path + ".", depth + 1, out);
                }
                continue;
            }
            switch (t) {
                case "string" -> out.put(path, "string");
                case "integer", "number" -> out.put(path, "number");
                case "boolean" -> out.put(path, "boolean");
                default -> {
                    if (prop.has("enum")) {
                        out.put(path, "string");
                    } // arrays, $refs, untyped: not nameable, or not knowable here
                }
            }
        }
    }

    private static void record(JsonNode rec, String prefix, int depth, Map<String, String> out) {
        for (JsonNode f : rec.path("fields")) {
            String path = prefix + f.path("name").asText();
            JsonNode type = nonNull(f.path("type"));
            if (type.isObject() && "record".equals(type.path("type").asText())) {
                if (depth + 1 < MAX_DEPTH) {
                    record(type, path + ".", depth + 1, out);
                }
            } else {
                String t = type.isObject() ? type.path("type").asText() : type.asText();
                switch (t) {
                    case "string", "enum" -> out.put(path, "string");
                    case "int", "long", "float", "double" -> out.put(path, "number");
                    case "boolean" -> out.put(path, "boolean");
                    default -> { } // arrays, maps, bytes: not nameable in a selector
                }
            }
        }
    }

    /** {@code ["null", X]} → {@code X}. */
    private static JsonNode nonNull(JsonNode type) {
        if (type.isArray()) {
            for (JsonNode t : type) {
                if (!"null".equals(t.asText())) {
                    return t;
                }
            }
        }
        return type;
    }

    /**
     * Whether a selector can name {@code name}: it parses, by itself, as exactly that identifier. Checked with
     * the filter's own parser rather than a copy of its rules, so keywords, dotted paths and odd characters are
     * judged the same way the filter will judge them.
     */
    static boolean nameable(String name) {
        try {
            return Set.of(name).equals(Selector.compile(name + " IS NULL").referencedNames());
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void addExample(Set<String> examples, String value) {
        if (value != null && !value.isEmpty() && value.length() <= MAX_EXAMPLE_LENGTH && examples.size() < MAX_EXAMPLES) {
            examples.add(value);
        }
    }

    private static JsonNode parse(byte[] value) {
        if (value == null) {
            return null;
        }
        try {
            int from = unframed(value);
            return JSON.readTree(new String(value, from, value.length - from, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }

    /** Confluent wire format: magic byte {@code 0x00} and a 4-byte schema id before the payload. */
    static final int WIRE_FORMAT_PREFIX = 5;

    /**
     * Where a value's payload starts: past the Confluent wire-format prefix when a Kafka serializer wrote one
     * (no JSON text starts with {@code 0x00}), else at 0.
     */
    static int unframed(byte[] value) {
        return value.length > WIRE_FORMAT_PREFIX && value[0] == 0 ? WIRE_FORMAT_PREFIX : 0;
    }
}
