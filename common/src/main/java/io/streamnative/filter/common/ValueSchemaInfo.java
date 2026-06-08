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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

/**
 * Pulsar-free wire description of a consumer's value schema, carried end-to-end from the SDK to the
 * filter function so the function can (in a follow-up) decode the message value for schema-aware
 * value filtering. Mirrors the relevant fields of {@code org.apache.pulsar.common.schema.SchemaInfo}
 * without depending on any Pulsar type — the SDK maps a {@code SchemaInfo} into this DTO at
 * subscribe time, and every module downstream treats it as opaque data.
 *
 * <p>The {@link #definition} is the base64 encoding of {@code SchemaInfo.getSchema()} — the Avro
 * {@code .avsc} text for {@code AVRO}, the JSON-schema text for {@code JSON}, the serialized
 * descriptor for {@code PROTOBUF_NATIVE}, and empty for {@code STRING}/{@code BYTES}. A schema
 * <em>name</em> alone (e.g. {@code "AVRO"}) is insufficient to decode a structured value, which is
 * why the full definition travels on the wire.
 *
 * <p>The class is immutable and JSON-serializable via Jackson. Unknown fields are tolerated for
 * forward-compat, and {@code null} fields are omitted on serialization so a record without a value
 * schema is wire-identical to the pre-schema format.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ValueSchemaInfo {

    private final String name;
    private final String type;
    private final String definition;
    private final Map<String, String> properties;

    @JsonCreator
    public ValueSchemaInfo(@JsonProperty("name") String name,
                           @JsonProperty("type") String type,
                           @JsonProperty("definition") String definition,
                           @JsonProperty("properties") Map<String, String> properties) {
        this.name       = name;
        this.type       = type;
        this.definition = definition;
        this.properties = properties == null ? null : Map.copyOf(properties);
    }

    /** Schema name — {@code SchemaInfo.getName()} (e.g. the POJO class name, {@code "STRING"}). */
    public String getName() {
        return name;
    }

    /** Schema type — {@code SchemaType.name()}: {@code JSON|AVRO|PROTOBUF_NATIVE|STRING|BYTES|...}. */
    public String getType() {
        return type;
    }

    /** Base64 of {@code SchemaInfo.getSchema()}; {@code null}/empty for {@code STRING}/{@code BYTES}. */
    public String getDefinition() {
        return definition;
    }

    /** Schema properties — {@code SchemaInfo.getProperties()}; {@code null} when none. */
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ValueSchemaInfo that)) return false;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(definition, that.definition)
                && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, definition, properties);
    }

    @Override
    public String toString() {
        // Avoid dumping the (potentially large) base64 definition; show only its length.
        return "ValueSchemaInfo{name='" + name + "', type='" + type + '\''
                + ", definitionLen=" + (definition == null ? 0 : definition.length())
                + ", properties=" + properties + '}';
    }
}
