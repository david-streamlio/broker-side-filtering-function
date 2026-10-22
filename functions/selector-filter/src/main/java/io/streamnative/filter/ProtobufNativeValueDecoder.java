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

package io.streamnative.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Decodes Pulsar {@code Schema.PROTOBUF_NATIVE} message values <em>without</em> the generated
 * protobuf class. Pulsar carries the message descriptor in the schema itself — the SDK forwards
 * {@code SchemaInfo.getSchema()} (a {@code ProtobufNativeSchemaData} JSON: a serialized
 * {@code FileDescriptorSet} plus the root message/file names) — so this decoder reconstructs the
 * {@link Descriptor} graph once and reads each message with {@link DynamicMessage}.
 *
 * <p>The reconstruction is done with the bundled unshaded {@code com.google.protobuf} classes
 * rather than Pulsar's internal {@code ProtobufNativeSchemaUtils}, keeping the decoder self-contained
 * in the NAR's isolated classloader. Only top-level scalar fields are surfaced; repeated/map/message
 * and {@code bytes} fields resolve to {@code null} (the JMS grammar addresses only flat identifiers).
 */
final class ProtobufNativeValueDecoder implements ValueDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufNativeValueDecoder.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Descriptor descriptor;

    /**
     * @param schemaData the raw {@code SchemaInfo.getSchema()} bytes for a PROTOBUF_NATIVE schema
     *                   (the {@code ProtobufNativeSchemaData} JSON)
     * @throws IllegalArgumentException if the descriptor can't be parsed/reconstructed
     */
    ProtobufNativeValueDecoder(byte[] schemaData) {
        this.descriptor = buildDescriptor(schemaData);
    }

    @Override
    public Map<String, Object> decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            return null;
        }
        try {
            return messageToMap(DynamicMessage.parseFrom(descriptor, payload), descriptor);
        } catch (Exception e) {
            LOG.debug("Protobuf decode failed for {}: {}", descriptor.getFullName(), e.toString());
            return null;
        }
    }

    /** Convert a message to a field map, recursing into nested messages (for dotted-path lookups). */
    private static Map<String, Object> messageToMap(Message message, Descriptor desc) {
        Map<String, Object> fields = new HashMap<>();
        for (FieldDescriptor field : desc.getFields()) {
            fields.put(field.getName(), coerce(field, message));
        }
        return fields;
    }

    // ------------------------------------------------------------------ descriptor reconstruction

    static Descriptor buildDescriptor(byte[] schemaData) {
        if (schemaData == null || schemaData.length == 0) {
            throw new IllegalArgumentException("Empty PROTOBUF_NATIVE schema data");
        }
        JsonNode root;
        try {
            root = MAPPER.readTree(schemaData);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid ProtobufNativeSchemaData JSON: " + e.getMessage(), e);
        }
        JsonNode fdsNode = root.get("fileDescriptorSet");
        String rootMessage = text(root, "rootMessageTypeName");
        String rootFile = text(root, "rootFileDescriptorName");
        if (fdsNode == null || rootMessage == null || rootFile == null) {
            throw new IllegalArgumentException(
                    "ProtobufNativeSchemaData missing fileDescriptorSet/rootMessageTypeName/rootFileDescriptorName");
        }

        byte[] fdsBytes;
        FileDescriptorSet set;
        try {
            fdsBytes = fdsNode.binaryValue();        // base64 text node → bytes
            set = FileDescriptorSet.parseFrom(fdsBytes);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid fileDescriptorSet: " + e.getMessage(), e);
        }

        Map<String, FileDescriptorProto> protos = new HashMap<>();
        for (FileDescriptorProto fdp : set.getFileList()) {
            protos.put(fdp.getName(), fdp);
        }

        Map<String, FileDescriptor> built = new HashMap<>();
        buildFile(rootFile, protos, built, new HashSet<>());

        Descriptor message = findMessage(built.get(rootFile), rootMessage);
        if (message == null) {
            // rootMessageTypeName may be fully-qualified and live in a different built file.
            for (FileDescriptor fd : built.values()) {
                message = findMessage(fd, rootMessage);
                if (message != null) {
                    break;
                }
            }
        }
        if (message == null) {
            throw new IllegalArgumentException("Root message type not found: " + rootMessage);
        }
        return message;
    }

    /** Build a {@link FileDescriptor} and its dependencies recursively, in dependency order. */
    private static FileDescriptor buildFile(String name,
                                            Map<String, FileDescriptorProto> protos,
                                            Map<String, FileDescriptor> built,
                                            Set<String> inProgress) {
        FileDescriptor existing = built.get(name);
        if (existing != null) {
            return existing;
        }
        FileDescriptorProto proto = protos.get(name);
        if (proto == null) {
            throw new IllegalArgumentException("FileDescriptorSet is missing dependency: " + name);
        }
        if (!inProgress.add(name)) {
            throw new IllegalArgumentException("Cyclic protobuf file dependency at: " + name);
        }
        FileDescriptor[] deps = new FileDescriptor[proto.getDependencyCount()];
        for (int i = 0; i < proto.getDependencyCount(); i++) {
            deps[i] = buildFile(proto.getDependency(i), protos, built, inProgress);
        }
        FileDescriptor fd;
        try {
            fd = FileDescriptor.buildFrom(proto, deps);
        } catch (Descriptors.DescriptorValidationException e) {
            throw new IllegalArgumentException("Failed to build descriptor for " + name + ": " + e.getMessage(), e);
        }
        inProgress.remove(name);
        built.put(name, fd);
        return fd;
    }

    private static Descriptor findMessage(FileDescriptor file, String name) {
        if (file == null) {
            return null;
        }
        Descriptor byName = file.findMessageTypeByName(simpleName(name));
        if (byName != null && (byName.getFullName().equals(name) || byName.getName().equals(name))) {
            return byName;
        }
        for (Descriptor m : file.getMessageTypes()) {
            if (m.getFullName().equals(name) || m.getName().equals(name)) {
                return m;
            }
        }
        return byName; // simple-name fallback
    }

    // ------------------------------------------------------------------ field coercion

    /**
     * Coerce a top-level protobuf field to a JMS-comparable scalar. Repeated/map/message and bytes
     * fields resolve to {@code null}. For fields that track presence (message types, proto2, and
     * {@code optional} proto3) an unset field is {@code null}; proto3 singular scalars have no
     * presence, so an unset value decodes to its type default (0 / "" / false) — document this.
     */
    private static Object coerce(FieldDescriptor field, Message message) {
        if (field.isRepeated() || field.isMapField()) {
            return null;
        }
        if (field.hasPresence() && !message.hasField(field)) {
            return null;
        }
        Object value = message.getField(field);
        switch (field.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return value;                                   // already Integer/Long/Float/Double/Boolean
            case STRING:
                return value.toString();
            case ENUM:
                return ((EnumValueDescriptor) value).getName();
            case MESSAGE:
                return messageToMap((Message) value, field.getMessageType());   // nested
            case BYTE_STRING:
            default:
                return null;                                    // not addressable as a scalar
        }
    }

    private static String text(JsonNode node, String field) {
        JsonNode n = node.get(field);
        return (n == null || n.isNull()) ? null : n.asText();
    }

    private static String simpleName(String name) {
        int dot = name.lastIndexOf('.');
        return dot < 0 ? name : name.substring(dot + 1);
    }
}
