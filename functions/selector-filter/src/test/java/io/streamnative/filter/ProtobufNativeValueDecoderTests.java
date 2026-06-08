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

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates {@link ProtobufNativeValueDecoder} against the exact schema bytes Pulsar produces:
 * a descriptor is built programmatically (no protoc/generated class), serialized with Pulsar's own
 * {@code ProtobufNativeSchemaUtils.serialize} (the same call the broker makes), and decoded back —
 * cross-checking our descriptor reconstruction against Pulsar's serializer.
 */
class ProtobufNativeValueDecoderTests {

    /** proto3: {@code Trade { string sym=1; double price=2; int32 qty=3; bool active=4; }}. */
    static Descriptor tradeDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("trade.proto")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Trade")
                        .addField(field("sym", 1, Type.TYPE_STRING))
                        .addField(field("price", 2, Type.TYPE_DOUBLE))
                        .addField(field("qty", 3, Type.TYPE_INT32))
                        .addField(field("active", 4, Type.TYPE_BOOL)))
                .build();
        FileDescriptor fd = FileDescriptor.buildFrom(file, new FileDescriptor[0]);
        return fd.findMessageTypeByName("Trade");
    }

    private static FieldDescriptorProto field(String name, int number, Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(Label.LABEL_OPTIONAL).build();
    }

    @Test
    void decodesPulsarProtobufNativeEncodedMessage() throws Exception {
        Descriptor trade = tradeDescriptor();
        byte[] schemaData = ProtobufNativeSchemaUtils.serialize(trade);   // Pulsar's real format

        DynamicMessage msg = DynamicMessage.newBuilder(trade)
                .setField(trade.findFieldByName("sym"), "AAPL")
                .setField(trade.findFieldByName("price"), 195.5)
                .setField(trade.findFieldByName("qty"), 10)
                .setField(trade.findFieldByName("active"), true)
                .build();

        Map<String, Object> fields = new ProtobufNativeValueDecoder(schemaData).decode(msg.toByteArray());

        assertThat(fields).isNotNull();
        assertThat(fields.get("sym")).isEqualTo("AAPL");
        assertThat(((Number) fields.get("price")).doubleValue()).isEqualTo(195.5);
        assertThat(((Number) fields.get("qty")).intValue()).isEqualTo(10);
        assertThat(fields.get("active")).isEqualTo(true);
    }

    /** proto3 with a nested message: {@code Order { string sym=1; Address address=2; }}. */
    static Descriptor orderDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("order.proto")
                .setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Address")
                        .addField(field("city", 1, Type.TYPE_STRING))
                        .addField(field("zip", 2, Type.TYPE_INT32)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Order")
                        .addField(field("sym", 1, Type.TYPE_STRING))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("address").setNumber(2)
                                .setType(Type.TYPE_MESSAGE).setTypeName(".Address")
                                .setLabel(Label.LABEL_OPTIONAL)))
                .build();
        FileDescriptor fd = FileDescriptor.buildFrom(file, new FileDescriptor[0]);
        return fd.findMessageTypeByName("Order");
    }

    @Test
    void decodesNestedMessageToNestedMap() throws Exception {
        Descriptor order = orderDescriptor();
        Descriptor address = order.getFile().findMessageTypeByName("Address");
        DynamicMessage addr = DynamicMessage.newBuilder(address)
                .setField(address.findFieldByName("city"), "NYC")
                .setField(address.findFieldByName("zip"), 10001)
                .build();
        DynamicMessage ord = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("sym"), "AAPL")
                .setField(order.findFieldByName("address"), addr)
                .build();

        byte[] schemaData = ProtobufNativeSchemaUtils.serialize(order);
        Map<String, Object> fields = new ProtobufNativeValueDecoder(schemaData).decode(ord.toByteArray());

        assertThat(fields.get("sym")).isEqualTo("AAPL");
        assertThat(fields.get("address")).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> a = (Map<String, Object>) fields.get("address");
        assertThat(a.get("city")).isEqualTo("NYC");
        assertThat(((Number) a.get("zip")).intValue()).isEqualTo(10001);
    }

    @Test
    void returnsNullForEmptyPayload() throws Exception {
        ProtobufNativeValueDecoder decoder = new ProtobufNativeValueDecoder(
                ProtobufNativeSchemaUtils.serialize(tradeDescriptor()));
        assertThat(decoder.decode(null)).isNull();
        assertThat(decoder.decode(new byte[0])).isNull();
    }

    @Test
    void rejectsInvalidSchemaData() {
        assertThatThrownBy(() -> new ProtobufNativeValueDecoder("{ not json".getBytes()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ProtobufNativeValueDecoder(new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
