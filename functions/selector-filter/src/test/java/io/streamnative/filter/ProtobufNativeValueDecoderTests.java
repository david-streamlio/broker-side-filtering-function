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
