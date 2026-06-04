package io.streamnative.filter;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Bench B (handoff §6) — per-message value-decode cost: Avro and Protobuf-native {@code decode(byte[])}
 * plus a field lookup, top-level vs nested, to size the decode path relative to selector eval
 * (Bench A in {@code common}). Reuses the exact fixtures from {@link AvroValueDecoderTests} and
 * {@link ProtobufNativeValueDecoderTests} — i.e. the wire bytes Pulsar's own encoders produce.
 *
 * <p>Lives in the function module's test tree because the decoders ({@link AvroValueDecoder},
 * {@link ProtobufNativeValueDecoder}) are package-private; it would not compile anywhere else
 * without widening their visibility. Not a unit test: the {@code *Bench} name matches no default
 * surefire/failsafe include pattern, so it never runs in a normal build. Run it explicitly:
 * <pre>{@code
 *   export JAVA_HOME=$(/usr/libexec/java_home -v 21)
 *   mvn -pl functions/selector-filter -am test -Dtest=ValueDecodeBench -DfailIfNoTests=false
 *   # tune: -Dbench.iters=1000000 -Dbench.warmup=100000
 * }</pre>
 */
class ValueDecodeBench {

    private static final long WARMUP = Long.getLong("bench.warmup", 50_000L);
    private static final long ITERS = Long.getLong("bench.iters", 500_000L);

    // ---- Avro fixtures (mirror AvroValueDecoderTests) ----
    public static class Trade {
        public String sym;
        public double price;
        public int qty;
        public boolean active;
    }

    public static class Address {
        public String city;
        public int zip;
    }

    public static class Order {
        public String sym;
        public Address address;
    }

    private long sink; // read at the end to defeat dead-code elimination

    @Test
    void benchmark() throws Exception {
        System.out.printf("%n=== Bench B: value decode (ns/op) — warmup=%d iters=%d, JDK %s ===%n",
                WARMUP, ITERS, System.getProperty("java.version"));
        System.out.printf("%-34s %14s %16s%n", "case", "decode", "decode+lookup");

        // ---------- Avro: top-level ----------
        Schema<Trade> tradeSchema = Schema.AVRO(Trade.class);
        Trade t = new Trade();
        t.sym = "AAPL"; t.price = 195.5; t.qty = 10; t.active = true;
        byte[] avroTop = tradeSchema.encode(t);
        AvroValueDecoder avroTopDec = new AvroValueDecoder(avsc(tradeSchema));
        row("avro top-level (4 scalars)",
                time(() -> sink += size(avroTopDec.decode(avroTop))),
                time(() -> sink += lookupTop(avroTopDec.decode(avroTop), "sym")));

        // ---------- Avro: nested ----------
        Schema<Order> orderSchema = Schema.AVRO(Order.class);
        Order o = new Order();
        o.sym = "AAPL";
        o.address = new Address();
        o.address.city = "NYC"; o.address.zip = 10001;
        byte[] avroNested = orderSchema.encode(o);
        AvroValueDecoder avroNestedDec = new AvroValueDecoder(avsc(orderSchema));
        row("avro nested (address.city)",
                time(() -> sink += size(avroNestedDec.decode(avroNested))),
                time(() -> sink += lookupNested(avroNestedDec.decode(avroNested), "address", "city")));

        // ---------- Protobuf-native: top-level ----------
        Descriptor trade = tradeDescriptor();
        byte[] pbTradeSchema = ProtobufNativeSchemaUtils.serialize(trade);
        byte[] pbTop = DynamicMessage.newBuilder(trade)
                .setField(trade.findFieldByName("sym"), "AAPL")
                .setField(trade.findFieldByName("price"), 195.5)
                .setField(trade.findFieldByName("qty"), 10)
                .setField(trade.findFieldByName("active"), true)
                .build().toByteArray();
        ProtobufNativeValueDecoder pbTopDec = new ProtobufNativeValueDecoder(pbTradeSchema);
        row("protobuf top-level (4 scalars)",
                time(() -> sink += size(pbTopDec.decode(pbTop))),
                time(() -> sink += lookupTop(pbTopDec.decode(pbTop), "sym")));

        // ---------- Protobuf-native: nested ----------
        Descriptor order = orderDescriptor();
        byte[] pbOrderSchema = ProtobufNativeSchemaUtils.serialize(order);
        Descriptor addrDesc = order.findFieldByName("address").getMessageType();
        DynamicMessage addrMsg = DynamicMessage.newBuilder(addrDesc)
                .setField(addrDesc.findFieldByName("city"), "NYC")
                .setField(addrDesc.findFieldByName("zip"), 10001)
                .build();
        byte[] pbNested = DynamicMessage.newBuilder(order)
                .setField(order.findFieldByName("sym"), "AAPL")
                .setField(order.findFieldByName("address"), addrMsg)
                .build().toByteArray();
        ProtobufNativeValueDecoder pbNestedDec = new ProtobufNativeValueDecoder(pbOrderSchema);
        row("protobuf nested (address.city)",
                time(() -> sink += size(pbNestedDec.decode(pbNested))),
                time(() -> sink += lookupNested(pbNestedDec.decode(pbNested), "address", "city")));

        System.out.printf("(sink=%d)%n", sink);
    }

    // ---- protobuf descriptors (mirror ProtobufNativeValueDecoderTests) ----

    static Descriptor tradeDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("trade.proto").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Trade")
                        .addField(field("sym", 1, Type.TYPE_STRING))
                        .addField(field("price", 2, Type.TYPE_DOUBLE))
                        .addField(field("qty", 3, Type.TYPE_INT32))
                        .addField(field("active", 4, Type.TYPE_BOOL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Trade");
    }

    static Descriptor orderDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("order.proto").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Address")
                        .addField(field("city", 1, Type.TYPE_STRING))
                        .addField(field("zip", 2, Type.TYPE_INT32)))
                .addMessageType(DescriptorProto.newBuilder().setName("Order")
                        .addField(field("sym", 1, Type.TYPE_STRING))
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("address").setNumber(2)
                                .setType(Type.TYPE_MESSAGE).setTypeName(".Address")
                                .setLabel(Label.LABEL_OPTIONAL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Order");
    }

    private static FieldDescriptorProto field(String name, int number, Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(Label.LABEL_OPTIONAL).build();
    }

    // ---- helpers ----

    private static String avsc(Schema<?> schema) {
        return new String(schema.getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
    }

    private static int size(Map<String, Object> m) {
        return m == null ? 0 : m.size();
    }

    private static int lookupTop(Map<String, Object> m, String key) {
        return m == null || m.get(key) == null ? 0 : 1;
    }

    @SuppressWarnings("unchecked")
    private static int lookupNested(Map<String, Object> m, String parent, String child) {
        if (m == null) return 0;
        Object inner = m.get(parent);
        if (!(inner instanceof Map)) return 0;
        return ((Map<String, Object>) inner).get(child) == null ? 0 : 1;
    }

    private void row(String label, double decodeNs, double decodeLookupNs) {
        System.out.printf("%-34s %14.1f %16.1f%n", label, decodeNs, decodeLookupNs);
    }

    private double time(LongOp op) {
        for (long i = 0; i < WARMUP; i++) sink += op.run();
        long start = System.nanoTime();
        for (long i = 0; i < ITERS; i++) sink += op.run();
        return (System.nanoTime() - start) / (double) ITERS;
    }

    @FunctionalInterface
    private interface LongOp {
        long run();
    }
}
