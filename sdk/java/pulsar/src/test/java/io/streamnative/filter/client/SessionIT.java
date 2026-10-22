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

package io.streamnative.filter.client;

import io.streamnative.filter.api.FilterControl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.ValueSchemaInfo;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * SDK end-to-end IT against a live Pulsar broker with the orchestrator function deployed.
 *
 * <p>Setup mirrors the production deployment sequence:
 * <ol>
 *   <li>Pre-upload the selector-filter NAR as a Pulsar package via {@code admin.packages().upload()}.</li>
 *   <li>Deploy the orchestrator function with {@code userConfig.function-package-url}
 *       referencing the uploaded package.</li>
 *   <li>SDK clients publish {@code FilterRequest}s to the orchestrator's input topic; the
 *       orchestrator spawns per-input-topic filter functions on demand.</li>
 * </ol>
 *
 * <p>Requires Docker and pre-built NARs in {@code functions/selector-filter/target/} and
 * {@code functions/orchestrator/target/}. Run {@code mvn verify} at the reactor root, which
 * builds in dependency order automatically.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SessionIT {

    private static final Logger LOG = LoggerFactory.getLogger(SessionIT.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Minimal POJO for the typed-value end-to-end case. Public fields so Jackson/Schema.JSON map it. */
    public static class Trade {
        public String sym;
        public double price;
    }

    private static final String FN_TENANT          = "public";
    private static final String FN_NAMESPACE       = "default";
    private static final String ORCH_NAME          = "selector-filter-orchestrator-sessionit";
    private static final String FILTER_PACKAGE_URL = "function://public/default/selector-filter@1.0";

    private static final String SDK_INPUT_TOPIC    = "persistent://public/default/sdk-it-filter-requests";
    private static final String ORCH_STATE_TOPIC   = "persistent://public/default/sdk-it-orchestrator-state";

    private static final String INPUT_TOPIC        = "persistent://public/default/trades-sdk-it";
    private static final String EXTRA_INPUT_TOPIC  = "persistent://public/default/extra-sdk-it";

    // Each value-field test gets its own input topic so its spawned filter has a distinct name; this
    // keeps the snapshot-diff readiness check (waitForNewSpawnedFilter) from latching onto a stale
    // filter left mid-teardown by a sibling value-field test running earlier in the same broker.
    private static final String VF_AVRO_TOPIC      = "persistent://public/default/trades-vf-avro-sdk-it";
    private static final String VF_JSON_TOPIC      = "persistent://public/default/trades-vf-json-sdk-it";
    private static final String VF_PROTOBUF_TOPIC  = "persistent://public/default/trades-vf-protobuf-sdk-it";

    private static final Path FILTER_NAR       = findFilterNar();
    private static final Path ORCHESTRATOR_NAR = findOrchestratorNar();

    @Container
    private static final PulsarContainer pulsar = new PulsarContainer(
            DockerImageName.parse("apachepulsar/pulsar:4.2.1"))
            .withFunctionsWorker()
            .withStartupTimeout(java.time.Duration.ofMinutes(3));

    private PulsarClient client;
    private PulsarAdmin admin;
    private Producer<byte[]> inputProducer;

    @BeforeAll
    void setup() throws Exception {
        client = PulsarClient.builder().serviceUrl(pulsar.getPulsarBrokerUrl()).build();
        admin  = PulsarAdmin.builder().serviceHttpUrl(pulsar.getHttpServiceUrl()).build();
        waitForDefaultNamespace(admin);

        admin.topics().createNonPartitionedTopic(SDK_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(ORCH_STATE_TOPIC);
        admin.topics().createNonPartitionedTopic(INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(EXTRA_INPUT_TOPIC);
        admin.topics().createNonPartitionedTopic(VF_AVRO_TOPIC);
        admin.topics().createNonPartitionedTopic(VF_JSON_TOPIC);
        admin.topics().createNonPartitionedTopic(VF_PROTOBUF_TOPIC);
        admin.namespaces().setAutoTopicCreation("public/default",
                org.apache.pulsar.common.policies.data.AutoTopicCreationOverride.builder()
                        .allowAutoTopicCreation(true)
                        .topicType("non-partitioned")
                        .build());

        // Upload the selector-filter NAR as a Pulsar package — orchestrator references this URL.
        PackageMetadata pkg = PackageMetadata.builder()
                .description("selector-filter NAR for SessionIT")
                .contact("ci@localhost")
                .build();
        admin.packages().upload(pkg, FILTER_PACKAGE_URL, FILTER_NAR.toString());
        LOG.info("Uploaded filter NAR as package {}", FILTER_PACKAGE_URL);

        // Deploy the orchestrator.
        FunctionConfig orchConfig = FunctionConfig.builder()
                .tenant(FN_TENANT)
                .namespace(FN_NAMESPACE)
                .name(ORCH_NAME)
                .runtime(FunctionConfig.Runtime.JAVA)
                .className("io.streamnative.filter.orchestrator.OrchestratorFunction")
                .inputs(List.of(SDK_INPUT_TOPIC))
                .subscriptionPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE)
                .parallelism(1)
                .userConfig(Map.of(
                        "state-topic",            ORCH_STATE_TOPIC,
                        "function-package-url",   FILTER_PACKAGE_URL,
                        "function-tenant",        FN_TENANT,
                        "function-namespace",     FN_NAMESPACE,
                        "function-parallelism",   1,
                        "apply-compaction",       true,
                        "pulsar-admin-url",       "http://localhost:8080"))
                .build();
        deployWithLeaderRetry(admin, orchConfig, ORCHESTRATOR_NAR.toString(),
                FN_TENANT, FN_NAMESPACE, ORCH_NAME);
        waitForFunctionRunning(admin, FN_TENANT, FN_NAMESPACE, ORCH_NAME, 90_000L);
        Thread.sleep(5_000); // orchestrator initialize() needs to open its TableView

        inputProducer = client.newProducer(Schema.BYTES).topic(INPUT_TOPIC).create();
    }

    @AfterAll
    void teardown() throws Exception {
        if (inputProducer != null) inputProducer.close();
        if (client != null) client.close();
        if (admin != null) admin.close();
    }

    // ---------------------------------------------------------------- Tests

    @Test
    void newConsumerEndToEnd() throws Exception {
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_TOPIC)
                     .selector("region = 'US'")
                     .subscribe()) {

            FilterControl fc = (FilterControl) consumer;
            waitForSpawnedFilter(INPUT_TOPIC);

            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("p1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BMW").property("region", "DE")
                    .value("p2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("MSFT").property("region", "US")
                    .value("p3".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> m1 = consumer.receive(30, TimeUnit.SECONDS);
            Message<byte[]> m2 = consumer.receive(15, TimeUnit.SECONDS);
            Message<byte[]> m3 = consumer.receive(5, TimeUnit.SECONDS);

            assertThat(m1).isNotNull();
            assertThat(m2).isNotNull();
            assertThat(new String(m1.getData(), StandardCharsets.UTF_8)).isEqualTo("p1");
            assertThat(new String(m2.getData(), StandardCharsets.UTF_8)).isEqualTo("p3");
            assertThat(m1.getProperty("region")).isEqualTo("US");
            assertThat(m3).as("Only US messages should pass the selector").isNull();

            consumer.acknowledge(m1);
            consumer.acknowledge(m2);

            String privateTopic = fc.privateTopic();
            consumer.close();

            assertTopicEventuallyDeleted(privateTopic, 15_000L);
        }
    }

    @Test
    void typedJsonSchemaEndToEnd() throws Exception {
        // The filter forwards raw value bytes verbatim; the SDK consumer decodes them client-side
        // with the supplied Schema.JSON. Produce JSON bytes through the byte[] inputProducer, then
        // consume as a typed Trade. The key stays a String (Pulsar has no key schema).
        try (Session session = newSession();
             Consumer<Trade> consumer = session.newConsumer(Schema.JSON(Trade.class))
                     .topic(INPUT_TOPIC)
                     .selector("region = 'US'")
                     .subscribe()) {

            waitForSpawnedFilter(INPUT_TOPIC);

            Trade aapl = new Trade(); aapl.sym = "AAPL"; aapl.price = 195.5;
            Trade bmw  = new Trade(); bmw.sym  = "BMW";  bmw.price  = 88.0;
            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value(JSON.writeValueAsBytes(aapl)).send();
            inputProducer.newMessage().key("BMW").property("region", "DE")
                    .value(JSON.writeValueAsBytes(bmw)).send();

            Message<Trade> m1 = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(m1).as("US message should pass the selector").isNotNull();
            // Key is still the plain String key.
            assertThat(m1.getKey()).isEqualTo("AAPL");
            // Value decoded client-side via Schema.JSON — no IncompatibleSchemaException.
            Trade decoded = m1.getValue();
            assertThat(decoded.sym).isEqualTo("AAPL");
            assertThat(decoded.price).isEqualTo(195.5);

            assertThat(consumer.receive(5, TimeUnit.SECONDS))
                    .as("Only US messages should pass the selector").isNull();
            consumer.acknowledge(m1);
        }
    }

    // ---------------------------------------------------------------- value-schema server-side decode
    //
    // These three exercise the filter decoding the message VALUE on the broker — via the unshaded
    // avro / protobuf-java the NAR bundles, or the best-effort JSON path — to evaluate a selector on
    // a value field. The two messages in each test share identical (absent) properties and differ
    // ONLY in the value field `price`; the private topic is consumed as raw bytes, so a routed
    // message proves the decode happened server-side (the typed client-side decode is covered by
    // typedJsonSchemaEndToEnd). They are the regression guard for the bundled avro-/protobuf-java/
    // jackson versions: a library bump that breaks the unshaded-vs-broker-shaded coexistence fails
    // these against a live broker, where the byte-level unit tests can't.

    @Test
    void avroValueFieldSelectorDecodedServerSide() throws Exception {
        Schema<Trade> avro = Schema.AVRO(Trade.class);
        ValueSchemaInfo vsi = Session.toValueSchemaInfo(avro.getSchemaInfo());
        byte[] routed = singleRoutedByValueField(vsi, "avro", VF_AVRO_TOPIC,
                avro.encode(tradeOf("AAPL", 195.5)), "AAPL",
                avro.encode(tradeOf("PENNY", 5.0)), "PENNY");
        Trade decoded = avro.decode(routed);
        assertThat(decoded.sym).isEqualTo("AAPL");
        assertThat(decoded.price).isEqualTo(195.5);
    }

    @Test
    void jsonValueFieldSelectorDecodedServerSide() throws Exception {
        Schema<Trade> json = Schema.JSON(Trade.class);
        ValueSchemaInfo vsi = Session.toValueSchemaInfo(json.getSchemaInfo());
        byte[] routed = singleRoutedByValueField(vsi, "json", VF_JSON_TOPIC,
                JSON.writeValueAsBytes(tradeOf("MSFT", 420.0)), "MSFT",
                JSON.writeValueAsBytes(tradeOf("CENT", 1.0)), "CENT");
        Trade decoded = JSON.readValue(routed, Trade.class);
        assertThat(decoded.sym).isEqualTo("MSFT");
        assertThat(decoded.price).isEqualTo(420.0);
    }

    @Test
    void protobufNativeValueFieldSelectorDecodedServerSide() throws Exception {
        // PROTOBUF_NATIVE carries the descriptor in the schema; the filter rebuilds it and decodes
        // with bundled protobuf-java. Build the descriptor by hand (no generated class) and serialize
        // it exactly as Pulsar does.
        Descriptor trade = tradeProtoDescriptor();
        ValueSchemaInfo vsi = new ValueSchemaInfo("Trade", "PROTOBUF_NATIVE",
                Base64.getEncoder().encodeToString(ProtobufNativeSchemaUtils.serialize(trade)), null);
        byte[] routed = singleRoutedByValueField(vsi, "protobuf", VF_PROTOBUF_TOPIC,
                protoTrade(trade, "GOOG", 1500.0), "GOOG",
                protoTrade(trade, "MICRO", 2.0), "MICRO");
        DynamicMessage got = DynamicMessage.parseFrom(trade, routed);
        assertThat(got.getField(trade.findFieldByName("sym"))).isEqualTo("GOOG");
        assertThat(((Number) got.getField(trade.findFieldByName("price"))).doubleValue()).isEqualTo(1500.0);
    }

    /**
     * Registers a {@code price > 100} value-field selector carrying {@code vsi} on {@code inputTopic},
     * produces {@code hi} (price&gt;100) then {@code lo} (price&le;100) as raw value bytes, and asserts
     * exactly the {@code hi} record is routed to the private topic — i.e. the filter decoded the value
     * server-side. Returns the routed value bytes for format-specific checks.
     *
     * <p>Each caller passes its own {@code inputTopic} and we wait for the specific filter newly
     * spawned for this query (see {@link #waitForNewSpawnedFilter}), so sibling value-field tests
     * cycling filters in the same broker can't make us produce into a stale/torn-down filter.
     */
    private byte[] singleRoutedByValueField(ValueSchemaInfo vsi, String label, String inputTopic,
                                            byte[] hi, String hiKey,
                                            byte[] lo, String loKey) throws Exception {
        try (Session session = newSession();
             Producer<byte[]> producer = client.newProducer(Schema.BYTES).topic(inputTopic).create()) {
            Set<String> filtersBefore = spawnedFilterNames();
            PrivateQuery pq = session.registerPrivateQuery(List.of(inputTopic), "price > 100",
                    null, vsi.getName(), vsi, ClientType.PULSAR);
            try (Consumer<byte[]> consumer = client.newConsumer(Schema.BYTES)
                    .topic(pq.privateTopic())
                    .subscriptionName("vf-it-" + pq.queryId())
                    .subscribe()) {

                waitForNewSpawnedFilter(filtersBefore);

                producer.newMessage().key(hiKey).value(hi).send();
                producer.newMessage().key(loKey).value(lo).send();

                Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
                assertThat(m).as("price>100 %s value should pass the value-field selector", label).isNotNull();
                byte[] data = m.getData();
                consumer.acknowledge(m);
                assertThat(consumer.receive(5, TimeUnit.SECONDS))
                        .as("price<=100 %s value must be filtered out server-side", label).isNull();
                return data;
            } finally {
                session.retirePrivateQuery(pq);
            }
        }
    }

    private static Trade tradeOf(String sym, double price) {
        Trade t = new Trade();
        t.sym = sym;
        t.price = price;
        return t;
    }

    /** proto3 {@code Trade { string sym=1; double price=2; int32 qty=3; bool active=4; }}. */
    private static Descriptor tradeProtoDescriptor() throws Exception {
        FileDescriptorProto file = FileDescriptorProto.newBuilder()
                .setName("trade_it.proto").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("Trade")
                        .addField(protoField("sym", 1, Type.TYPE_STRING))
                        .addField(protoField("price", 2, Type.TYPE_DOUBLE))
                        .addField(protoField("qty", 3, Type.TYPE_INT32))
                        .addField(protoField("active", 4, Type.TYPE_BOOL)))
                .build();
        return FileDescriptor.buildFrom(file, new FileDescriptor[0]).findMessageTypeByName("Trade");
    }

    private static FieldDescriptorProto protoField(String name, int number, Type type) {
        return FieldDescriptorProto.newBuilder()
                .setName(name).setNumber(number).setType(type).setLabel(Label.LABEL_OPTIONAL).build();
    }

    private static byte[] protoTrade(Descriptor trade, String sym, double price) {
        return DynamicMessage.newBuilder(trade)
                .setField(trade.findFieldByName("sym"), sym)
                .setField(trade.findFieldByName("price"), price)
                .setField(trade.findFieldByName("qty"), 1)
                .setField(trade.findFieldByName("active"), true)
                .build().toByteArray();
    }

    @Test
    void setSelectorHotReloadsAgainstLiveFunction() throws Exception {
        try (Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topic(INPUT_TOPIC)
                     .selector("region = 'US'")
                     .subscribe()) {

            FilterControl fc = (FilterControl) consumer;
            String stableQueryId = fc.queryId();
            waitForSpawnedFilter(INPUT_TOPIC);

            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("us-1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BMW").property("region", "DE")
                    .value("de-1".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("BARC").property("region", "UK")
                    .value("uk-1".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> a1 = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(a1).isNotNull();
            assertThat(new String(a1.getData(), StandardCharsets.UTF_8)).isEqualTo("us-1");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only US should match under selector A").isNull();
            consumer.acknowledge(a1);

            fc.setSelector("region = 'UK'");
            assertThat(fc.queryId()).isEqualTo(stableQueryId);
            assertThat(fc.currentSelector()).isEqualTo("region = 'UK'");
            Thread.sleep(5_000); // orchestrator MODIFY → filter TableView hot-reload

            inputProducer.newMessage().key("MSFT").property("region", "US")
                    .value("us-2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("HSBC").property("region", "UK")
                    .value("uk-2".getBytes(StandardCharsets.UTF_8)).send();
            inputProducer.newMessage().key("AUDI").property("region", "DE")
                    .value("de-2".getBytes(StandardCharsets.UTF_8)).send();

            Message<byte[]> b1 = consumer.receive(30, TimeUnit.SECONDS);
            assertThat(b1).isNotNull();
            assertThat(new String(b1.getData(), StandardCharsets.UTF_8)).isEqualTo("uk-2");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only UK should match under selector B").isNull();
            consumer.acknowledge(b1);
        }
    }

    @Test
    void multiTopicSubscribeRoutesBothSources() throws Exception {
        try (Producer<byte[]> extraProducer = client.newProducer(Schema.BYTES).topic(EXTRA_INPUT_TOPIC).create();
             Session session = newSession();
             Consumer<byte[]> consumer = session.newConsumer()
                     .topics(List.of(INPUT_TOPIC, EXTRA_INPUT_TOPIC))
                     .selector("region = 'US'")
                     .subscribe()) {

            // Orchestrator spawns ONE filter function per input topic — wait for both.
            waitForSpawnedFilter(INPUT_TOPIC);
            waitForSpawnedFilter(EXTRA_INPUT_TOPIC);

            inputProducer.newMessage().key("AAPL").property("region", "US")
                    .value("from-a".getBytes(StandardCharsets.UTF_8)).send();
            extraProducer.newMessage().key("MSFT").property("region", "US")
                    .value("from-b".getBytes(StandardCharsets.UTF_8)).send();
            extraProducer.newMessage().key("BMW").property("region", "DE")
                    .value("from-b-de".getBytes(StandardCharsets.UTF_8)).send();

            java.util.Set<String> seen = new java.util.HashSet<>();
            for (int i = 0; i < 2; i++) {
                Message<byte[]> m = consumer.receive(30, TimeUnit.SECONDS);
                assertThat(m).isNotNull();
                seen.add(new String(m.getData(), StandardCharsets.UTF_8));
                consumer.acknowledge(m);
            }
            assertThat(seen).containsExactlyInAnyOrder("from-a", "from-b");
            assertThat(consumer.receive(3, TimeUnit.SECONDS))
                    .as("Only US messages should match").isNull();
        }
    }

    @Test
    void subscribeWithMalformedSelectorFailsLocallyWithoutTouchingControlTopic() throws Exception {
        try (Session session = newSession()) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> session.newConsumer()
                            .topic(INPUT_TOPIC)
                            .selector("THIS IS NOT @@## VALID JMS")
                            .subscribe())
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    // ---------------------------------------------------------------- Helpers

    private Session newSession() throws Exception {
        return Session.builder()
                .pulsarClient(client)
                .pulsarAdmin(admin)
                .controlTopic(SDK_INPUT_TOPIC)
                .privateTopicTenant(FN_TENANT)
                .privateTopicNamespace(FN_NAMESPACE)
                .privateTopicPolicy(PrivateTopicPolicy.builder()
                        .messageTtlSeconds(60)
                        .retentionTimeMinutes(1)
                        .retentionSizeMB(8)
                        .inactiveDeleteSeconds(60)
                        .build())
                .build();
    }

    /**
     * Wait for the orchestrator to have spawned a filter function for {@code inputTopic} and for
     * that function to reach RUNNING. The function name is derived from the same
     * {@code TopicKeySanitizer} the orchestrator uses, computed via the function-status admin API
     * — we don't reach into the orchestrator's internal class here.
     */
    /** Names of currently-known spawned filter functions (excludes the orchestrator itself). */
    private Set<String> spawnedFilterNames() throws Exception {
        Set<String> names = new HashSet<>();
        for (String name : admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE)) {
            if (name.startsWith("selector-filter-") && !name.equals(ORCH_NAME)) {
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Waits for a filter function that did <em>not</em> exist in {@code before} to reach RUNNING.
     * Unlike {@link #waitForSpawnedFilter}, this ignores stale/dying filters left by earlier tests —
     * essential when value-field tests each register a query (on their own input topic) and retire it,
     * cycling filters within one broker lifetime. Matching only a brand-new function name guarantees we
     * gate on <em>this</em> test's filter, never a sibling's leftover.
     */
    private void waitForNewSpawnedFilter(Set<String> before) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000L;
        String foundName = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                for (String name : admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE)) {
                    if (!name.startsWith("selector-filter-") || name.equals(ORCH_NAME) || before.contains(name)) {
                        continue;
                    }
                    FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, name);
                    if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                        foundName = name;
                        break;
                    }
                }
                if (foundName != null) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        if (foundName == null) {
            throw new IllegalStateException("No newly-spawned filter function reached RUNNING within 90s");
        }
        LOG.info("Newly-spawned filter function {} is RUNNING", foundName);
        Thread.sleep(3_000); // grace for the filter function's TableView to load the QueryMessage
    }

    private void waitForSpawnedFilter(String inputTopic) throws Exception {
        // The orchestrator names spawned functions "selector-filter-<sanitized>-<8-hex-hash>".
        // We don't reproduce the sanitizer here; instead we poll the function list for any name
        // starting with "selector-filter-" that's NOT the orchestrator itself, and wait for it
        // to reach RUNNING.
        long deadline = System.currentTimeMillis() + 90_000L;
        String foundName = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                List<String> functions = admin.functions().getFunctions(FN_TENANT, FN_NAMESPACE);
                for (String name : functions) {
                    if (name.startsWith("selector-filter-") && !name.equals(ORCH_NAME)) {
                        FunctionStatus status = admin.functions().getFunctionStatus(FN_TENANT, FN_NAMESPACE, name);
                        if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                            foundName = name;
                            break;
                        }
                    }
                }
                if (foundName != null) break;
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        if (foundName == null) {
            throw new IllegalStateException(
                    "No spawned filter function reached RUNNING within 90s for input topic " + inputTopic);
        }
        LOG.info("Spawned filter function {} is RUNNING for input topic {}", foundName, inputTopic);
        Thread.sleep(3_000); // grace for filter function's TableView to load the QueryMessage
    }

    private void assertTopicEventuallyDeleted(String privateTopic, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (!admin.topics().getList("public/default").contains(privateTopic)) {
                return;
            }
            Thread.sleep(500);
        }
        assertThat(admin.topics().getList("public/default"))
                .as("private topic %s should be cleaned up on close", privateTopic)
                .doesNotContain(privateTopic);
    }

    /**
     * The container can report ready before Pulsar standalone has created {@code public/default};
     * creating a topic then fails with "Namespace not found". Seen in CI (twice, Oct 2026) when this
     * IT ran right after WindowedSessionIT.
     */
    private static void waitForDefaultNamespace(PulsarAdmin admin) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (true) {
            try {
                if (admin.namespaces().getNamespaces("public").contains("public/default")) {
                    return;
                }
            } catch (PulsarAdminException.NotFoundException tenantNotCreatedYet) {
                // keep waiting
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Namespace public/default did not appear within 60s");
            }
            Thread.sleep(500);
        }
    }

    private static void deployWithLeaderRetry(PulsarAdmin admin, FunctionConfig config, String pkg,
                                              String tenant, String ns, String name) throws Exception {
        String packageUrl = "function://" + tenant + "/" + ns + "/" + name + "@0";
        long deadline = System.currentTimeMillis() + 90_000L;
        Exception last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                admin.functions().createFunction(config, pkg);
                return;
            } catch (PulsarAdminException e) {
                last = e;
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (msg.contains("Leader not yet ready") || msg.contains("Service Unavailable")
                        || msg.contains("already exists")) {
                    try {
                        admin.packages().delete(packageUrl);
                    } catch (Exception ignored) {}
                    Thread.sleep(1_000);
                    continue;
                }
                throw e;
            }
        }
        throw new IllegalStateException("Function deploy timed out waiting for leader readiness: " + name, last);
    }

    private static void waitForFunctionRunning(PulsarAdmin admin, String tenant, String ns,
                                               String name, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try {
                FunctionStatus status = admin.functions().getFunctionStatus(tenant, ns, name);
                if (status.getNumInstances() > 0 && status.getNumRunning() == status.getNumInstances()) {
                    LOG.info("Function {}/{}/{} is RUNNING", tenant, ns, name);
                    return;
                }
            } catch (Exception ignored) {}
            Thread.sleep(500);
        }
        throw new IllegalStateException("Function did not reach RUNNING within " + timeoutMs + " ms: " + name);
    }

    private static Path findFilterNar() {
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", "selector-filter", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-function-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "Filter NAR not found in " + target + ". Run 'mvn -pl functions/selector-filter package' first.");
        }
        return files[0].toPath();
    }

    private static Path findOrchestratorNar() {
        Path target = Paths.get(System.getProperty("user.dir"),
                "..", "..", "..", "functions", "orchestrator", "target").normalize();
        File[] files = target.toFile().listFiles((dir, n) ->
                n.startsWith("selector-filter-orchestrator-") && n.endsWith(".nar"));
        if (files == null || files.length == 0) {
            throw new IllegalStateException(
                    "Orchestrator NAR not found in " + target + ". The orchestrator module builds"
                    + " AFTER this one in the reactor (it depends on selector-filter-client), so a"
                    + " single 'mvn verify' won't have produced it yet. Build all NARs first:"
                    + " 'mvn -DskipTests package' then 'mvn verify' (or 'mvn -pl functions/orchestrator"
                    + " package' before running this IT in isolation).");
        }
        return files[0].toPath();
    }
}
