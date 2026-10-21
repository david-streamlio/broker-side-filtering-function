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

package io.streamnative.filter.orchestrator;

import io.streamnative.filter.common.ClientType;
import io.streamnative.filter.common.FilterRequest;
import io.streamnative.filter.common.WindowSpec;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.TopicPolicies;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Answers.RETURNS_SELF;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** {@link ClientWindowPreparer} and {@link DefaultFilterFunctionSpawner#spawnWindowed} against mocks. */
class WindowedSpawnTests {

    private static final String TOPIC = "persistent://public/default/orders";
    private static final String SUB = "win-q1";

    private PulsarAdmin admin;
    private Topics topics;
    private PulsarClient client;
    private ConsumerBuilder<byte[]> consumerBuilder;
    private Consumer<byte[]> consumer;
    private ReaderBuilder<byte[]> readerBuilder;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setup() throws Exception {
        admin = mock(PulsarAdmin.class);
        topics = mock(Topics.class);
        when(admin.topics()).thenReturn(topics);
        client = mock(PulsarClient.class);
        consumerBuilder = mock(ConsumerBuilder.class, RETURNS_SELF);
        consumer = mock(Consumer.class);
        readerBuilder = mock(ReaderBuilder.class, RETURNS_SELF);
        when(client.newConsumer(any(Schema.class))).thenReturn((ConsumerBuilder) consumerBuilder);
        when(consumerBuilder.subscribe()).thenReturn(consumer);
        when(client.newReader(any(Schema.class))).thenReturn((ReaderBuilder) readerBuilder);
    }

    /** {@code partitions} partitions; the namespace lists the topic's log (or nothing when absent). */
    private void givenTopic(int partitions, boolean exists) throws Exception {
        when(topics.getPartitionedTopicMetadata(TOPIC)).thenReturn(new PartitionedTopicMetadata(partitions));
        String log = partitions == 1 ? TOPIC + "-partition-0" : TOPIC;
        when(topics.getList("public/default")).thenReturn(exists ? List.of(log) : List.of());
    }

    @SuppressWarnings("unchecked")
    private static Message<byte[]> messageAt(long logTime) {
        Message<byte[]> m = mock(Message.class);
        when(m.getBrokerPublishTime()).thenReturn(Optional.of(logTime));
        return m;
    }

    /** Last message at ledger:entry; readers return the earliest message, then the last one. */
    @SuppressWarnings("unchecked")
    private void givenLog(long ledger, long entry, Long earliestTime, Long lastTime) throws Exception {
        when(consumer.getLastMessageIds()).thenReturn(List.of(TopicMessageId.create(TOPIC, new MessageIdImpl(ledger, entry, -1))));
        Reader<byte[]> earliest = mock(Reader.class);
        Reader<byte[]> last = mock(Reader.class);
        // Build the messages before stubbing: Mockito rejects stubbing inside another when(...).
        Message<byte[]> earliestMsg = earliestTime == null ? null : messageAt(earliestTime);
        Message<byte[]> lastMsg = lastTime == null ? null : messageAt(lastTime);
        when(earliest.readNext(anyInt(), any())).thenReturn(earliestMsg);
        when(last.readNext(anyInt(), any())).thenReturn(lastMsg);
        when(readerBuilder.create()).thenReturn(earliest, last);
    }

    // ---------- ClientWindowPreparer ----------

    @Test
    void positionsTheSubscriptionThenCapturesTheEndPosition() throws Exception {
        givenTopic(0, true);
        givenLog(9, 188, 500L, 1_900L);

        WindowPreparer.WindowPlan plan = new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L);

        InOrder order = inOrder(consumerBuilder, consumer);
        order.verify(consumerBuilder).startPaused(true);
        order.verify(consumerBuilder).subscribe();
        order.verify(consumer).seek(1_000L);
        order.verify(consumer).getLastMessageIds();
        order.verify(consumer).close();
        verify(consumerBuilder).subscriptionName(SUB);
        verify(consumer, never()).unsubscribe();
        assertThat(plan.spec()).isEqualTo(new WindowSpec(1_000L, 2_000L, 9L, 188L, false));
        assertThat(plan.empty()).isFalse();
    }

    @Test
    void windowStartingBeforeTheEarliestRetainedMessageIsTruncated() throws Exception {
        givenTopic(0, true);
        givenLog(9, 188, 1_500L, 1_900L);

        assertThat(new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L).spec()
                .isTruncatedByRetention()).isTrue();
    }

    @Test
    void emptyTopicGivesAnEmptyWindowAndRemovesTheSubscription() throws Exception {
        givenTopic(0, true);
        when(consumer.getLastMessageIds()).thenReturn(List.of(TopicMessageId.create(TOPIC, new MessageIdImpl(3, -1, -1))));

        WindowPreparer.WindowPlan plan = new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L);

        assertThat(plan.empty()).isTrue();
        assertThat(plan.spec().hasEndPosition()).isFalse();
        assertThat(plan.spec().isTruncatedByRetention()).isFalse();
        verify(consumer).unsubscribe();
        verify(client, never()).newReader(any(Schema.class));
    }

    @Test
    void lastMessageBeforeTheWindowStartGivesAnEmptyWindow() throws Exception {
        givenTopic(0, true);
        givenLog(9, 188, 500L, 900L);

        assertThat(new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L).empty()).isTrue();
        verify(consumer).unsubscribe();
    }

    @Test
    void openEndedWindowWithoutHistoryKeepsTheSubscriptionForLiveEvents() throws Exception {
        givenTopic(0, true);
        givenLog(9, 188, 500L, 900L);

        WindowPreparer.WindowPlan plan = new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, null);

        assertThat(plan.empty()).isTrue();
        assertThat(plan.spec().isOpenEnded()).isTrue();
        verify(consumer).close();
        verify(consumer, never()).unsubscribe();
    }

    @Test
    void windowFromTheBeginningIsNeverTruncated() throws Exception {
        givenTopic(0, true);
        givenLog(9, 188, 1_500L, 1_900L);

        assertThat(new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 0L, 2_000L).spec()
                .isTruncatedByRetention()).isFalse();
    }

    @Test
    void singlePartitionTopicIsReadThroughItsPartition() throws Exception {
        givenTopic(1, true);
        givenLog(4, 7, 500L, 1_900L);

        WindowPreparer.WindowPlan plan = new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L);

        verify(consumerBuilder).topic(TOPIC);
        verify(readerBuilder, org.mockito.Mockito.atLeastOnce()).topic(TOPIC + "-partition-0");
        assertThat(plan.spec().getEndLedgerId()).isEqualTo(4L);
        assertThat(plan.spec().getEndEntryId()).isEqualTo(7L);
    }

    @Test
    void multiPartitionTopicIsRejected() throws Exception {
        givenTopic(3, true);

        assertThatThrownBy(() -> new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("3 partitions");
        verify(client, never()).newConsumer(any(Schema.class));
    }

    @Test
    void missingSourceTopicIsRejectedWithoutAutoCreatingIt() throws Exception {
        givenTopic(0, false);

        assertThatThrownBy(() -> new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not found");
        verify(client, never()).newConsumer(any(Schema.class));
    }

    @Test
    void failureAfterSubscribingClosesTheConsumer() throws Exception {
        givenTopic(0, true);
        org.mockito.Mockito.doThrow(new PulsarClientException("seek failed")).when(consumer).seek(1_000L);
        when(consumer.closeAsync()).thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        assertThatThrownBy(() -> new ClientWindowPreparer(client, admin).prepare(TOPIC, SUB, 1_000L, 2_000L))
                .isInstanceOf(PulsarClientException.class);
        verify(consumer).closeAsync();
    }

    // ---------- DefaultFilterFunctionSpawner.spawnWindowed ----------

    @Test
    void spawnWindowedPublishesBeforeDeployingAndReadsTheSourceOnTheWindowSubscription() throws Exception {
        Functions functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.topicPolicies()).thenReturn(mock(TopicPolicies.class));
        OrchestratorConfig cfg = OrchestratorConfig.forTesting("persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0", "public", "filtered");
        FilterRequest req = FilterRequest.registerWindowed(TOPIC, "persistent://public/filtered/out", "x = 1",
                null, null, null, ClientType.PULSAR, 1_000L, 2_000L);
        FilterFunctionSpawner.ControlTopicAction beforeDeploy = mock(FilterFunctionSpawner.ControlTopicAction.class);

        RegistryEntry entry = new DefaultFilterFunctionSpawner(admin)
                .spawnWindowed("q1", TOPIC, SUB, req, cfg, beforeDeploy);

        String ctrl = entry.getControlTopic();
        assertThat(ctrl).startsWith("persistent://public/filtered/ctrl-win-");
        InOrder order = inOrder(topics, beforeDeploy, functions);
        order.verify(topics).createNonPartitionedTopic(ctrl);
        order.verify(beforeDeploy).accept(ctrl);
        ArgumentCaptor<FunctionConfig> fc = ArgumentCaptor.forClass(FunctionConfig.class);
        order.verify(functions).createFunctionWithUrl(fc.capture(), eq(cfg.functionPackageUrl()));

        FunctionConfig f = fc.getValue();
        assertThat(f.getInputs()).containsExactly(TOPIC);
        assertThat(f.getSubName()).isEqualTo(SUB);
        assertThat(f.getParallelism()).isEqualTo(1);
        assertThat(f.getCleanupSubscription()).isTrue();
        assertThat(f.getName()).isEqualTo(entry.getFilterFunctionName()).startsWith("selector-filter-win-");
        assertThat(f.getUserConfig()).containsEntry("control-topic", ctrl);
    }

    @Test
    void spawnWindowedDoesNotDeployWhenPublishingFails() throws Exception {
        Functions functions = mock(Functions.class);
        when(admin.functions()).thenReturn(functions);
        when(admin.topicPolicies()).thenReturn(mock(TopicPolicies.class));
        OrchestratorConfig cfg = OrchestratorConfig.forTesting("persistent://public/orchestrator/state",
                "function://public/filters/selector-filter@1.0.0", "public", "filtered");
        FilterRequest req = FilterRequest.registerWindowed(TOPIC, "persistent://public/filtered/out", "x = 1",
                null, null, null, ClientType.PULSAR, 1_000L, 2_000L);

        assertThatThrownBy(() -> new DefaultFilterFunctionSpawner(admin).spawnWindowed("q1", TOPIC, SUB, req, cfg,
                ctrl -> { throw new IllegalStateException("control topic unavailable"); }))
                .isInstanceOf(IllegalStateException.class);
        verify(functions, never()).createFunctionWithUrl(any(), anyString());
    }
}
