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

import io.streamnative.filter.WindowState.Admission;
import io.streamnative.filter.common.LogPosition;
import io.streamnative.filter.common.WindowSpec;
import io.streamnative.filter.common.WindowStats;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class WindowStateTests {

    private final AtomicLong clock = new AtomicLong(10_000L);

    private WindowState window(Long endLedger, Long endEntry, boolean truncated) {
        return new WindowState(new WindowSpec(1_000L, 2_000L, endLedger, endEntry, truncated), clock::get);
    }

    private static LogPosition pos(long ledger, long entry) {
        return LogPosition.fromBytes(new MessageIdImpl(ledger, entry, -1).toByteArray());
    }

    private static LogPosition batchPos(long ledger, long entry, int index, int size) {
        return LogPosition.fromBytes(new BatchMessageIdImpl(ledger, entry, -1, index, size, new BitSet()).toByteArray());
    }

    // ---------- LogPosition: decoded from real MessageId encodings ----------

    @Test
    void decodesUnbatchedMessageId() {
        LogPosition p = pos(17, 42);
        assertThat(p.ledgerId()).isEqualTo(17L);
        assertThat(p.entryId()).isEqualTo(42L);
        assertThat(p.isLastInEntry()).isTrue();
    }

    @Test
    void decodesBatchedMessageId() {
        LogPosition middle = batchPos(17, 42, 3, 10);
        assertThat(middle.ledgerId()).isEqualTo(17L);
        assertThat(middle.entryId()).isEqualTo(42L);
        assertThat(middle.batchIndex()).isEqualTo(3);
        assertThat(middle.batchSize()).isEqualTo(10);
        assertThat(middle.isLastInEntry()).isFalse();
        assertThat(batchPos(17, 42, 9, 10).isLastInEntry()).isTrue();
    }

    @Test
    void decodesTopicMessageIdOfAPartitionedTopic() {
        LogPosition p = LogPosition.fromBytes(new TopicMessageIdImpl("persistent://t/n/x-partition-0",
                new MessageIdImpl(5, 6, 0)).toByteArray());
        assertThat(p.ledgerId()).isEqualTo(5L);
        assertThat(p.entryId()).isEqualTo(6L);
    }

    @Test
    void largeIdsSurviveVarintDecoding() {
        LogPosition p = pos(9_876_543_210L, 123_456_789L);
        assertThat(p.ledgerId()).isEqualTo(9_876_543_210L);
        assertThat(p.entryId()).isEqualTo(123_456_789L);
    }

    @Test
    void positionComparesByLedgerThenEntry() {
        assertThat(pos(5, 9).compareTo(6, 0)).isNegative();
        assertThat(pos(6, 1).compareTo(6, 0)).isPositive();
        assertThat(batchPos(6, 0, 4, 5).compareTo(6, 0)).isZero();
        assertThat(LogPosition.fromBytes(null)).isNull();
        assertThat(LogPosition.fromBytes(new byte[]{(byte) 0xFF})).isNull();
        assertThat(LogPosition.fromBytes(new MessageIdImpl(3, -1, -1).toByteArray()).isMessage()).isFalse();
    }

    // ---------- WindowState ----------

    @Test
    void classifiesByTime() {
        WindowState w = window(null, null, false);
        assertThat(w.admit(999L, null)).isEqualTo(Admission.BEFORE_WINDOW);
        assertThat(w.admit(1_000L, null)).isEqualTo(Admission.IN_WINDOW);
        assertThat(w.admit(2_000L, null)).isEqualTo(Admission.IN_WINDOW);
        assertThat(w.admit(2_001L, null)).isEqualTo(Admission.PAST_WINDOW);
    }

    @Test
    void unknownTimeIsTreatedAsInWindow() {
        assertThat(window(null, null, false).admit(0L, null)).isEqualTo(Admission.IN_WINDOW);
    }

    @Test
    void endsAtTheCapturedLogEnd() {
        WindowState w = window(6L, 0L, false);
        assertThat(w.admit(1_500L, pos(5, 99))).isEqualTo(Admission.IN_WINDOW);
        assertThat(w.admit(1_500L, pos(6, 0))).isEqualTo(Admission.LAST_IN_WINDOW);
        assertThat(window(6L, 0L, false).admit(1_500L, pos(6, 1))).isEqualTo(Admission.PAST_WINDOW);
    }

    @Test
    void batchAtTheLogEndCompletesOnItsLastMessage() {
        WindowState w = window(6L, 0L, false);
        assertThat(w.admit(1_500L, batchPos(6, 0, 0, 3))).isEqualTo(Admission.IN_WINDOW);
        assertThat(w.admit(1_500L, batchPos(6, 0, 1, 3))).isEqualTo(Admission.IN_WINDOW);
        assertThat(w.admit(1_500L, batchPos(6, 0, 2, 3))).isEqualTo(Admission.LAST_IN_WINDOW);
    }

    @Test
    void timeEndBeatsPositionWhenBothApply() {
        WindowState w = window(6L, 0L, false);
        assertThat(w.admit(2_500L, pos(6, 0))).isEqualTo(Admission.PAST_WINDOW);
        assertThat(w.complete("q").getEndedBy()).isEqualTo(WindowStats.EndedBy.WINDOW_END);
    }

    @Test
    void completionStatsAndDoneState() {
        WindowState w = window(6L, 0L, false);
        w.admit(1_100L, pos(5, 1));
        w.recordScanned(1_100L, 100);
        w.recordMatched();
        w.admit(1_900L, pos(6, 0));
        w.recordScanned(1_900L, 50);
        clock.addAndGet(1_234L);

        WindowStats stats = w.complete("q-1");

        assertThat(stats.getQueryId()).isEqualTo("q-1");
        assertThat(stats.getScanned()).isEqualTo(2L);
        assertThat(stats.getMatched()).isEqualTo(1L);
        assertThat(stats.getBytesScanned()).isEqualTo(150L);
        assertThat(stats.getFirstScannedMs()).isEqualTo(1_100L);
        assertThat(stats.getLastScannedMs()).isEqualTo(1_900L);
        assertThat(stats.getElapsedMs()).isEqualTo(1_234L);
        assertThat(stats.getEndedBy()).isEqualTo(WindowStats.EndedBy.LOG_END);
        assertThat(stats.getStatus()).isEqualTo(WindowStats.Status.COMPLETE);
        assertThat(w.isDone()).isTrue();
        assertThat(w.admit(1_500L, pos(5, 2))).isEqualTo(Admission.DONE);
    }

    @Test
    void truncatedWindowReportsIt() {
        WindowState w = window(null, null, true);
        w.admit(2_500L, null);
        assertThat(w.complete("q").getStatus()).isEqualTo(WindowStats.Status.TRUNCATED_BY_RETENTION);
    }

    @Test
    void reopenAfterAFailedCompletion() {
        WindowState w = window(null, null, false);
        w.admit(2_500L, null);
        w.complete("q");
        w.reopen();
        assertThat(w.admit(2_500L, null)).isEqualTo(Admission.PAST_WINDOW);
    }
}
