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

package io.streamnative.filter.api;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TimeWindowTests {

    private static final long NOW = 1_800_000_000_000L;
    private static final long HOUR = 3_600_000L;

    @Test
    void boundedWindows() {
        assertThat(TimeWindow.all().resolve(NOW)).isEqualTo(new TimeWindow.Bounds(0L, NOW));
        assertThat(TimeWindow.last(1, TimeUnit.HOURS).resolve(NOW)).isEqualTo(new TimeWindow.Bounds(NOW - HOUR, NOW));
        assertThat(TimeWindow.last(Duration.ofHours(1)).resolve(NOW)).isEqualTo(new TimeWindow.Bounds(NOW - HOUR, NOW));
        assertThat(TimeWindow.between(Instant.ofEpochMilli(1_000), Instant.ofEpochMilli(2_000)).resolve(NOW))
                .isEqualTo(new TimeWindow.Bounds(1_000L, 2_000L));
        assertThat(TimeWindow.last(1, TimeUnit.HOURS).isOpenEnded()).isFalse();
        assertThat(TimeWindow.all().isOpenEnded()).isFalse();
    }

    @Test
    void openEndedWindows() {
        assertThat(TimeWindow.allThenLive().resolve(NOW)).isEqualTo(new TimeWindow.Bounds(0L, null));
        assertThat(TimeWindow.since(30, TimeUnit.MINUTES).resolve(NOW))
                .isEqualTo(new TimeWindow.Bounds(NOW - HOUR / 2, null));
        assertThat(TimeWindow.since(Duration.ofMinutes(30)).resolve(NOW))
                .isEqualTo(new TimeWindow.Bounds(NOW - HOUR / 2, null));
        assertThat(TimeWindow.since(Instant.ofEpochMilli(5_000)).resolve(NOW)).isEqualTo(new TimeWindow.Bounds(5_000L, null));
        assertThat(TimeWindow.since(1, TimeUnit.HOURS).isOpenEnded()).isTrue();
        assertThat(TimeWindow.allThenLive().isOpenEnded()).isTrue();
    }

    @Test
    void liveOnlyHasNoHistory() {
        TimeWindow live = TimeWindow.liveOnly();
        assertThat(live.isLiveOnly()).isTrue();
        assertThat(live.isOpenEnded()).isTrue();
        assertThatThrownBy(() -> live.resolve(NOW)).isInstanceOf(IllegalStateException.class);
        assertThat(TimeWindow.since(1, TimeUnit.HOURS).isLiveOnly()).isFalse();
    }

    @Test
    void relativeWindowsResolveAtSubscribeTime() {
        TimeWindow lastHour = TimeWindow.last(1, TimeUnit.HOURS);
        assertThat(lastHour.resolve(NOW).fromMs()).isEqualTo(NOW - HOUR);
        assertThat(lastHour.resolve(NOW + HOUR).fromMs()).isEqualTo(NOW);
    }

    @Test
    void lookbackBeforeTheEpochIsClampedSoItIsNotFromTheBeginning() {
        // fromMs = 0 means "from the beginning" (never truncated); a huge lookback must not turn into it.
        assertThat(TimeWindow.last(100_000, TimeUnit.DAYS).resolve(NOW).fromMs()).isEqualTo(1L);
        assertThat(TimeWindow.since(100_000, TimeUnit.DAYS).resolve(NOW).fromMs()).isEqualTo(1L);
    }

    @Test
    void invalidWindowsAreRejected() {
        assertThatThrownBy(() -> TimeWindow.last(0, TimeUnit.HOURS)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimeWindow.since(-1, TimeUnit.MINUTES)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimeWindow.last(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimeWindow.last(1, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> TimeWindow.between(Instant.ofEpochMilli(2_000), Instant.ofEpochMilli(2_000)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TimeWindow.since(Instant.ofEpochMilli(-1))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void equalityAndReadableToString() {
        assertThat(TimeWindow.last(1, TimeUnit.HOURS)).isEqualTo(TimeWindow.last(Duration.ofHours(1)));
        assertThat(TimeWindow.last(1, TimeUnit.HOURS)).isNotEqualTo(TimeWindow.since(1, TimeUnit.HOURS));
        assertThat(TimeWindow.last(1, TimeUnit.HOURS)).hasToString("TimeWindow.last(PT1H)");
        assertThat(TimeWindow.liveOnly()).hasToString("TimeWindow.liveOnly()");
    }
}
