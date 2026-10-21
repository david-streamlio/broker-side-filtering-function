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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void windowSpecValidation() {
        new WindowSpec(0, 1L, null, null, false).validate();
        new WindowSpec(10, 20L, 3L, 4L, false).validate();
        assertThatThrownBy(() -> new WindowSpec(20, 20L, null, null, false).validate())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("fromMs < toMs");
        assertThatThrownBy(() -> new WindowSpec(-1, 20L, null, null, false).validate())
                .isInstanceOf(IllegalArgumentException.class);
        new WindowSpec(10, null, 3L, 4L, false).validate();   // open-ended
        assertThat(new WindowSpec(10, null, null, null, false).isOpenEnded()).isTrue();
        assertThat(new WindowSpec(10, 20L, null, null, false).isOpenEnded()).isFalse();
        assertThatThrownBy(() -> new WindowSpec(10, 20L, 3L, null, false).validate())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("both");
    }

    @Test
    void windowStatsRoundTrip() throws Exception {
        WindowStats stats = new WindowStats("q-1", 1_000L, 2_000L, 500L, 7L, 600_000L,
                1_001L, 1_999L, 3_210L, WindowStats.EndedBy.LOG_END, WindowStats.Status.COMPLETE);

        String json = MAPPER.writeValueAsString(stats);
        assertThat(json).contains("\"endedBy\":\"LOG_END\"").contains("\"status\":\"COMPLETE\"");
        assertThat(MAPPER.readValue(json, WindowStats.class)).isEqualTo(stats);
    }

    @Test
    void openEndedSpecAndStatsOmitTheEnd() throws Exception {
        WindowSpec spec = new WindowSpec(1_000L, null, 9L, 188L, false);
        String specJson = MAPPER.writeValueAsString(spec);
        assertThat(specJson).doesNotContain("toMs").doesNotContain("openEnded");
        assertThat(MAPPER.readValue(specJson, WindowSpec.class)).isEqualTo(spec);

        WindowStats stats = new WindowStats("q-1", 1_000L, null, 10L, 1L, 100L, 1_001L, 1_002L, 3L,
                WindowStats.EndedBy.LOG_END, WindowStats.Status.COMPLETE);
        String statsJson = MAPPER.writeValueAsString(stats);
        assertThat(statsJson).doesNotContain("windowToMs");
        assertThat(MAPPER.readValue(statsJson, WindowStats.class)).isEqualTo(stats);
    }

    @Test
    void windowStatsWithNothingScannedOmitsTimes() throws Exception {
        WindowStats stats = new WindowStats("q-1", 1_000L, 2_000L, 0L, 0L, 0L, null, null, 5L,
                WindowStats.EndedBy.WINDOW_END, WindowStats.Status.TRUNCATED_BY_RETENTION);
        String json = MAPPER.writeValueAsString(stats);
        assertThat(json).doesNotContain("firstScannedMs").doesNotContain("lastScannedMs");
        assertThat(MAPPER.readValue(json, WindowStats.class)).isEqualTo(stats);
    }
}
