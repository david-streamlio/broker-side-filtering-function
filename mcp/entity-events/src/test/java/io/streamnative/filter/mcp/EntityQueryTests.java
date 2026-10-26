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

package io.streamnative.filter.mcp;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityQueryTests {

    private static final EntityQuery.Limits LIMITS =
            new EntityQuery.Limits(Duration.ofHours(24), 200, Duration.ofSeconds(60));

    private static Map<String, Object> args(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void defaultsToTheLastHourAndFiftyEvents() {
        EntityQuery q = EntityQuery.parse(args("key", "8675309"), LIMITS);

        assertThat(q).isEqualTo(new EntityQuery("8675309", Duration.ofHours(1), 50));
        assertThat(q.selector()).isEqualTo("__key__ = '8675309'");
    }

    @Test
    void keysThatCouldChangeTheSelectorAreRejected() {
        for (String key : new String[] {"8675309' OR __key__ LIKE '%", "a b", "", "x".repeat(129), "ä"}) {
            assertThatThrownBy(() -> EntityQuery.parse(args("key", key), LIMITS))
                    .as(key).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key must be");
        }
        assertThatThrownBy(() -> EntityQuery.parse(args(), LIMITS)).hasMessageContaining("got nothing");
        assertThatThrownBy(() -> EntityQuery.parse(args("key", 8675309), LIMITS)).hasMessageContaining("key must be");
    }

    @Test
    void lookbackUnits() {
        assertThat(EntityQuery.parseDuration("90s")).isEqualTo(Duration.ofSeconds(90));
        assertThat(EntityQuery.parseDuration("15m")).isEqualTo(Duration.ofMinutes(15));
        assertThat(EntityQuery.parseDuration(" 6H ")).isEqualTo(Duration.ofHours(6));
        assertThat(EntityQuery.parseDuration("1d")).isEqualTo(Duration.ofDays(1));
        for (String bad : new String[] {"0m", "15", "m", "1.5h", "-1h", "an hour"}) {
            assertThatThrownBy(() -> EntityQuery.parseDuration(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aLookbackBeyondTheServerMaximumIsRejectedNotShortened() {
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "lookback", "25h"), LIMITS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("longer than this server allows (24h)");
        assertThat(EntityQuery.parse(args("key", "1", "lookback", "24h"), LIMITS).lookback()).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void theDefaultLookbackNeverExceedsTheMaximum() {
        EntityQuery.Limits tight = new EntityQuery.Limits(Duration.ofMinutes(15), 200, Duration.ofSeconds(60));

        assertThat(EntityQuery.parse(args("key", "1"), tight).lookback()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void maxEventsIsLoweredToTheServerMaximum() {
        assertThat(EntityQuery.parse(args("key", "1", "max_events", 500), LIMITS).maxEvents()).isEqualTo(200);
        assertThat(EntityQuery.parse(args("key", "1", "max_events", "7"), LIMITS).maxEvents()).isEqualTo(7);
        assertThat(EntityQuery.parse(args("key", "1", "max_events", 12.0), LIMITS).maxEvents()).isEqualTo(12);
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "max_events", 0), LIMITS))
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "max_events", "lots"), LIMITS))
                .hasMessageContaining("whole number");
    }

    @Test
    void formatsDurationsTheWayTheyAreWritten() {
        assertThat(EntityQuery.format(Duration.ofHours(24))).isEqualTo("24h");
        assertThat(EntityQuery.format(Duration.ofDays(3))).isEqualTo("3d");
        assertThat(EntityQuery.format(Duration.ofHours(6))).isEqualTo("6h");
        assertThat(EntityQuery.format(Duration.ofMinutes(15))).isEqualTo("15m");
        assertThat(EntityQuery.format(Duration.ofSeconds(90))).isEqualTo("90s");
    }
}
