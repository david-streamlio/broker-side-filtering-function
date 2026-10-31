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

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicsTests {

    private static final String ORDERS = "persistent://public/default/orders";
    private static final String SESSIONS = "persistent://public/default/session_events";

    @Test
    void theListWinsOverTheSingleTopicAndIsTrimmedAndDeduplicated() {
        Topics topics = Topics.fromConfig(" " + ORDERS + " ,," + SESSIONS + "," + ORDERS, "persistent://x/y/ignored");

        assertThat(topics.all()).containsExactly(ORDERS, SESSIONS);
        assertThat(topics.first()).isEqualTo(ORDERS);
    }

    @Test
    void aBlankOrMissingListFallsBackToTheSingleTopic() {
        assertThat(Topics.fromConfig(null, ORDERS).all()).containsExactly(ORDERS);
        assertThat(Topics.fromConfig("  , ", ORDERS).all()).containsExactly(ORDERS);
    }

    @Test
    void aTopicResolvesByShortOrFullNameAndDefaultsToTheFirst() {
        Topics topics = Topics.of(ORDERS, SESSIONS);

        assertThat(topics.resolve("session_events")).isEqualTo(SESSIONS);
        assertThat(topics.resolve(SESSIONS)).isEqualTo(SESSIONS);
        assertThat(topics.resolve(" orders ")).isEqualTo(ORDERS);
        assertThat(topics.resolve(null)).isEqualTo(ORDERS);
        assertThat(topics.resolve("")).isEqualTo(ORDERS);
    }

    @Test
    void anUnknownTopicIsRejectedWithTheTopicsTheAgentCanUse() {
        Topics topics = Topics.of(ORDERS, SESSIONS);

        assertThatThrownBy(() -> topics.resolve("persistent://public/default/secrets"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("secrets")
                .hasMessageContaining("list_topics")
                .hasMessageContaining("orders, session_events");
        // the short name only matches the last path segment, not a suffix
        assertThatThrownBy(() -> topics.resolve("default/orders")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void describeGivesEachTopicsShortAndFullName() {
        assertThat(Topics.of(ORDERS, SESSIONS).describe()).isEqualTo(List.of(
                Map.of("name", "orders", "topic", ORDERS),
                Map.of("name", "session_events", "topic", SESSIONS)));
    }

    @Test
    void atLeastOneTopicIsRequired() {
        assertThatThrownBy(Topics::of).isInstanceOf(IllegalArgumentException.class);
    }
}
