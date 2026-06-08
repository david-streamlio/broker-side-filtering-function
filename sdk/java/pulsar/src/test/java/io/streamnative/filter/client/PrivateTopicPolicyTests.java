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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PrivateTopicPolicyTests {

    @Test
    void defaultsAreAggressive() {
        PrivateTopicPolicy p = PrivateTopicPolicy.DEFAULT;
        assertThat(p.getMessageTtlSeconds()).isEqualTo(300);
        assertThat(p.getRetentionTimeMinutes()).isEqualTo(5);
        assertThat(p.getRetentionSizeMB()).isEqualTo(64);
        assertThat(p.getInactiveDeleteSeconds()).isEqualTo(600);
        assertThat(p.isDeleteOnClose()).isTrue();
        assertThat(p.isApplyPerTopicPolicies()).isTrue();
    }

    @Test
    void builderOverridesEachField() {
        PrivateTopicPolicy p = PrivateTopicPolicy.builder()
                .messageTtlSeconds(60)
                .retentionTimeMinutes(1)
                .retentionSizeMB(8)
                .inactiveDeleteSeconds(120)
                .deleteOnClose(false)
                .applyPerTopicPolicies(false)
                .build();
        assertThat(p.getMessageTtlSeconds()).isEqualTo(60);
        assertThat(p.getRetentionTimeMinutes()).isEqualTo(1);
        assertThat(p.getRetentionSizeMB()).isEqualTo(8);
        assertThat(p.getInactiveDeleteSeconds()).isEqualTo(120);
        assertThat(p.isDeleteOnClose()).isFalse();
        assertThat(p.isApplyPerTopicPolicies()).isFalse();
    }

    @Test
    void toBuilderRoundTrips() {
        PrivateTopicPolicy orig = PrivateTopicPolicy.builder()
                .messageTtlSeconds(42)
                .retentionTimeMinutes(7)
                .build();
        PrivateTopicPolicy copy = orig.toBuilder().build();
        assertThat(copy).isEqualTo(orig);
    }
}
