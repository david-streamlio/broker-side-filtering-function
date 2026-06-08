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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopicNamesTests {

    @Test
    void stripsPartitionSuffix() {
        assertThat(TopicNames.normalize("persistent://t/n/foo-partition-3"))
                .isEqualTo("persistent://t/n/foo");
    }

    @Test
    void leavesNonPartitionedAlone() {
        assertThat(TopicNames.normalize("persistent://t/n/foo"))
                .isEqualTo("persistent://t/n/foo");
    }

    @Test
    void leavesNonNumericPartitionAlone() {
        assertThat(TopicNames.normalize("persistent://t/n/foo-partition-bar"))
                .isEqualTo("persistent://t/n/foo-partition-bar");
    }

    @Test
    void nullSafe() {
        assertThat(TopicNames.normalize(null)).isNull();
    }

    @Test
    void preservesTenantNamespacePrefix() {
        assertThat(TopicNames.normalize("persistent://tenantA/nsB/topicC-partition-0"))
                .isEqualTo("persistent://tenantA/nsB/topicC");
    }
}
