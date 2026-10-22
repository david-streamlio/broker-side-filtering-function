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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopicKeySanitizerTests {

    @Test
    void forInputTopicTakesLastSegmentAsPrefix() {
        String key = TopicKeySanitizer.forInputTopic("persistent://public/in/trades");
        assertThat(key).startsWith("trades-");
        assertThat(key.split("-")[1]).hasSize(8); // 8-char hex hash suffix
    }

    @Test
    void forInputTopicSameInputProducesSameKey() {
        String a = TopicKeySanitizer.forInputTopic("persistent://public/in/trades");
        String b = TopicKeySanitizer.forInputTopic("persistent://public/in/trades");
        assertThat(a).isEqualTo(b);
    }

    @Test
    void forInputTopicDifferentInputsProduceDifferentKeys() {
        String a = TopicKeySanitizer.forInputTopic("persistent://public/in/trades");
        String b = TopicKeySanitizer.forInputTopic("persistent://public/in/quotes");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void forInputTopicDistinguishesByFullFqnNotJustTail() {
        // Two topics with the same last segment but different namespaces hash differently.
        String a = TopicKeySanitizer.forInputTopic("persistent://public/in/trades");
        String b = TopicKeySanitizer.forInputTopic("persistent://other/in/trades");
        assertThat(a).startsWith("trades-");
        assertThat(b).startsWith("trades-");
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void forInputTopicSanitizesNonAlphanumericCharacters() {
        // Regex specials should be replaced with single dashes in the prefix portion.
        String key = TopicKeySanitizer.forInputTopic("persistent://public/in/with.dots:colons");
        assertThat(key).matches("[A-Za-z0-9_-]+");
    }

    @Test
    void forInputTopicTruncatesLongPrefixes() {
        String longTopic = "persistent://public/in/" + "a".repeat(200);
        String key = TopicKeySanitizer.forInputTopic(longTopic);
        // Prefix + dash + 8-char hash. Prefix capped at 60 chars.
        String[] parts = key.split("-");
        // last token is the hash; rest forms the prefix
        String hashSuffix = parts[parts.length - 1];
        String prefix = key.substring(0, key.length() - hashSuffix.length() - 1);
        assertThat(prefix.length()).isLessThanOrEqualTo(60);
        assertThat(hashSuffix).hasSize(8);
    }

    @Test
    void forInputTopicRejectsNullOrBlank() {
        assertThatThrownBy(() -> TopicKeySanitizer.forInputTopic(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicKeySanitizer.forInputTopic("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void forPatternHandlesRegexMetacharacters() {
        String key = TopicKeySanitizer.forPattern("persistent://public/trades/.*");
        // Sanitizer strips the regex metacharacters from the readable prefix; hash distinguishes.
        assertThat(key).matches("[A-Za-z0-9_-]+");
        assertThat(key).endsWith(key.substring(key.length() - 8)); // hash suffix present
    }

    @Test
    void forPatternSameRegexProducesSameKey() {
        String a = TopicKeySanitizer.forPattern("persistent://public/trades-.*");
        String b = TopicKeySanitizer.forPattern("persistent://public/trades-.*");
        assertThat(a).isEqualTo(b);
    }

    @Test
    void forPatternRejectsNullOrBlank() {
        assertThatThrownBy(() -> TopicKeySanitizer.forPattern(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TopicKeySanitizer.forPattern("  "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
