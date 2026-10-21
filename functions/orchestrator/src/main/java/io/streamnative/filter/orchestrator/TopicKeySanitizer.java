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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Produces stable, length-bounded keys derived from Pulsar topic FQNs or regex patterns. Used to
 * name per-input-topic filter function instances and their control topics.
 *
 * <p>Format: a sanitized human-readable prefix (alnum + dash, truncated) followed by a short
 * SHA-256 hash suffix to guarantee uniqueness even when the prefix collides after truncation.
 *
 * <pre>
 *   forInputTopic("persistent://public/in/trades")
 *     → "trades-a3f9b1c2"
 *   forPattern("persistent://public/.*-trades")
 *     → "trades-7d34e8a1"
 *   forInputTopic("persistent://public/very/deeply/nested/path/with/segments/that/exceed/the/limit")
 *     → "segments-that-exceed-the-limit-<hash>"   (truncated readable portion)
 * </pre>
 */
final class TopicKeySanitizer {

    /** Maximum length of the readable prefix portion (before the hash suffix). */
    private static final int MAX_PREFIX_LEN = 60;

    /** Hash suffix length in hex chars (8 chars = 32 bits of entropy — collision-safe for 10k+ entries). */
    private static final int HASH_HEX_LEN = 8;

    private TopicKeySanitizer() {}

    /**
     * Stable key for a fully-qualified Pulsar topic FQN. Uses the last segment (topic name) as the
     * readable prefix; appends a hash of the full FQN for uniqueness.
     */
    static String forInputTopic(String topicFqn) {
        if (topicFqn == null || topicFqn.isBlank()) {
            throw new IllegalArgumentException("topicFqn must not be null or blank");
        }
        int slash = topicFqn.lastIndexOf('/');
        String prefix = (slash >= 0) ? topicFqn.substring(slash + 1) : topicFqn;
        return sanitize(prefix) + "-" + hashSuffix(topicFqn);
    }

    /**
     * Stable key for a topicsPattern regex. Strips non-alphanumeric characters from the pattern
     * for the readable prefix; appends a hash of the full pattern for uniqueness.
     */
    static String forPattern(String regex) {
        if (regex == null || regex.isBlank()) {
            throw new IllegalArgumentException("regex must not be null or blank");
        }
        return sanitize(regex) + "-" + hashSuffix(regex);
    }

    /**
     * Short, stable key for a windowed query's dedicated filter function and control topic: the first
     * 8 readable characters of the queryId plus a hash of the whole queryId, so function names stay
     * well under typical 63-character resource-name limits.
     */
    static String forWindowedQuery(String queryId) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId must not be null or blank");
        }
        String readable = sanitize(queryId);
        String prefix = readable.isEmpty() ? "q" : readable.substring(0, Math.min(8, readable.length()));
        return prefix + "-" + hashSuffix(queryId);
    }

    private static String sanitize(String raw) {
        // Allow alnum, dash, underscore. Replace runs of anything else with a single dash.
        // Trim leading/trailing dashes. Truncate to MAX_PREFIX_LEN.
        String cleaned = raw.replaceAll("[^A-Za-z0-9_-]+", "-")
                            .replaceAll("^-+|-+$", "");
        if (cleaned.length() > MAX_PREFIX_LEN) {
            // Take the tail — typically the most distinguishing portion of a topic name.
            cleaned = cleaned.substring(cleaned.length() - MAX_PREFIX_LEN);
            cleaned = cleaned.replaceAll("^-+", ""); // trim any leading dash after truncation
        }
        return cleaned.isEmpty() ? "k" : cleaned;
    }

    private static String hashSuffix(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(HASH_HEX_LEN);
            for (int i = 0; i < HASH_HEX_LEN / 2; i++) {
                sb.append(String.format("%02x", digest[i] & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
