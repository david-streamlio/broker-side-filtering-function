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

package io.streamnative.filter.broker;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.function.Function;
import org.apache.pulsar.common.api.proto.KeyValue;
import org.apache.pulsar.common.api.proto.MessageMetadata;

/**
 * Selector bindings resolved from an entry's {@link MessageMetadata} only: the part of a stored
 * entry the broker parses anyway, which sits outside the (possibly compressed) payload.
 *
 * <ul>
 *   <li>{@code __key__}: the partition key, base64-decoded when the producer flagged it (KoP's
 *       {@code pulsar_non_batched} format does).</li>
 *   <li>{@code __topic__}: the subscription's topic, with any {@code -partition-N} suffix removed,
 *       matching the function's binding.</li>
 *   <li>{@code __publish_time__} / {@code __event_time__}: epoch ms as {@code Long}.</li>
 *   <li>Anything else: a message property ({@code String}), or {@code null} (UNKNOWN) when absent.
 *       Value fields can't be bound here; that would need the payload.</li>
 * </ul>
 *
 * <p>For a batched entry this metadata describes the batch, not its messages: per-message keys
 * and properties are inside the payload. Callers must not evaluate selectors against it.
 */
final class MetadataBindings implements Function<String, Object> {

    static final String KEY = "__key__";
    static final String TOPIC = "__topic__";
    static final String PUBLISH_TIME = "__publish_time__";
    static final String EVENT_TIME = "__event_time__";

    private final MessageMetadata metadata;
    private final String topic;

    MetadataBindings(MessageMetadata metadata, String topic) {
        this.metadata = metadata;
        this.topic = topic;
    }

    static boolean isBatched(MessageMetadata metadata) {
        return metadata.hasNumMessagesInBatch();
    }

    @Override
    public Object apply(String name) {
        switch (name) {
            case KEY:
                return key();
            case TOPIC:
                return baseTopic(topic);
            case PUBLISH_TIME:
                return metadata.getPublishTime();
            case EVENT_TIME:
                return metadata.hasEventTime() ? metadata.getEventTime() : null;
            default:
                for (int i = 0; i < metadata.getPropertiesCount(); i++) {
                    KeyValue kv = metadata.getPropertyAt(i);
                    if (name.equals(kv.getKey())) {
                        return kv.getValue();
                    }
                }
                return null;
        }
    }

    private String key() {
        if (!metadata.hasPartitionKey()) {
            return null;
        }
        String key = metadata.getPartitionKey();
        if (metadata.isPartitionKeyB64Encoded()) {
            try {
                return new String(Base64.getDecoder().decode(key), StandardCharsets.UTF_8);
            } catch (IllegalArgumentException e) {
                return key;
            }
        }
        return key;
    }

    static String baseTopic(String topic) {
        if (topic == null) {
            return null;
        }
        int idx = topic.lastIndexOf("-partition-");
        if (idx > 0 && topic.substring(idx + "-partition-".length()).chars().allMatch(Character::isDigit)) {
            return topic.substring(0, idx);
        }
        return topic;
    }
}
