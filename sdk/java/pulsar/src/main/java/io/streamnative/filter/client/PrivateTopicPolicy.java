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

import java.util.Objects;

/**
 * Settings applied to the per-consumer private output topic. Defaults are aggressive — private
 * topics are expected to be short-lived. Use the builder to override individual fields.
 *
 * <p>The defaults are designed to prevent topic explosion even if a client crashes or forgets
 * to call {@code close()} on the consumer returned from
 * {@link FilterableConsumerBuilder#subscribe()}:
 * <ul>
 *   <li>Messages older than {@code messageTtlSeconds} are deleted from the topic backlog.</li>
 *   <li>The compaction/retention policy caps backlog age and size.</li>
 *   <li>Inactive-topic policy deletes the topic if no subscriptions exist for
 *       {@code inactiveDeleteSeconds}.</li>
 *   <li>{@code deleteOnClose} controls whether closing the consumer actively deletes the
 *       topic. Even with {@code deleteOnClose=false}, the inactive-topic policy will
 *       eventually reap the topic.</li>
 * </ul>
 */
public final class PrivateTopicPolicy {

    public static final PrivateTopicPolicy DEFAULT = new Builder().build();

    /**
     * Default for windowed queries' private topics. A window's results can be large and are read
     * once from the start, so the live defaults (5-minute TTL, 64 MB retention) could expire results
     * a slow consumer hasn't read yet: a silent gap. One hour TTL and retention, no size cap.
     */
    public static final PrivateTopicPolicy WINDOWED_DEFAULT = new Builder()
            .messageTtlSeconds(3_600)
            .retentionTimeMinutes(60)
            .retentionSizeMB(-1)
            .build();

    private final int messageTtlSeconds;
    private final int retentionTimeMinutes;
    private final int retentionSizeMB;
    private final int inactiveDeleteSeconds;
    private final boolean deleteOnClose;
    private final boolean applyPerTopicPolicies;

    private PrivateTopicPolicy(Builder b) {
        this.messageTtlSeconds      = b.messageTtlSeconds;
        this.retentionTimeMinutes   = b.retentionTimeMinutes;
        this.retentionSizeMB        = b.retentionSizeMB;
        this.inactiveDeleteSeconds  = b.inactiveDeleteSeconds;
        this.deleteOnClose          = b.deleteOnClose;
        this.applyPerTopicPolicies  = b.applyPerTopicPolicies;
    }

    public int getMessageTtlSeconds()      { return messageTtlSeconds; }
    public int getRetentionTimeMinutes()   { return retentionTimeMinutes; }
    public int getRetentionSizeMB()        { return retentionSizeMB; }
    public int getInactiveDeleteSeconds()  { return inactiveDeleteSeconds; }
    public boolean isDeleteOnClose()       { return deleteOnClose; }
    public boolean isApplyPerTopicPolicies() { return applyPerTopicPolicies; }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .messageTtlSeconds(messageTtlSeconds)
                .retentionTimeMinutes(retentionTimeMinutes)
                .retentionSizeMB(retentionSizeMB)
                .inactiveDeleteSeconds(inactiveDeleteSeconds)
                .deleteOnClose(deleteOnClose)
                .applyPerTopicPolicies(applyPerTopicPolicies);
    }

    public static final class Builder {
        private int messageTtlSeconds     = 300;   // 5 min
        private int retentionTimeMinutes  = 5;     // 5 min
        private int retentionSizeMB       = 64;    // 64 MB
        private int inactiveDeleteSeconds = 600;   // 10 min
        private boolean deleteOnClose          = true;
        private boolean applyPerTopicPolicies  = true;

        public Builder messageTtlSeconds(int v)     { this.messageTtlSeconds = v; return this; }
        public Builder retentionTimeMinutes(int v)  { this.retentionTimeMinutes = v; return this; }
        public Builder retentionSizeMB(int v)       { this.retentionSizeMB = v; return this; }
        public Builder inactiveDeleteSeconds(int v) { this.inactiveDeleteSeconds = v; return this; }
        public Builder deleteOnClose(boolean v)     { this.deleteOnClose = v; return this; }
        public Builder applyPerTopicPolicies(boolean v) { this.applyPerTopicPolicies = v; return this; }

        public PrivateTopicPolicy build() {
            return new PrivateTopicPolicy(this);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof PrivateTopicPolicy that)) return false;
        return messageTtlSeconds == that.messageTtlSeconds
                && retentionTimeMinutes == that.retentionTimeMinutes
                && retentionSizeMB == that.retentionSizeMB
                && inactiveDeleteSeconds == that.inactiveDeleteSeconds
                && deleteOnClose == that.deleteOnClose
                && applyPerTopicPolicies == that.applyPerTopicPolicies;
    }

    @Override
    public int hashCode() {
        return Objects.hash(messageTtlSeconds, retentionTimeMinutes, retentionSizeMB,
                inactiveDeleteSeconds, deleteOnClose, applyPerTopicPolicies);
    }
}
