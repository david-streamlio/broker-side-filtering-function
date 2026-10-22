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

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Creates a live filter's subscription at the current end of its input topics (or of every topic
 * matching its pattern), so a filter attached to it starts when the query is registered, not at the
 * start of the topics' retained history.
 *
 * <p>Uses the function runtime's {@link PulsarClient} (a paused consumer at {@code Latest}, closed
 * straight away), like {@link ClientWindowPreparer}: admin calls that serialize NAR-only beans fail in
 * the Functions runtime. A subscription that already exists is left where it is.
 */
final class ClientSubscriptionPositioner implements SubscriptionPositioner {

    private static final Logger LOG = LoggerFactory.getLogger(ClientSubscriptionPositioner.class);

    private final PulsarClient client;

    ClientSubscriptionPositioner(PulsarClient client) {
        this.client = client;
    }

    @Override
    public void createAtEnd(List<String> topics, String topicsPattern, String subscription) throws Exception {
        ConsumerBuilder<byte[]> builder = client.newConsumer(Schema.BYTES)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .startPaused(true);
        builder = topicsPattern != null ? builder.topicsPattern(topicsPattern) : builder.topics(topics);
        builder.subscribe().close();
        LOG.info("Created subscription {} at the end of {}", subscription,
                topicsPattern != null ? topicsPattern : topics);
    }
}
