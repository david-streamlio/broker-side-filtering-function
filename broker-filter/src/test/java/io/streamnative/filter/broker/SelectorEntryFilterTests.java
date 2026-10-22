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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter.FilterResult;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.jupiter.api.Test;

class SelectorEntryFilterTests {

    private static final String TOPIC = "persistent://public/default/orders";

    private final SelectorEntryFilter filter = new SelectorEntryFilter();
    private final Entry entry = mock(Entry.class);

    private static MessageMetadata metadata(String key, String... props) {
        MessageMetadata md = new MessageMetadata()
                .setProducerName("p").setSequenceId(1).setPublishTime(1_700_000_000_000L);
        if (key != null) {
            md.setPartitionKey(key);
        }
        for (int i = 0; i < props.length; i += 2) {
            md.addProperty().setKey(props[i]).setValue(props[i + 1]);
        }
        return md;
    }

    private static FilterContext context(Map<String, String> subscriptionProps, MessageMetadata md, String topic) {
        Subscription sub = mock(Subscription.class);
        when(sub.getSubscriptionProperties()).thenReturn(subscriptionProps);
        when(sub.getTopicName()).thenReturn(topic);
        FilterContext ctx = new FilterContext();
        ctx.setSubscription(sub);
        ctx.setMsgMetadata(md);
        return ctx;
    }

    private FilterResult filter(String selector, MessageMetadata md) {
        Map<String, String> props = selector == null ? Map.of() : Map.of("selector", selector);
        return filter.filterEntry(entry, context(props, md, TOPIC));
    }

    @Test
    void subscriptionsWithoutSelectorAcceptEverything() {
        assertThat(filter(null, metadata("8675309", "region", "r07"))).isEqualTo(FilterResult.ACCEPT);
        assertThat(filter.filterEntry(entry, context(null, metadata("k"), TOPIC))).isEqualTo(FilterResult.ACCEPT);
    }

    @Test
    void keySelector() {
        assertThat(filter("__key__ = '8675309'", metadata("8675309"))).isEqualTo(FilterResult.ACCEPT);
        assertThat(filter("__key__ = '8675309'", metadata("10000001"))).isEqualTo(FilterResult.REJECT);
        assertThat(filter("__key__ = '8675309'", metadata(null))).isEqualTo(FilterResult.REJECT);
    }

    @Test
    void base64EncodedKeyIsDecoded() {
        String b64 = Base64.getEncoder().encodeToString("8675309".getBytes(StandardCharsets.UTF_8));
        MessageMetadata md = metadata(b64).setPartitionKeyB64Encoded(true);
        assertThat(filter("__key__ = '8675309'", md)).isEqualTo(FilterResult.ACCEPT);
    }

    @Test
    void propertySelectors() {
        MessageMetadata md = metadata("1", "region", "r07", "event_type", "shipped");
        assertThat(filter("region = 'r07'", md)).isEqualTo(FilterResult.ACCEPT);
        assertThat(filter("region = 'r07' AND event_type IN ('paid', 'shipped')", md)).isEqualTo(FilterResult.ACCEPT);
        assertThat(filter("region = 'r08'", md)).isEqualTo(FilterResult.REJECT);
        assertThat(filter("missing = 'x'", md)).isEqualTo(FilterResult.REJECT);
    }

    @Test
    void publishTimeIsNumeric() {
        MessageMetadata md = metadata("1");
        assertThat(filter("__publish_time__ BETWEEN 1600000000000 AND 1800000000000", md))
                .isEqualTo(FilterResult.ACCEPT);
        assertThat(filter("__publish_time__ > 1800000000000", md)).isEqualTo(FilterResult.REJECT);
    }

    @Test
    void topicBindingStripsPartitionSuffix() {
        FilterContext ctx = context(Map.of("selector", "__topic__ = '" + TOPIC + "'"), metadata("1"),
                TOPIC + "-partition-3");
        assertThat(filter.filterEntry(entry, ctx)).isEqualTo(FilterResult.ACCEPT);
        assertThat(MetadataBindings.baseTopic(TOPIC + "-partition-x")).isEqualTo(TOPIC + "-partition-x");
    }

    @Test
    void batchedEntriesPassUnfiltered() {
        MessageMetadata batch = metadata(null).setNumMessagesInBatch(10);
        assertThat(filter("region = 'r07'", batch)).isEqualTo(FilterResult.ACCEPT);
        assertThat(filter.summary()).contains("batchedUnfiltered=1").contains("evaluated=0");
    }

    @Test
    void invalidSelectorFailsClosed() {
        assertThat(filter("region = = 'r07'", metadata("1", "region", "r07"))).isEqualTo(FilterResult.REJECT);
        assertThat(filter.summary()).contains("invalidSelector=1");
    }

    @Test
    void countersTrackDecisions() {
        filter("region = 'r07'", metadata("1", "region", "r07"));
        filter("region = 'r07'", metadata("2", "region", "r08"));
        assertThat(filter.summary()).contains("evaluated=2").contains("accepted=1").contains("rejected=1")
                .contains("selectors=1");
    }
}
