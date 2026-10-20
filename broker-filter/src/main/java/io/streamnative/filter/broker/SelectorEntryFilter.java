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

import io.streamnative.filter.common.selector.Selector;
import io.streamnative.filter.common.selector.SelectorParseException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.plugin.EntryFilter;
import org.apache.pulsar.broker.service.plugin.FilterContext;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Broker-side selector filter: evaluates the subscription property {@value #SELECTOR_PROPERTY}
 * against each entry's {@link MessageMetadata} in the dispatch path. The payload is never read,
 * so compressed payloads are never decompressed.
 *
 * <ul>
 *   <li>No selector on the subscription: ACCEPT (every other subscription is unaffected).</li>
 *   <li>Unbatched entry: evaluate; ACCEPT on TRUE, REJECT otherwise. The broker acks rejected
 *       entries for the subscription, so they never reach the consumer.</li>
 *   <li>Batched entry: ACCEPT unfiltered and count it. Per-message metadata is inside the
 *       payload, so filtering it would require reading (and decompressing) the payload.</li>
 *   <li>Invalid selector: REJECT (fail closed) and log once per expression.</li>
 * </ul>
 *
 * <p>Kafka clients on KoP read the managed ledger directly, not through a subscription
 * dispatcher, so this filter doesn't apply to them.
 */
public class SelectorEntryFilter implements EntryFilter {

    private static final Logger LOG = LoggerFactory.getLogger(SelectorEntryFilter.class);

    public static final String SELECTOR_PROPERTY = "selector";
    private static final long SUMMARY_EVERY_MS = 10_000L;

    private final ConcurrentMap<String, Optional<Selector>> compiled = new ConcurrentHashMap<>();
    private final LongAdder evaluated = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder batchedUnfiltered = new LongAdder();
    private final LongAdder invalidSelector = new LongAdder();
    private final LongAdder evalNanos = new LongAdder();
    private final AtomicLong lastSummaryMs = new AtomicLong(System.currentTimeMillis());

    @Override
    public FilterResult filterEntry(Entry entry, FilterContext context) {
        Subscription subscription = context.getSubscription();
        String expression = selectorOf(subscription);
        if (expression == null) {
            return FilterResult.ACCEPT;
        }
        MessageMetadata metadata = context.getMsgMetadata();
        if (metadata == null || MetadataBindings.isBatched(metadata)) {
            batchedUnfiltered.increment();
            maybeLogSummary();
            return FilterResult.ACCEPT;
        }
        Optional<Selector> selector = compiled.computeIfAbsent(expression, SelectorEntryFilter::compile);
        if (selector.isEmpty()) {
            invalidSelector.increment();
            return FilterResult.REJECT;
        }

        long start = System.nanoTime();
        boolean match;
        try {
            match = selector.get().matches(new MetadataBindings(metadata, subscription.getTopicName()));
        } catch (RuntimeException e) {
            match = false;
        }
        evalNanos.add(System.nanoTime() - start);
        evaluated.increment();
        (match ? accepted : rejected).increment();
        maybeLogSummary();
        return match ? FilterResult.ACCEPT : FilterResult.REJECT;
    }

    static String selectorOf(Subscription subscription) {
        if (subscription == null) {
            return null;
        }
        Map<String, String> props = subscription.getSubscriptionProperties();
        String expression = props == null ? null : props.get(SELECTOR_PROPERTY);
        return expression == null || expression.isBlank() ? null : expression;
    }

    private static Optional<Selector> compile(String expression) {
        try {
            return Optional.of(Selector.compile(expression));
        } catch (SelectorParseException e) {
            LOG.warn("Invalid selector [{}]: {}; rejecting all entries for subscriptions using it",
                    expression, e.getMessage());
            return Optional.empty();
        }
    }

    private void maybeLogSummary() {
        long now = System.currentTimeMillis();
        long last = lastSummaryMs.get();
        if (now - last >= SUMMARY_EVERY_MS && lastSummaryMs.compareAndSet(last, now)) {
            LOG.info("[selector-entry-filter] {}", summary());
        }
    }

    /** Cumulative counters, as one log-friendly line. */
    String summary() {
        long n = evaluated.sum();
        return String.format("evaluated=%d accepted=%d rejected=%d batchedUnfiltered=%d invalidSelector=%d "
                        + "avgEvalNanos=%d selectors=%d",
                n, accepted.sum(), rejected.sum(), batchedUnfiltered.sum(), invalidSelector.sum(),
                n == 0 ? 0 : evalNanos.sum() / n, compiled.size());
    }

    @Override
    public void close() {
        LOG.info("[selector-entry-filter] closing: {}", summary());
    }
}
