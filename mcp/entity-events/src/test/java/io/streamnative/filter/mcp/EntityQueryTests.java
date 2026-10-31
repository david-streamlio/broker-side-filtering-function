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

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityQueryTests {

    private static final EntityQuery.Limits LIMITS =
            new EntityQuery.Limits(Duration.ofHours(24), Duration.ofSeconds(60));

    private static final String SESSIONS = "persistent://public/default/session_events";
    private static final String ORDERS = "persistent://public/default/orders";
    private static final Topics TOPICS = Topics.of(SESSIONS, ORDERS);
    private static final Set<String> SESSION_FIELDS = Set.of("__key__", "__publish_time__", "event_type", "amount");

    /** Field names per topic, recording which topics were asked about. */
    private final List<String> asked = new ArrayList<>();
    private final Function<String, Set<String>> fields = topic -> {
        asked.add(topic);
        return topic.equals(SESSIONS) ? SESSION_FIELDS : Set.of("__key__", "status");
    };

    private EntityQuery parse(Map<String, Object> args) {
        return EntityQuery.parse(args, LIMITS, TOPICS, fields);
    }

    private static Map<String, Object> args(Object... kv) {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    void defaultsToTheLastHourAndAThousandEvents() {
        EntityQuery q = EntityQuery.parse(args("key", "8675309"), LIMITS);

        assertThat(q).isEqualTo(new EntityQuery("8675309", Duration.ofHours(1), 1000));
        assertThat(q.filter()).isEqualTo("__key__ = '8675309'");
        assertThat(q.selector()).isNull();
    }

    @Test
    void keysThatCouldChangeTheSelectorAreRejected() {
        for (String key : new String[] {"8675309' OR __key__ LIKE '%", "a b", "", "x".repeat(129), "ä"}) {
            assertThatThrownBy(() -> EntityQuery.parse(args("key", key), LIMITS))
                    .as(key).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("key must be");
        }
        assertThatThrownBy(() -> EntityQuery.parse(args(), LIMITS)).hasMessageContaining("got nothing");
        assertThatThrownBy(() -> EntityQuery.parse(args("key", 8675309), LIMITS)).hasMessageContaining("key must be");
    }

    @Test
    void lookbackUnits() {
        assertThat(EntityQuery.parseDuration("90s")).isEqualTo(Duration.ofSeconds(90));
        assertThat(EntityQuery.parseDuration("15m")).isEqualTo(Duration.ofMinutes(15));
        assertThat(EntityQuery.parseDuration(" 6H ")).isEqualTo(Duration.ofHours(6));
        assertThat(EntityQuery.parseDuration("1d")).isEqualTo(Duration.ofDays(1));
        for (String bad : new String[] {"0m", "15", "m", "1.5h", "-1h", "an hour"}) {
            assertThatThrownBy(() -> EntityQuery.parseDuration(bad)).as(bad).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aLookbackBeyondTheServerMaximumIsRejectedNotShortened() {
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "lookback", "25h"), LIMITS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("longer than this server allows (24h)");
        assertThat(EntityQuery.parse(args("key", "1", "lookback", "24h"), LIMITS).lookback()).isEqualTo(Duration.ofHours(24));
    }

    @Test
    void theDefaultLookbackNeverExceedsTheMaximum() {
        EntityQuery.Limits tight = new EntityQuery.Limits(Duration.ofMinutes(15), Duration.ofSeconds(60));

        assertThat(EntityQuery.parse(args("key", "1"), tight).lookback()).isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void maxEventsDefaultsToAThousandAndTheAgentCanAskForMoreOrFewer() {
        assertThat(EntityQuery.parse(args("key", "1"), LIMITS).maxEvents()).isEqualTo(1000);
        assertThat(EntityQuery.parse(args("key", "1", "max_events", 5000), LIMITS).maxEvents()).isEqualTo(5000);
        assertThat(EntityQuery.parse(args("key", "1", "max_events", "7"), LIMITS).maxEvents()).isEqualTo(7);
        assertThat(EntityQuery.parse(args("key", "1", "max_events", 12.0), LIMITS).maxEvents()).isEqualTo(12);
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "max_events", 0), LIMITS))
                .hasMessageContaining("at least 1");
        assertThatThrownBy(() -> EntityQuery.parse(args("key", "1", "max_events", "lots"), LIMITS))
                .hasMessageContaining("whole number");
    }

    @Test
    void formatsDurationsTheWayTheyAreWritten() {
        assertThat(EntityQuery.format(Duration.ofHours(24))).isEqualTo("24h");
        assertThat(EntityQuery.format(Duration.ofDays(3))).isEqualTo("3d");
        assertThat(EntityQuery.format(Duration.ofHours(6))).isEqualTo("6h");
        assertThat(EntityQuery.format(Duration.ofMinutes(15))).isEqualTo("15m");
        assertThat(EntityQuery.format(Duration.ofSeconds(90))).isEqualTo("90s");
    }

    @Test
    void aSelectorAloneIsTheFilter() {
        EntityQuery q = parse(args("selector", "  event_type LIKE '3ds_%'  "));

        assertThat(q.topic()).isEqualTo(SESSIONS);
        assertThat(q.key()).isNull();
        assertThat(q.selector()).isEqualTo("event_type LIKE '3ds_%'");
        assertThat(q.filter()).isEqualTo("event_type LIKE '3ds_%'");
        assertThat(asked).containsExactly(SESSIONS);
    }

    @Test
    void aKeyAndASelectorMustBothMatch() {
        EntityQuery q = parse(args("key", "8675309", "selector", "event_type = 'a' OR amount > 5"));

        // Parenthesised, so the selector's OR can't widen the match beyond the key.
        assertThat(q.filter()).isEqualTo("__key__ = '8675309' AND (event_type = 'a' OR amount > 5)");
    }

    @Test
    void aKeyAloneNeverAsksForTheTopicsFields() {
        EntityQuery q = parse(args("key", "8675309", "topic", "orders"));

        assertThat(q).isEqualTo(new EntityQuery(ORDERS, "8675309", null, Duration.ofHours(1), 1000));
        assertThat(q.filter()).isEqualTo("__key__ = '8675309'");
        assertThat(asked).isEmpty();
    }

    @Test
    void neitherKeyNorSelectorIsRejected() {
        assertThatThrownBy(() -> parse(args("lookback", "15m")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("give a key, a selector, or both");
        assertThatThrownBy(() -> parse(null)).hasMessageContaining("give a key, a selector, or both");
    }

    @Test
    void aGivenKeyIsStillValidated() {
        assertThatThrownBy(() -> parse(args("key", "x' OR '1'='1", "selector", "amount > 1")))
                .hasMessageContaining("key must be");
    }

    @Test
    void badSelectorSyntaxIsRejectedWithAnExample() {
        assertThatThrownBy(() -> parse(args("selector", "event_type LIKE")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("isn't valid JMS selector syntax")
                .hasMessageContaining("event_type LIKE '3ds_%'");
        assertThat(asked).isEmpty();
    }

    @Test
    void aBlankOrNonStringSelectorIsRejected() {
        assertThatThrownBy(() -> parse(args("selector", "   "))).hasMessageContaining("non-empty string");
        assertThatThrownBy(() -> parse(args("selector", 7))).hasMessageContaining("non-empty string");
    }

    @Test
    void aTooLongSelectorIsRejectedBeforeParsing() {
        String longSelector = "amount > 1" + " OR amount > 1".repeat(100);

        assertThatThrownBy(() -> parse(args("selector", longSelector)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("the most this server accepts is 1000");
        assertThat(asked).isEmpty();
    }

    @Test
    void anUnknownFieldIsNamedAndPointsAtGetSchema() {
        assertThatThrownBy(() -> parse(args("selector", "event_typ = 'a' AND amount > 1 AND colour = 'red'")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("event_typ, colour")
                .hasMessageNotContaining("amount")
                .hasMessageContaining("get_schema with topic \"session_events\"");
    }

    @Test
    void fieldsAreCheckedAgainstTheTopicAsked() {
        assertThat(parse(args("topic", "orders", "selector", "status = 'shipped'")).topic()).isEqualTo(ORDERS);
        assertThatThrownBy(() -> parse(args("topic", "orders", "selector", "event_type = 'a'")))
                .hasMessageContaining("orders doesn't have: event_type");
        assertThat(asked).containsExactly(ORDERS, ORDERS);
    }

    @Test
    void aFailureToReadTheTopicsFieldsPassesThrough() {
        Function<String, Set<String>> broken = t -> {
            throw new IllegalStateException("couldn't read the fields of session_events");
        };

        assertThatThrownBy(() -> EntityQuery.parse(args("selector", "amount > 1"), LIMITS, TOPICS, broken))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void topicsResolveByShortOrFullNameAndDefaultToTheFirst() {
        assertThat(parse(args("key", "1")).topic()).isEqualTo(SESSIONS);
        assertThat(parse(args("key", "1", "topic", "orders")).topic()).isEqualTo(ORDERS);
        assertThat(parse(args("key", "1", "topic", ORDERS)).topic()).isEqualTo(ORDERS);
        assertThatThrownBy(() -> parse(args("key", "1", "topic", "persistent://public/default/secrets")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("isn't one this server reads");
    }
    @Test
    void anUnknownFieldGetsADidYouMean() {
        assertThatThrownBy(() -> parse(args("selector", "Event_Type = 'a' AND amout > 1")))
                .hasMessageContaining("For Event_Type, did you mean event_type?")
                .hasMessageContaining("For amout, did you mean amount?");
    }

    @Test
    void aSyntaxErrorSaysWhereItIs() {
        assertThatThrownBy(() -> parse(args("selector", "event_type = 'a' AND")))
                .hasMessageContaining("at the end of the selector")
                .hasMessageContaining("two single quotes for a quote inside one");
    }

    @Test
    void comparisonsThatCantMatchAreWarningsNotErrors() {
        FieldCatalog.Fields sessionFields = new FieldCatalog.Fields(SESSIONS, List.of(
                new FieldCatalog.Field("__key__", "key", "string", List.of()),
                new FieldCatalog.Field("seq", "header", "string", List.of("1")),
                new FieldCatalog.Field("amount", "body", "number", List.of("5"))), 1, "sampled JSON values");

        EntityQuery q = EntityQuery.parseAgainst(args("selector", "seq > 5 AND amount = '5'"), LIMITS, TOPICS,
                t -> sessionFields);

        assertThat(q.selector()).isEqualTo("seq > 5 AND amount = '5'");
        assertThat(q.selectorWarnings()).hasSize(2);
        assertThat(q.selectorWarnings().get(0)).contains("seq is a header").contains("matches no event");
        assertThat(q.selectorWarnings().get(1)).contains("amount = '5' is false for every event");
        // Names alone carry no types, so nothing is inferred.
        assertThat(parse(args("selector", "event_type > 5")).selectorWarnings()).isEmpty();
    }

    @Test
    void sqlHabitsAreNormalizedBeforeTheChecksAndTheRewriteIsReported() {
        EntityQuery.CheckedSelector checked = EntityQuery.checkSelector("\"event_type\" != 'login'", "t",
                t -> FieldCatalog.Fields.ofNames(t, java.util.Set.of("__key__", "event_type")));

        assertThat(checked.text()).isEqualTo("event_type <> 'login'");
        assertThat(checked.warnings()).anySatisfy(w -> assertThat(w).contains("ran as: event_type <> 'login'"));
    }
}
