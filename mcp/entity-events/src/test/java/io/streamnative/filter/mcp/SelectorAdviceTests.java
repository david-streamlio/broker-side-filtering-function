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

import io.streamnative.filter.common.selector.Selector;
import io.streamnative.filter.common.selector.SelectorParseException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class SelectorAdviceTests {

    private static final String TOPIC = "persistent://public/default/orders";

    /** A header seq, a numeric body amount, a string body status, and a body price of numeric text. */
    private static final FieldCatalog.Fields FIELDS = FieldCatalog.build(TOPIC, Optional.empty(), List.of(
            new FieldCatalog.Sample("o-1", Map.of("seq", "7", "region", "US"),
                    "{\"amount\":12.5,\"status\":\"PAID\",\"price\":\"77351.21\",\"customer\":{\"tier\":\"GOLD\"}}"
                            .getBytes(StandardCharsets.UTF_8))));

    private static String parseError(String selector) {
        SelectorParseException e = catchThrowableOfType(() -> Selector.compile(selector), SelectorParseException.class);
        return SelectorAdvice.parseError(selector, e);
    }

    @Test
    void aParseErrorSaysWhereAndHowToWriteIt() {
        String msg = parseError("event_type = 'a' AND amount >");

        assertThat(msg).startsWith("selector isn't valid JMS selector syntax: ")
                .contains("at the end of the selector")
                .contains("single quotes").contains("'it''s'").contains("BETWEEN IS NULL");

        assertThat(parseError("event_type LIKE 3ds")).contains("LIKE requires a string pattern")
                .contains("at character 17 (right after \"event_type LIKE\")")
                .doesNotContain("(at position");
        assertThat(parseError("event_type LIKE")).contains("but the selector ends there");
    }

    @Test
    void habitsFromOtherLanguagesAreNamed() {
        assertThat(parseError("event_type = \"login\"")).contains("single quotes, not double quotes");
        assertThat(parseError("event_type == 'login'")).contains("Compare with =, not ==");
        assertThat(parseError("event_type != 'login'")).contains("<>, not !=");
        assertThat(parseError("a = 'x' && b = 'y'")).contains("AND and OR, not && and ||");
    }

    @Test
    void unknownNamesGetADidYouMean() {
        Set<String> known = FIELDS.names();

        assertThat(SelectorAdvice.suggest("STATUS", known)).contains("status");
        assertThat(SelectorAdvice.suggest("key", known)).contains("__key__");
        assertThat(SelectorAdvice.suggest("tier", known)).contains("customer.tier");
        assertThat(SelectorAdvice.suggest("ammount", known)).contains("amount");
        assertThat(SelectorAdvice.suggest("colour", known)).isEmpty();

        assertThat(SelectorAdvice.unknownFields(TOPIC, List.of("ammount", "colour"), known))
                .isEqualTo("selector names fields that orders doesn't have: ammount, colour. For ammount, did you mean "
                        + "amount? Call get_schema with topic \"orders\" for the fields you can use");
    }

    @Test
    void aHeaderComparedWithANumberIsFlagged() {
        List<String> hints = SelectorAdvice.typeHints("seq > 5 AND seq = 7 AND seq <> 3", FIELDS);

        assertThat(hints).hasSize(3);
        assertThat(hints.get(0)).startsWith("seq is a header, and header values are always strings, so seq > 5 "
                + "matches no event").contains("seq IN ('5')");
        assertThat(hints.get(1)).contains("seq = 7 is false for every event").contains("seq = '7'");
        assertThat(hints.get(2)).contains("seq <> 3 is true for every event");
        assertThat(SelectorAdvice.typeHints("seq = '7' AND region IN ('US')", FIELDS)).isEmpty();
        assertThat(SelectorAdvice.typeHints("5 < seq", FIELDS)).singleElement().asString().contains("5 < seq");
    }

    @Test
    void theHintsAgreeWithHowTheFilterEvaluates() {
        // Header "7" against numbers, under the selector engine's own rules.
        Function<String, Object> resolver = SelectorAdvice.resolver(FIELDS.samples().get(0));

        assertThat(Selector.compile("seq = 7").matches(resolver)).isFalse();
        assertThat(Selector.compile("seq <> 7").matches(resolver)).isTrue();
        assertThat(Selector.compile("seq > 5").matches(resolver)).isFalse();
        assertThat(Selector.compile("NOT (seq > 5)").matches(resolver)).isFalse(); // unknown, not false
        assertThat(Selector.compile("amount = '12.5'").matches(resolver)).isFalse();
        assertThat(Selector.compile("price = '77351.21'").matches(resolver)).isFalse();
        assertThat(Selector.compile("price = 77351.21").matches(resolver)).isTrue();
    }

    @Test
    void aNumericBodyFieldComparedWithAStringIsFlagged() {
        assertThat(SelectorAdvice.typeHints("amount = '12.5'", FIELDS)).singleElement().asString()
                .contains("amount is a number in the message value").contains("Drop the quotes: amount = 12.5.");
        assertThat(SelectorAdvice.typeHints("amount BETWEEN '1' AND '20'", FIELDS)).singleElement().asString()
                .contains("matches no event").contains("both bounds");
        assertThat(SelectorAdvice.typeHints("amount LIKE '12%'", FIELDS)).singleElement().asString()
                .contains("LIKE only matches strings");
        assertThat(SelectorAdvice.typeHints("price = '77351.21'", FIELDS)).singleElement().asString()
                .contains("numbers written as text").contains("price = 77351.21");
        assertThat(SelectorAdvice.typeHints("amount > 10 AND status = 'PAID' AND amount + 1 > '3'", FIELDS)).isEmpty();
    }

    @Test
    void namesKnownOnlyByNameGetNoHints() {
        FieldCatalog.Fields names = FieldCatalog.Fields.ofNames(TOPIC, Set.of("seq"));

        assertThat(SelectorAdvice.typeHints("seq > 5", names)).isEmpty();
    }

    @Test
    void exactValuesAndLeadingWildcardsAreSpotted() {
        assertThat(SelectorAdvice.picksExactValues("status = 'PAID'", null)).isTrue();
        assertThat(SelectorAdvice.picksExactValues("region IN ('US', 'EU')", null)).isTrue();
        assertThat(SelectorAdvice.picksExactValues("amount > 10", "o-1")).isTrue();
        assertThat(SelectorAdvice.picksExactValues("amount > 10 OR region NOT IN ('US')", null)).isFalse();
        assertThat(SelectorAdvice.leadingWildcards("status LIKE '%ID' AND region LIKE 'U%'"))
                .extracting(SelectorAdvice.Term::text).containsExactly("status LIKE '%ID'");
    }

    @Test
    void theResolverReadsLikeTheFilter() {
        String format = "{\"enc\":\"base64\"}";
        FieldCatalog.Sample s = new FieldCatalog.Sample("k", Map.of("enc", Base64.getEncoder().encodeToString(
                "café".getBytes(StandardCharsets.UTF_8)), "__ksn_internal_header_format", format, "status", "header"),
                "{\"status\":\"body\",\"n\":3,\"ok\":true,\"a\":{\"b\":\"c\"}}".getBytes(StandardCharsets.UTF_8), 42L);
        Function<String, Object> r = SelectorAdvice.resolver(s);

        assertThat(r.apply("__key__")).isEqualTo("k");
        assertThat(r.apply("__publish_time__")).isEqualTo(42L);
        assertThat(r.apply("enc")).isEqualTo("café");
        assertThat(r.apply("__ksn_internal_header_format")).isNull();
        assertThat(r.apply("status")).isEqualTo("header"); // properties shadow the value
        assertThat(r.apply("n")).isEqualTo(3);
        assertThat(r.apply("ok")).isEqualTo(true);
        assertThat(r.apply("a.b")).isEqualTo("c");
        assertThat(r.apply("missing")).isNull();
    }
}
