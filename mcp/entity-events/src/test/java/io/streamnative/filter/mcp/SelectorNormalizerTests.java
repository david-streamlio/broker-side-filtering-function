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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectorNormalizerTests {

    @Test
    void notEqualsBecomesTheJmsOperator() {
        assertThat(SelectorNormalizer.normalize("event_type != 'login'")).isEqualTo("event_type <> 'login'");
        assertThat(SelectorNormalizer.normalize("a!=1 AND b != 2")).isEqualTo("a<>1 AND b <> 2");
        assertThat(SelectorNormalizer.normalize("a <> 1")).isEqualTo("a <> 1");
    }

    @Test
    void quotedIdentifiersLoseTheirQuotes() {
        assertThat(SelectorNormalizer.normalize("\"event_type\" = 'login'")).isEqualTo("event_type = 'login'");
        assertThat(SelectorNormalizer.normalize("`region` IN ('US','UK')")).isEqualTo("region IN ('US','UK')");
        assertThat(SelectorNormalizer.normalize("\"customer.tier\" = 'GOLD'")).isEqualTo("customer.tier = 'GOLD'");
    }

    @Test
    void stringLiteralsAreLeftAloneIncludingEscapedQuotes() {
        assertThat(SelectorNormalizer.normalize("note = 'a != b'")).isEqualTo("note = 'a != b'");
        assertThat(SelectorNormalizer.normalize("note = 'say \"hi\"'")).isEqualTo("note = 'say \"hi\"'");
        assertThat(SelectorNormalizer.normalize("name = 'O''Brien' AND \"x\" != 'y'")).isEqualTo("name = 'O''Brien' AND x <> 'y'");
    }

    @Test
    void aQuotedNameNoSelectorCanExpressIsRejectedWithTheReason() {
        assertThatThrownBy(() -> SelectorNormalizer.normalize("\"x-trace\" = 'abc'"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("\"x-trace\"").hasMessageContaining("get_schema");
        assertThatThrownBy(() -> SelectorNormalizer.normalize("`and` = 1")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUnterminatedQuoteIsLeftForTheParserToReport() {
        assertThat(SelectorNormalizer.normalize("\"event_type = 'x'")).isEqualTo("\"event_type = 'x'");
        assertThat(SelectorNormalizer.normalize("a = 'open")).isEqualTo("a = 'open");
    }

    @Test
    void doubleQuotesWhereAValueGoesAreLeftForTheParsersSingleQuoteHint() {
        assertThat(SelectorNormalizer.normalize("event_type = \"login\"")).isEqualTo("event_type = \"login\"");
        assertThat(SelectorNormalizer.normalize("region IN (\"US\", \"UK\")")).isEqualTo("region IN (\"US\", \"UK\")");
        assertThat(SelectorNormalizer.normalize("note LIKE \"a%\"")).isEqualTo("note LIKE \"a%\"");
        assertThat(SelectorNormalizer.normalize("\"event_type\" = 'x' AND \"region\" <> 'US'"))
                .isEqualTo("event_type = 'x' AND region <> 'US'");
        assertThat(SelectorNormalizer.normalize("NOT \"gift\"")).isEqualTo("NOT gift");
    }
}
