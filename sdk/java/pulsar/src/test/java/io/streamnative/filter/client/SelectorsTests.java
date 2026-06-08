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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Selectors only retains client-side syntax validation. Composition helpers (topicEquals,
 * topicIn, combine, literal, MessageBindingNames) were removed in the orchestrator migration —
 * the orchestrator routes by input topic so the SDK publishes the user's selector verbatim.
 */
class SelectorsTests {

    @Test
    void validateAcceptsNullAndBlank() {
        // null and blank are valid — they mean "no user-side filter; the orchestrator will
        // translate to a JMS-selector identity downstream".
        assertThatCode(() -> Selectors.validate(null)).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("   ")).doesNotThrowAnyException();
    }

    @Test
    void validateAcceptsValidJmsSelectors() {
        assertThatCode(() -> Selectors.validate("region = 'US'")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("__key__ IN ('AAPL','MSFT')")).doesNotThrowAnyException();
        assertThatCode(() -> Selectors.validate("price > 100 AND region = 'US'")).doesNotThrowAnyException();
    }

    @Test
    void validateRejectsMalformedSelector() {
        assertThatThrownBy(() -> Selectors.validate("THIS IS NOT @@## VALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid selector syntax");
    }

    @Test
    void validateRejectsUnbalancedQuotes() {
        assertThatThrownBy(() -> Selectors.validate("region = 'US"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
