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

package io.streamnative.filter.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * The time window of a windowed query, as carried to the filter function in a {@link QueryMessage}.
 *
 * <p>Times are epoch milliseconds on the log's clock: the broker-entry timestamp when the broker
 * appends one (the demo stack does), else the producer publish time. That is the clock
 * {@code resetCursor(timestamp)} positions by, so the window bounds and the cursor agree.
 *
 * <p>{@code endLedgerId}/{@code endEntryId} are the topic's last message position captured when the
 * query was registered: the end of history. A bounded window ({@code toMs} set) ends at whichever comes
 * first, a record past {@code toMs} or the last record at or before that position; without the position,
 * a window whose {@code toMs} is "now" would never end on a quiet topic. An open-ended window
 * ({@code toMs} {@code null}) never ends: at that position the function marks the end of history with a
 * caught-up record and keeps forwarding live matches. Both are {@code null} when no position was
 * captured.
 *
 * <p>{@code truncatedByRetention} is set by the orchestrator when {@code fromMs} is earlier than the
 * earliest retained message, so the completion record can say the result doesn't cover the whole
 * requested window.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WindowSpec {

    private final long fromMs;
    private final Long toMs;
    private final Long endLedgerId;
    private final Long endEntryId;
    private final boolean truncatedByRetention;

    @JsonCreator
    public WindowSpec(@JsonProperty("fromMs") long fromMs,
                      @JsonProperty("toMs") Long toMs,
                      @JsonProperty("endLedgerId") Long endLedgerId,
                      @JsonProperty("endEntryId") Long endEntryId,
                      @JsonProperty("truncatedByRetention") boolean truncatedByRetention) {
        this.fromMs = fromMs;
        this.toMs = toMs;
        this.endLedgerId = endLedgerId;
        this.endEntryId = endEntryId;
        this.truncatedByRetention = truncatedByRetention;
    }

    public long getFromMs()                  { return fromMs; }
    /** End of the window, or {@code null} for an open-ended window. */
    public Long getToMs()                    { return toMs; }
    public Long getEndLedgerId()             { return endLedgerId; }
    public Long getEndEntryId()              { return endEntryId; }
    public boolean isTruncatedByRetention()  { return truncatedByRetention; }

    /** True for an open-ended window: replay from {@code fromMs}, then keep streaming. */
    @JsonIgnore
    public boolean isOpenEnded() {
        return toMs == null;
    }

    /** True when an end-of-log position was captured. */
    public boolean hasEndPosition() {
        return endLedgerId != null && endEntryId != null;
    }

    /** @throws IllegalArgumentException if the window is empty or the end position is half-set */
    public void validate() {
        if (fromMs < 0 || (toMs != null && toMs <= fromMs)) {
            throw new IllegalArgumentException("window requires 0 <= fromMs < toMs, got [" + fromMs + ", " + toMs + "]");
        }
        if ((endLedgerId == null) != (endEntryId == null)) {
            throw new IllegalArgumentException("window end position needs both endLedgerId and endEntryId");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WindowSpec that)) return false;
        return fromMs == that.fromMs && Objects.equals(toMs, that.toMs)
                && truncatedByRetention == that.truncatedByRetention
                && Objects.equals(endLedgerId, that.endLedgerId)
                && Objects.equals(endEntryId, that.endEntryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromMs, toMs, endLedgerId, endEntryId, truncatedByRetention);
    }

    @Override
    public String toString() {
        return "WindowSpec{[" + fromMs + ", " + (toMs == null ? "open" : toMs) + "], end=" + endLedgerId + ":" + endEntryId
                + (truncatedByRetention ? ", truncatedByRetention" : "") + '}';
    }
}
