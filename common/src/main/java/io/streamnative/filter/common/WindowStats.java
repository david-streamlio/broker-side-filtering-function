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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

/**
 * Completion evidence for a windowed query: the value of the completion record the filter function
 * writes to the query's output topic (property {@link WindowProtocol#COMPLETE_PROPERTY}) after the
 * last match.
 *
 * <p>Counts cover the records the function scanned inside the window, so a client can see how much
 * the server read to answer it. They reset if the function restarts mid-window (at-least-once
 * delivery may then also produce duplicate matches; clients dedupe by
 * {@link WindowProtocol#SRC_MSGID_PROPERTY}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WindowStats {

    /** Whether the result covers the whole requested window. */
    public enum Status {
        COMPLETE,
        /** The window starts before the earliest retained message. */
        TRUNCATED_BY_RETENTION
    }

    /** What ended the window. */
    public enum EndedBy {
        /** A record past {@code toMs}. */
        WINDOW_END,
        /** The last record at or before the end-of-log position captured at registration. */
        LOG_END
    }

    private final String queryId;
    private final long windowFromMs;
    private final long windowToMs;
    private final long scanned;
    private final long matched;
    private final long bytesScanned;
    private final Long firstScannedMs;
    private final Long lastScannedMs;
    private final long elapsedMs;
    private final EndedBy endedBy;
    private final Status status;

    @JsonCreator
    public WindowStats(@JsonProperty("queryId") String queryId,
                       @JsonProperty("windowFromMs") long windowFromMs,
                       @JsonProperty("windowToMs") long windowToMs,
                       @JsonProperty("scanned") long scanned,
                       @JsonProperty("matched") long matched,
                       @JsonProperty("bytesScanned") long bytesScanned,
                       @JsonProperty("firstScannedMs") Long firstScannedMs,
                       @JsonProperty("lastScannedMs") Long lastScannedMs,
                       @JsonProperty("elapsedMs") long elapsedMs,
                       @JsonProperty("endedBy") EndedBy endedBy,
                       @JsonProperty("status") Status status) {
        this.queryId = queryId;
        this.windowFromMs = windowFromMs;
        this.windowToMs = windowToMs;
        this.scanned = scanned;
        this.matched = matched;
        this.bytesScanned = bytesScanned;
        this.firstScannedMs = firstScannedMs;
        this.lastScannedMs = lastScannedMs;
        this.elapsedMs = elapsedMs;
        this.endedBy = endedBy;
        this.status = status;
    }

    public String getQueryId()        { return queryId; }
    public long getWindowFromMs()     { return windowFromMs; }
    public long getWindowToMs()       { return windowToMs; }
    public long getScanned()          { return scanned; }
    public long getMatched()          { return matched; }
    public long getBytesScanned()     { return bytesScanned; }
    public Long getFirstScannedMs()   { return firstScannedMs; }
    public Long getLastScannedMs()    { return lastScannedMs; }
    public long getElapsedMs()        { return elapsedMs; }
    public EndedBy getEndedBy()       { return endedBy; }
    public Status getStatus()         { return status; }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WindowStats that)) return false;
        return windowFromMs == that.windowFromMs && windowToMs == that.windowToMs
                && scanned == that.scanned && matched == that.matched
                && bytesScanned == that.bytesScanned && elapsedMs == that.elapsedMs
                && Objects.equals(queryId, that.queryId)
                && Objects.equals(firstScannedMs, that.firstScannedMs)
                && Objects.equals(lastScannedMs, that.lastScannedMs)
                && endedBy == that.endedBy && status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, windowFromMs, windowToMs, scanned, matched, bytesScanned,
                firstScannedMs, lastScannedMs, elapsedMs, endedBy, status);
    }

    @Override
    public String toString() {
        return "WindowStats{queryId='" + queryId + "', window=[" + windowFromMs + ", " + windowToMs
                + "], scanned=" + scanned + ", matched=" + matched + ", bytesScanned=" + bytesScanned
                + ", elapsedMs=" + elapsedMs + ", endedBy=" + endedBy + ", status=" + status + '}';
    }
}
