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

/**
 * Reserved message properties on a windowed query's output topic. Kafka clients (via KoP) see them
 * as record headers.
 */
public final class WindowProtocol {

    /** Set to {@code "true"} on the completion record; its value is a JSON {@link WindowStats}. */
    public static final String COMPLETE_PROPERTY = "__window_complete__";

    /** Source message ID of a forwarded match ({@code ledger:entry:partition:batch}); the dedupe key. */
    public static final String SRC_MSGID_PROPERTY = "__src_msgid__";

    /** Source message's log-clock time (epoch ms) of a forwarded match. */
    public static final String SRC_PUBLISH_TIME_PROPERTY = "__src_publish_time__";

    private WindowProtocol() {
    }
}
