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

package io.streamnative.filter.api;

/**
 * A control-plane failure in either SDK: registering, changing, or retracting a filtered query
 * couldn't be delivered to the server (e.g. the broker was unreachable, the request timed out, the
 * session was closed, or the caller isn't authorized). The underlying client exception is the
 * {@link #getCause() cause}.
 *
 * <p>Unchecked, like Kafka's client exceptions. Failures reading results come from the underlying
 * consumer itself (Pulsar {@code PulsarClientException}, Kafka {@code KafkaException}) and are not
 * wrapped.
 */
public class FilterException extends RuntimeException {

    public FilterException(String message, Throwable cause) {
        super(message, cause);
    }
}
