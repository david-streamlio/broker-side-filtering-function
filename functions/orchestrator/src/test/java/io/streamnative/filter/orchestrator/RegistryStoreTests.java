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

package io.streamnative.filter.orchestrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.streamnative.filter.common.ClientType;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RegistryStoreTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TableView<byte[]> view;
    private Producer<byte[]> producer;
    private TypedMessageBuilder<byte[]> msgBuilder;
    private RegistryStore store;

    private final Map<String, byte[]> tableStorage = new HashMap<>();
    private String lastKey;

    @SuppressWarnings("unchecked")
    @BeforeEach
    void setup() {
        view = (TableView<byte[]>) mock(TableView.class);
        producer = (Producer<byte[]>) mock(Producer.class);
        msgBuilder = (TypedMessageBuilder<byte[]>) mock(TypedMessageBuilder.class);

        when(view.get(anyString())).thenAnswer(inv -> tableStorage.get(inv.getArgument(0, String.class)));
        when(producer.newMessage()).thenReturn(msgBuilder);
        doAnswer(inv -> { lastKey = inv.getArgument(0, String.class); return msgBuilder; })
                .when(msgBuilder).key(anyString());
        doAnswer(inv -> { tableStorage.put(lastKey, inv.getArgument(0)); return msgBuilder; })
                .when(msgBuilder).value(any(byte[].class));
        doAnswer(inv -> { tableStorage.remove(lastKey); return msgBuilder; })
                .when(msgBuilder).value(isNull());

        store = new RegistryStore(view, producer);
    }

    @Test
    void inputTopicEntryUsesTopicPrefix() throws Exception {
        RegistryEntry entry = new RegistryEntry(
                "ctrl-x", "fn-x", "K", "V", ClientType.KAFKA, "2026-05-11T00:00:00Z");
        store.putInputTopicEntry("persistent://t/n/x", entry);

        // Stored under "t:" prefix.
        assertThat(tableStorage).containsKey("t:persistent://t/n/x");
        assertThat(tableStorage).doesNotContainKey("persistent://t/n/x");

        // Read back via the same prefix.
        RegistryEntry roundTrip = store.getInputTopicEntry("persistent://t/n/x");
        assertThat(roundTrip).isEqualTo(entry);
    }

    @Test
    void queryEntryUsesQueryPrefix() throws Exception {
        QueryEntry q = new QueryEntry("persistent://t/n/out", List.of("persistent://t/n/ctrl-1"));
        store.putQueryEntry("query-abc", q);

        assertThat(tableStorage).containsKey("q:query-abc");
        assertThat(store.getQueryEntry("query-abc")).isEqualTo(q);
    }

    @Test
    void keyspacesDoNotCollide() throws Exception {
        // Same logical name used as both an input topic and a queryId — different storage slots.
        RegistryEntry entry = new RegistryEntry(
                "ctrl-x", "fn-x", null, null, ClientType.PULSAR, "2026-05-11T00:00:00Z");
        QueryEntry q = new QueryEntry("persistent://t/n/out", List.of("persistent://t/n/ctrl-1"));

        store.putInputTopicEntry("same-name", entry);
        store.putQueryEntry("same-name", q);

        assertThat(tableStorage).containsKey("t:same-name");
        assertThat(tableStorage).containsKey("q:same-name");
        assertThat(store.getInputTopicEntry("same-name")).isEqualTo(entry);
        assertThat(store.getQueryEntry("same-name")).isEqualTo(q);
    }

    @Test
    void getReturnsNullForAbsentKeys() {
        assertThat(store.getInputTopicEntry("missing")).isNull();
        assertThat(store.getQueryEntry("missing")).isNull();
    }

    @Test
    void removeQueryEntryPublishesTombstoneOnPrefixedKey() throws Exception {
        QueryEntry q = new QueryEntry("persistent://t/n/out", List.of("persistent://t/n/ctrl-1"));
        store.putQueryEntry("query-abc", q);

        store.removeQueryEntry("query-abc");

        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        verify(msgBuilder, org.mockito.Mockito.atLeastOnce()).key(keys.capture());
        assertThat(keys.getAllValues()).contains("q:query-abc");
        assertThat(tableStorage).doesNotContainKey("q:query-abc");
    }

    @Test
    void removeInputTopicEntryPublishesTombstoneOnPrefixedKey() throws Exception {
        RegistryEntry entry = new RegistryEntry(
                "ctrl-x", "fn-x", null, null, ClientType.PULSAR, "2026-05-11T00:00:00Z");
        store.putInputTopicEntry("persistent://t/n/x", entry);

        store.removeInputTopicEntry("persistent://t/n/x");

        assertThat(tableStorage).doesNotContainKey("t:persistent://t/n/x");
    }
}
