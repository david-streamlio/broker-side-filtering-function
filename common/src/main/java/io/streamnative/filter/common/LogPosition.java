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
 * A message's position in the log: ledger, entry, and (for a batched message) index and batch size.
 *
 * <p>Decoded from the bytes of {@code MessageId.toByteArray()}, the serialized {@code MessageIdData}
 * ({@code ledgerId = 1}, {@code entryId = 2}, {@code batch_index = 4}, {@code batch_size = 6}). Working
 * from the bytes keeps code inside a Pulsar Function or a NAR independent of which classloader created
 * the {@code MessageId}: in Pulsar 4.2 {@code MessageIdAdv} ships in {@code pulsar-common}, which a
 * function can't see, and a NAR's bundled copy differs from the runtime's.
 */
public record LogPosition(long ledgerId, long entryId, int batchIndex, int batchSize) {

    private static final int LEDGER_ID = 1;
    private static final int ENTRY_ID = 2;
    private static final int BATCH_INDEX = 4;
    private static final int BATCH_SIZE = 6;

    /** Decode {@code MessageId.toByteArray()} output, or return {@code null} if it can't be decoded. */
    public static LogPosition fromBytes(byte[] messageIdBytes) {
        if (messageIdBytes == null) {
            return null;
        }
        try {
            return decode(messageIdBytes);
        } catch (RuntimeException e) {
            return null;
        }
    }

    static LogPosition decode(byte[] data) {
        long ledgerId = -1;
        long entryId = -1;
        int batchIndex = -1;
        int batchSize = 0;
        int[] pos = {0};
        while (pos[0] < data.length) {
            long tag = readVarint(data, pos);
            int field = (int) (tag >>> 3);
            int wireType = (int) (tag & 0x7);
            if (wireType == 0) {
                long value = readVarint(data, pos);
                switch (field) {
                    case LEDGER_ID -> ledgerId = value;
                    case ENTRY_ID -> entryId = value;
                    case BATCH_INDEX -> batchIndex = (int) value;
                    case BATCH_SIZE -> batchSize = (int) value;
                    default -> { }
                }
            } else {
                skip(data, pos, wireType);
            }
        }
        if (ledgerId < 0) {
            throw new IllegalArgumentException("MessageIdData without ledgerId");
        }
        return new LogPosition(ledgerId, entryId, batchIndex, batchSize);
    }

    /** Compare by ledger, then entry: batch messages in one entry share a position. */
    public int compareTo(long otherLedgerId, long otherEntryId) {
        int byLedger = Long.compare(ledgerId, otherLedgerId);
        return byLedger != 0 ? byLedger : Long.compare(entryId, otherEntryId);
    }

    /** True for an unbatched message, or the last message of a batch entry. */
    public boolean isLastInEntry() {
        return batchIndex < 0 || batchSize <= 1 || batchIndex >= batchSize - 1;
    }

    /** False for an empty topic's "last message" position (entry -1). */
    public boolean isMessage() {
        return entryId >= 0;
    }

    private static long readVarint(byte[] data, int[] pos) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = data[pos[0]++];
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new IllegalArgumentException("malformed varint");
    }

    private static void skip(byte[] data, int[] pos, int wireType) {
        switch (wireType) {
            case 1 -> pos[0] += 8;
            case 2 -> {
                long len = readVarint(data, pos);
                pos[0] += (int) len;
            }
            case 5 -> pos[0] += 4;
            default -> throw new IllegalArgumentException("unsupported wire type " + wireType);
        }
    }
}
