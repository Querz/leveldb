/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.table;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.iq80.leveldb.impl.SeekingIterator;
import org.iq80.leveldb.util.Buffers;
import org.testng.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import static com.google.common.base.Charsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_BYTE;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;
import static org.testng.Assert.*;

public final class BlockHelper {
    private BlockHelper() {
    }

    public static int estimateBlockSize(int blockRestartInterval, List<BlockEntry> entries) {
        if (entries.isEmpty()) {
            return SIZE_OF_INT;
        }
        int restartCount = (int) Math.ceil(1.0 * entries.size() / blockRestartInterval);
        return estimateEntriesSize(blockRestartInterval, entries) +
                (restartCount * SIZE_OF_INT) +
                SIZE_OF_INT;
    }

    @SafeVarargs
    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Entry<K, V>... entries) {
        assertSequence(seekingIterator, Arrays.asList(entries));
    }

    public static <K, V> void assertSequence(SeekingIterator<K, V> seekingIterator, Iterable<? extends Entry<K, V>> entries) {
        Assert.assertNotNull(seekingIterator, "blockIterator is not null");

        for (Entry<K, V> entry : entries) {
            assertTrue(seekingIterator.hasNext());
            assertEntryEquals(seekingIterator.peek(), entry);
            assertEntryEquals(seekingIterator.next(), entry);
        }
        assertFalse(seekingIterator.hasNext());

        try {
            seekingIterator.peek();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException expected) {
        }
        try {
            seekingIterator.next();
            fail("expected NoSuchElementException");
        } catch (NoSuchElementException expected) {
        }
    }

    public static <K, V> void assertEntryEquals(Entry<K, V> actual, Entry<K, V> expected) {
        if (actual.getKey() instanceof ByteBuf) {
            assertSliceEquals((ByteBuf) actual.getKey(), (ByteBuf) expected.getKey());
            assertSliceEquals((ByteBuf) actual.getValue(), (ByteBuf) expected.getValue());
        }
        assertEquals(actual, expected);
    }

    public static void assertSliceEquals(ByteBuf actual, ByteBuf expected) {
        assertEquals(actual.toString(UTF_8), expected.toString(UTF_8));
    }

    public static String beforeString(Entry<String, ?> expectedEntry) {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte - 1));
    }

    public static String afterString(Entry<String, ?> expectedEntry) {
        String key = expectedEntry.getKey();
        int lastByte = key.charAt(key.length() - 1);
        return key.substring(0, key.length() - 1) + ((char) (lastByte + 1));
    }

    public static ByteBuf before(Entry<ByteBuf, ?> expectedEntry) {
        ByteBuf slice = expectedEntry.getKey().slice(0, expectedEntry.getKey().readableBytes()).copy();
        int lastByte = slice.writerIndex() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) - 1);
        return slice;
    }

    public static ByteBuf after(Entry<ByteBuf, ?> expectedEntry) {
        ByteBuf slice = expectedEntry.getKey().slice(0, expectedEntry.getKey().readableBytes()).copy();
        int lastByte = slice.writerIndex() - 1;
        slice.setByte(lastByte, slice.getUnsignedByte(lastByte) + 1);
        return slice;
    }

    public static int estimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries) {
        int size = 0;
        ByteBuf previousKey = null;
        int restartBlockCount = 0;
        for (BlockEntry entry : entries) {
            int nonSharedBytes;
            if (restartBlockCount < blockRestartInterval) {
                nonSharedBytes = entry.getKey().readableBytes() - BlockBuilder.calculateSharedBytes(entry.getKey(), previousKey);
            } else {
                nonSharedBytes = entry.getKey().readableBytes();
                restartBlockCount = 0;
            }
            size += nonSharedBytes +
                    entry.getValue().readableBytes() +
                    (SIZE_OF_BYTE * 3); // 3 bytes for sizes

            previousKey = entry.getKey();
            restartBlockCount++;

        }
        return size;
    }

    static BlockEntry createBlockEntry(String key, String value) {
        return new BlockEntry(Unpooled.wrappedBuffer(key.getBytes(UTF_8)), Unpooled.wrappedBuffer(value.getBytes(UTF_8)));
    }
}
