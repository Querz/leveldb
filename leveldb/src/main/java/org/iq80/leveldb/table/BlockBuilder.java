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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.iq80.leveldb.util.IntVector;
import org.iq80.leveldb.util.VariableLengthQuantity;

import java.util.Comparator;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.iq80.leveldb.util.SizeOf.SIZE_OF_INT;

public class BlockBuilder implements ReferenceCounted {
    private final int blockRestartInterval;
    private final IntVector restartPositions;
    private final Comparator<ByteBuf> comparator;
    private final ByteBuf block;
    private int entryCount;
    private int restartBlockEntryCount;
    private boolean finished;
    private ByteBuf lastKey;

    public BlockBuilder(int estimatedSize, int blockRestartInterval, Comparator<ByteBuf> comparator) {
        Preconditions.checkArgument(estimatedSize >= 0, "estimatedSize is negative");
        Preconditions.checkArgument(blockRestartInterval >= 0, "blockRestartInterval is negative");
        Preconditions.checkNotNull(comparator, "comparator is null");

        this.block = ByteBufAllocator.DEFAULT.ioBuffer(estimatedSize);
        this.blockRestartInterval = blockRestartInterval;
        this.comparator = comparator;

        restartPositions = new IntVector(32);
        restartPositions.add(0);  // first restart point must be 0
    }

    public static int calculateSharedBytes(ByteBuf leftKey, ByteBuf rightKey) {
        int sharedKeyBytes = 0;

        if (leftKey != null && rightKey != null) {
            int minSharedKeyBytes = Ints.min(leftKey.writerIndex(), rightKey.writerIndex());
            while (sharedKeyBytes < minSharedKeyBytes && leftKey.getByte(sharedKeyBytes) == rightKey.getByte(sharedKeyBytes)) {
                sharedKeyBytes++;
            }
        }

        return sharedKeyBytes;
    }

    public void reset() {
        block.clear();
        entryCount = 0;
        restartPositions.clear();
        restartPositions.add(0); // first restart point must be 0
        restartBlockEntryCount = 0;
        ReferenceCountUtil.release(lastKey);
        lastKey = null;
        finished = false;
    }

    public int getEntryCount() {
        return entryCount;
    }

    public boolean isEmpty() {
        return entryCount == 0;
    }

    public int currentSizeEstimate() {
        // no need to estimate if closed
        if (finished) {
            return block.writerIndex();
        }

        // no records is just a single int
        if (block.writerIndex() == 0) {
            return SIZE_OF_INT;
        }

        return block.writerIndex() +                     // raw data buffer
                restartPositions.size() * SIZE_OF_INT +    // restart positions
                SIZE_OF_INT;                               // restart position size
    }

    public void add(BlockEntry blockEntry) {
        Preconditions.checkNotNull(blockEntry, "blockEntry is null");
        add(blockEntry.getKey(), blockEntry.getValue());
    }

    public void add(ByteBuf key, ByteBuf value) {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkState(!finished, "block is finished");
        Preconditions.checkPositionIndex(restartBlockEntryCount, blockRestartInterval);

        Preconditions.checkArgument(lastKey == null || comparator.compare(key, lastKey) > 0,
                "key must be greater than last key (%s, %s)",
                lastKey == null ? "" : lastKey.toString(UTF_8), key.toString(UTF_8));

        int sharedKeyBytes = 0;
        if (restartBlockEntryCount < blockRestartInterval) {
            sharedKeyBytes = calculateSharedBytes(key, lastKey);
        } else {
            // restart prefix compression
            restartPositions.add(block.writerIndex());
            restartBlockEntryCount = 0;
        }

        int nonSharedKeyBytes = key.writerIndex() - sharedKeyBytes;

        // write "<shared><non_shared><value_size>"
        VariableLengthQuantity.writeVariableLengthInt(sharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(nonSharedKeyBytes, block);
        VariableLengthQuantity.writeVariableLengthInt(value.writerIndex(), block);

        // update last key
        ReferenceCountUtil.release(lastKey);
        lastKey = key.retainedDuplicate();

        // write non-shared key bytes
        block.writeBytes(key, sharedKeyBytes, nonSharedKeyBytes);

        // write value bytes
        block.writeBytes(value, 0, value.writerIndex());

        // update state
        entryCount++;
        restartBlockEntryCount++;
    }

    public ByteBuf finish() {
        if (!finished) {
            finished = true;

            if (entryCount > 0) {
                restartPositions.write(block);
                block.writeInt(restartPositions.size());
            } else {
                block.writeInt(0);
            }
        }
        return block.slice();
    }

    @Override
    public int refCnt() {
        return block.refCnt();
    }

    @Override
    public BlockBuilder retain() {
        block.retain();
        return this;
    }

    @Override
    public BlockBuilder retain(int i) {
        block.retain(i);
        return this;
    }

    @Override
    public BlockBuilder touch() {
        block.touch();
        return this;
    }

    @Override
    public BlockBuilder touch(Object o) {
        block.touch(o);
        return this;
    }

    @Override
    public boolean release() {
        return block.release();
    }

    @Override
    public boolean release(int i) {
        return block.release(i);
    }
}
