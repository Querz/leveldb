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
import io.netty.buffer.ByteBufAllocator;
import org.iq80.leveldb.util.*;

public class BlockHandle {
    public static final int MAX_ENCODED_LENGTH = 10 + 10;

    private final long offset;
    private final int dataSize;

    BlockHandle(long offset, int dataSize) {
        this.offset = offset;
        this.dataSize = dataSize;
    }

    public static BlockHandle readBlockHandle(ByteBuf buffer) {
        long offset = VariableLengthQuantity.readVariableLengthLong(buffer);
        long size = VariableLengthQuantity.readVariableLengthLong(buffer);

        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Blocks can not be larger than Integer.MAX_VALUE");
        }

        return new BlockHandle(offset, (int) size);
    }

    public static ByteBuf writeBlockHandle(BlockHandle blockHandle) {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(MAX_ENCODED_LENGTH);
        writeBlockHandleTo(blockHandle, buffer);
        return buffer;
    }

    public static void writeBlockHandleTo(BlockHandle blockHandle, ByteBuf buffer) {
        VariableLengthQuantity.writeVariableLengthLong(blockHandle.offset, buffer);
        VariableLengthQuantity.writeVariableLengthLong(blockHandle.dataSize, buffer);
    }

    public long getOffset() {
        return offset;
    }

    public int getDataSize() {
        return dataSize;
    }

    public int getFullBlockSize() {
        return dataSize + BlockTrailer.ENCODED_LENGTH;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BlockHandle that = (BlockHandle) o;

        if (dataSize != that.dataSize) {
            return false;
        }
        if (offset != that.offset) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (offset ^ (offset >>> 32));
        result = 31 * result + dataSize;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BlockHandle");
        sb.append("{offset=").append(offset);
        sb.append(", dataSize=").append(dataSize);
        sb.append('}');
        return sb.toString();
    }
}
