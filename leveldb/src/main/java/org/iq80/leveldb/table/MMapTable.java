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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.iq80.leveldb.util.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.Callable;

public class MMapTable extends Table {
    private MappedByteBuffer data;

    public MMapTable(Path path, FileChannel fileChannel, Comparator<ByteBuf> comparator, boolean verifyChecksums)
            throws IOException {
        super(path, fileChannel, comparator, verifyChecksums);
        Preconditions.checkArgument(fileChannel.size() <= Integer.MAX_VALUE, "File must be smaller than %s bytes", Integer.MAX_VALUE);
    }

    public static ByteBuf read(MappedByteBuffer data, int offset, int length) {
        int newPosition = data.position() + offset;
        return Unpooled.wrappedBuffer((ByteBuffer) data.duplicate().order(ByteOrder.LITTLE_ENDIAN).clear().limit(newPosition + length).position(newPosition));
    }

    @Override
    protected Footer init() throws IOException {
        long size = fileChannel.size();
        data = fileChannel.map(MapMode.READ_ONLY, 0, size);
        ByteBuf buffer = Unpooled.wrappedBuffer(data);
        ByteBuf footer = buffer.slice((int) size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footer);
    }

    @Override
    public Callable<?> closer() {
        return new Closer(path, fileChannel, data);
    }

    @SuppressWarnings({"NonPrivateFieldAccessedInSynchronizedContext", "AssignmentToStaticFieldFromInstanceMethod"})
    @Override
    protected Block readBlock(BlockHandle blockHandle)
            throws IOException {
        // read block trailer
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(read(this.data,
                (int) blockHandle.getOffset() + blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH));

// todo re-enable crc check when ported to support direct buffers
//        // only verify check sums if explicitly asked by the user
//        if (verifyChecksums) {
//            // checksum data and the compression type in the trailer
//            PureJavaCrc32C checksum = new PureJavaCrc32C();
//            checksum.update(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize() + 1);
//            int actualCrc32c = checksum.getMaskedValue();
//
//            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
//        }

        // decompress data
        ByteBuf diskBuffer = read(this.data, (int) blockHandle.getOffset(), blockHandle.getDataSize());

        Compressor compressor = null;
        switch (blockTrailer.getCompressionType()) {
            case ZLIB:
                compressor = Zlib.ZLIB;
                break;
            case ZLIB_RAW:
                compressor = Zlib.ZLIB_RAW;
                break;
            case SNAPPY:
                compressor = Snappy.COMPRESSOR;
        }

        ByteBuf uncompressedBuffer;
        if (compressor != null) {
            uncompressedBuffer = ByteBufAllocator.DEFAULT.ioBuffer();
            compressor.decompress(diskBuffer, uncompressedBuffer);
        } else {
            uncompressedBuffer = diskBuffer.slice();
        }

        return new Block(uncompressedBuffer, comparator);
    }

    private static class Closer
            implements Callable<Void> {
        private final Path path;
        private final Closeable closeable;
        private final MappedByteBuffer data;

        public Closer(Path path, Closeable closeable, MappedByteBuffer data) {
            this.path = path;
            this.closeable = closeable;
            this.data = data;
        }

        public Void call() {
            ByteBufferSupport.unmap(data);
            Closeables.closeQuietly(closeable);
            return null;
        }
    }
}
