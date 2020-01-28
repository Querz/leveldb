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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Comparator;

import static org.iq80.leveldb.CompressionType.*;

public class FileChannelTable extends Table {
    public FileChannelTable(Path path, FileChannel fileChannel, Comparator<ByteBuf> comparator, boolean verifyChecksums)
            throws IOException {
        super(path, fileChannel, comparator, verifyChecksums);
    }

    @Override
    protected Footer init() throws IOException {
        long size = fileChannel.size();
        ByteBuf footerData = read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        try {
            return Footer.readFooter(footerData);
        } finally {
            footerData.release();
        }
    }

    @SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod", "NonPrivateFieldAccessedInSynchronizedContext"})
    @Override
    protected Block readBlock(BlockHandle blockHandle) throws IOException {
        // read block trailer
        ByteBuf trailerData = read(blockHandle.getOffset() + blockHandle.getDataSize(), BlockTrailer.ENCODED_LENGTH);
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(trailerData);

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

        ByteBuf diskBuffer = read(blockHandle.getOffset(), blockHandle.getDataSize());

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

    private ByteBuf read(long offset, int length) throws IOException {
        ByteBuf buffer = ByteBufAllocator.DEFAULT.ioBuffer(length);
        if (fileChannel.read(buffer.internalNioBuffer(0, length), offset) != length) {
            throw new IOException("Could not read all the data");
        }
        return buffer;
    }
}
