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
package org.iq80.leveldb.impl;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.iq80.leveldb.util.*;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.iq80.leveldb.impl.LogConstants.BLOCK_SIZE;
import static org.iq80.leveldb.impl.LogConstants.HEADER_SIZE;
import static org.iq80.leveldb.util.CRC32CUtils.getChunkChecksum;

public class MMapLogWriter implements LogWriter {
    private static final int PAGE_SIZE = 1024 * 1024;

    private final Path path;
    private final long fileNumber;
    private final FileChannel fileChannel;
    private final AtomicBoolean closed = new AtomicBoolean();
    private MappedByteBuffer mappedByteBuffer;
    private long fileOffset;
    /**
     * Current offset in the current block
     */
    private int blockOffset;

    public MMapLogWriter(Path path, long fileNumber) throws IOException {
        Preconditions.checkNotNull(path, "file is null");
        Preconditions.checkArgument(fileNumber >= 0, "fileNumber is negative");
        this.path = path;
        this.fileNumber = fileNumber;
        this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, PAGE_SIZE);
    }

    private static ByteBuf newLogRecordHeader(LogChunkType type, ByteBuf buffer) {
        int crc = getChunkChecksum(type.getPersistentId(), buffer);

        // Format the header
        ByteBuf header = ByteBufAllocator.DEFAULT.ioBuffer(HEADER_SIZE);
        header.writeInt(crc);
        header.writeByte((byte) (buffer.readableBytes() & 0xff));
        header.writeByte((byte) (buffer.readableBytes() >>> 8));
        header.writeByte((byte) (type.getPersistentId()));

        return header;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public synchronized void close()
            throws IOException {
        closed.set(true);

        destroyMappedByteBuffer();

        if (fileChannel.isOpen()) {
            fileChannel.truncate(fileOffset);
        }

        // close the channel
        Closeables.closeQuietly(fileChannel);
    }

    @Override
    public synchronized void delete()
            throws IOException {
        close();

        // try to delete the file
        Files.deleteIfExists(path);
    }

    private void destroyMappedByteBuffer() {
        if (mappedByteBuffer != null) {
            fileOffset += mappedByteBuffer.position();
            unmap();
        }
        mappedByteBuffer = null;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public long getFileNumber() {
        return fileNumber;
    }

    // Writes a stream of chunks such that no chunk is split across a block boundary
    @Override
    public synchronized void addRecord(ByteBuf record, boolean force) throws IOException {
        Preconditions.checkState(!closed.get(), "Log has been closed");

        // used to track first, middle and last blocks
        boolean begin = true;

        // Fragment the record int chunks as necessary and write it.  Note that if record
        // is empty, we still want to iterate once to write a single
        // zero-length chunk.
        do {
            int bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            Preconditions.checkState(bytesRemainingInBlock >= 0);

            // Switch to a new block if necessary
            if (bytesRemainingInBlock < HEADER_SIZE) {
                if (bytesRemainingInBlock > 0) {
                    // Fill the rest of the block with zeros
                    // todo lame... need a better way to write zeros
                    ensureCapacity(bytesRemainingInBlock);
                    mappedByteBuffer.put(new byte[bytesRemainingInBlock]);
                }
                blockOffset = 0;
                bytesRemainingInBlock = BLOCK_SIZE - blockOffset;
            }

            // Invariant: we never leave less than HEADER_SIZE bytes available in a block
            int bytesAvailableInBlock = bytesRemainingInBlock - HEADER_SIZE;
            Preconditions.checkState(bytesAvailableInBlock >= 0);

            // if there are more bytes in the record then there are available in the block,
            // fragment the record; otherwise write to the end of the record
            boolean end;
            int fragmentLength;
            if (record.readableBytes() > bytesAvailableInBlock) {
                end = false;
                fragmentLength = bytesAvailableInBlock;
            } else {
                end = true;
                fragmentLength = record.readableBytes();
            }

            // determine block type
            LogChunkType type;
            if (begin && end) {
                type = LogChunkType.FULL;
            } else if (begin) {
                type = LogChunkType.FIRST;
            } else if (end) {
                type = LogChunkType.LAST;
            } else {
                type = LogChunkType.MIDDLE;
            }

            // write the chunk
            writeChunk(type, record.readSlice(fragmentLength));

            // we are no longer on the first chunk
            begin = false;
        } while (record.isReadable());

        if (force) {
            mappedByteBuffer.force();
        }
    }

    private void writeChunk(LogChunkType type, ByteBuf slice) throws IOException {
        Preconditions.checkArgument(slice.readableBytes() <= 0xffff, "length %s is larger than two bytes", slice.readableBytes());
        Preconditions.checkArgument(blockOffset + HEADER_SIZE <= BLOCK_SIZE);

        // create header
        ByteBuf header = newLogRecordHeader(type, slice);
        try {
            // write the header and the payload
            ensureCapacity(header.readableBytes() + slice.readableBytes());
            mappedByteBuffer.put(header.internalNioBuffer(0, header.writerIndex()));
            mappedByteBuffer.put(slice.internalNioBuffer(0, slice.writerIndex()));

            blockOffset += HEADER_SIZE + slice.readableBytes();
        } finally {
            header.release();
        }
    }

    private void ensureCapacity(int bytes) throws IOException {
        if (mappedByteBuffer.remaining() < bytes) {
            // remap
            fileOffset += mappedByteBuffer.position();
            unmap();

            mappedByteBuffer = fileChannel.map(MapMode.READ_WRITE, fileOffset, PAGE_SIZE);
        }
    }

    private void unmap() {
        ByteBufferSupport.unmap(mappedByteBuffer);
    }
}
