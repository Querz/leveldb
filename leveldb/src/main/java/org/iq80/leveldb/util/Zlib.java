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
package org.iq80.leveldb.util;

import com.nukkitx.natives.util.Natives;
import com.nukkitx.natives.zlib.Deflater;
import com.nukkitx.natives.zlib.Inflater;
import io.netty.buffer.ByteBuf;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;

/**
 * Some glue code that uses the java.util.zip classes to implement ZLIB
 * compression for leveldb.
 */
public class Zlib {

    private static final com.nukkitx.natives.zlib.Zlib ZLIB_NATIVE_CODE = Natives.ZLIB.get();

    public static final ZLibCompressor ZLIB = new ZLibCompressor(false);
    public static final ZLibCompressor ZLIB_RAW = new ZLibCompressor(true);

    public static boolean available() {
        return true; // Zlib is built into java
    }


    private static class ZLibCompressor implements Compressor {

        private final ThreadLocal<Inflater> inflator;
        private final ThreadLocal<Deflater> deflater;

        public ZLibCompressor(boolean nowrap) {
            this.inflator = ThreadLocal.withInitial(() -> ZLIB_NATIVE_CODE.create(nowrap));
            this.deflater = ThreadLocal.withInitial(() -> ZLIB_NATIVE_CODE.create(-1, nowrap));
        }

        @Override
        public void compress(ByteBuf uncompressed, ByteBuf compressed) throws IOException {
            Deflater deflater = this.deflater.get();
            deflater.reset();

            deflater.setInput(uncompressed.internalNioBuffer(uncompressed.readerIndex(), uncompressed.readableBytes()));

            while (!deflater.finished()) {
                compressed.ensureWritable(8192);
                ByteBuffer internalBuffer = compressed.internalNioBuffer(compressed.writerIndex(), compressed.writableBytes());
                int result = deflater.deflate(internalBuffer);
                compressed.writerIndex(compressed.writerIndex() + result);
            }
        }

        @Override
        public void decompress(ByteBuf compressed, ByteBuf uncompressed) throws IOException {
            Inflater inflater = inflator.get();
            inflater.reset();

            inflater.setInput(compressed.internalNioBuffer(compressed.readerIndex(), compressed.readableBytes()));

            try {
                while (!inflater.finished()) {
                    uncompressed.ensureWritable(8192);
                    ByteBuffer internalBuffer = uncompressed.internalNioBuffer(uncompressed.writerIndex(), uncompressed.writableBytes());
                    int result = inflater.inflate(internalBuffer);
                    uncompressed.writerIndex(uncompressed.writerIndex() + result);
                }
            } catch (DataFormatException e) {
                throw new IOException("Unable to inflate zlib data", e);
            }
        }
    }
}