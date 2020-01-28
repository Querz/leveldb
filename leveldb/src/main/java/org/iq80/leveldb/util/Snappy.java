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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <p>
 * A Snappy abstraction which attempts uses the xerial implementation and falls back
 * to the iq80 Snappy implementation it cannot be loaded.  You can change the
 * load order by setting the 'leveldb.snappy' system property.  Example:
 * <p/>
 * <code>
 * -Dleveldb.snappy=xerial,iq80
 * </code>
 * <p/>
 * The system property can also be configured with the name of a class which
 * implements the Snappy.SPI interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public final class Snappy {
    public static final Compressor COMPRESSOR;

    static {
        Compressor attempt = null;
        String[] factories = System.getProperty("leveldb.snappy", "xerial,iq80").split(",");
        for (int i = 0; i < factories.length && attempt == null; i++) {
            String name = factories[i];
            try {
                name = name.trim();
                if ("xerial".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$XerialCompressor";
                } else if ("iq80".equals(name.toLowerCase())) {
                    name = "org.iq80.leveldb.util.Snappy$IQ80Compressor";
                }
                attempt = (Compressor) Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
            } catch (Throwable ignored) {
            }
        }
        COMPRESSOR = attempt;
    }

    private Snappy() {
    }

    public static boolean available() {
        return COMPRESSOR != null;
    }

    public static class XerialCompressor implements Compressor {
        static {
            // Make sure that the JNI libs are fully loaded.
            try {
                org.xerial.snappy.Snappy.compress("test");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void compress(ByteBuf uncompressed, ByteBuf compressed) throws IOException {
            ByteBuffer uncompressedBuffer = uncompressed.internalNioBuffer(uncompressed.readerIndex(), uncompressed.readableBytes());
            ByteBuffer compressedBuffer = compressed.internalNioBuffer(compressed.writerIndex(), compressed.writableBytes());
            int written = org.xerial.snappy.Snappy.compress(uncompressedBuffer, compressedBuffer);
            compressed.writerIndex(compressed.writerIndex() + written);
        }

        @Override
        public void decompress(ByteBuf compressed, ByteBuf uncompressed) throws IOException {
            ByteBuffer compressedBuffer = compressed.internalNioBuffer(compressed.writerIndex(), compressed.writableBytes());
            ByteBuffer uncompressedBuffer = uncompressed.internalNioBuffer(uncompressed.readerIndex(), uncompressed.readableBytes());
            int written = org.xerial.snappy.Snappy.uncompress(compressedBuffer, uncompressedBuffer);
            uncompressed.writerIndex(uncompressed.writerIndex() + written);
        }
    }

    public static class IQ80Compressor implements Compressor {

        private static final ThreadLocal<byte[]> BUFFER = ThreadLocal.withInitial(() -> new byte[Short.MAX_VALUE]);

        static {
            // Make sure that the library can fully load.
            try {
                new IQ80Compressor().compress(Buffers.encodeString("test"), ByteBufAllocator.DEFAULT.ioBuffer());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void compress(ByteBuf uncompressed, ByteBuf compressed) throws IOException {
            byte[] input = new byte[uncompressed.readableBytes()];;
            int length = input.length;

            uncompressed.markReaderIndex();
            uncompressed.readBytes(input);
            uncompressed.resetReaderIndex();

            byte[] output = BUFFER.get();

            int written = org.iq80.snappy.Snappy.compress(input, 0, length, output, 0);

            compressed.writeBytes(output, 0, written);
        }

        @Override
        public void decompress(ByteBuf compressed, ByteBuf uncompressed) {
            byte[] input = new byte[compressed.readableBytes()];;
            int length = input.length;

            compressed.markReaderIndex();
            compressed.readBytes(input);
            compressed.resetReaderIndex();

            int t = org.iq80.snappy.Snappy.getUncompressedLength(input, 0);
            byte[] output = new byte[t];

            int written = org.iq80.snappy.Snappy.uncompress(input, 0, length, output, 0);

            uncompressed.writeBytes(output, 0, written);
        }
    }
}
