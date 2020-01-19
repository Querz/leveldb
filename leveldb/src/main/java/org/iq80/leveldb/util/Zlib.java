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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.*;

/**
 * Some glue code that uses the java.util.zip classes to implement ZLIB
 * compression for leveldb.
 */
public class Zlib {

    static final private ZLibSPI ZLIB;
    static final private ZLibSPI ZLIB_RAW;

    static {
        ZLIB = new ZLibSPI(false);
        ZLIB_RAW = new ZLibSPI(true);
    }

    public static boolean available() {
        return ZLIB != null && ZLIB_RAW != null;
    }

    public static void uncompress(ByteBuffer compressed, ByteArrayOutputStream uncompressed)
            throws IOException {
        ZLIB.uncompress(compressed, uncompressed);
    }

    public static void uncompress(byte[] input, int inputOffset, int length,
                                  byte[] output, int outputOffset) throws IOException {
        ZLIB.uncompress(input, inputOffset, length, output, outputOffset);
    }

    public static int compress(byte[] input, int inputOffset, int length,
                               byte[] output, int outputOffset) throws IOException {
        return ZLIB.compress(input, inputOffset, length, output, outputOffset);
    }

    public static byte[] compress(String text) throws IOException {
        return ZLIB.compress(text);
    }

    public static void uncompressRaw(ByteBuffer compressed, ByteArrayOutputStream uncompressed)
            throws IOException {
        ZLIB_RAW.uncompress(compressed, uncompressed);
    }

    public static void uncompressRaw(byte[] input, int inputOffset, int length,
                                     byte[] output, int outputOffset) throws IOException {
        ZLIB_RAW.uncompress(input, inputOffset, length, output, outputOffset);
    }

    public static int compressRaw(byte[] input, int inputOffset, int length,
                                  byte[] output, int outputOffset) throws IOException {
        return ZLIB_RAW.compress(input, inputOffset, length, output, outputOffset);
    }

    public static byte[] compressRaw(String text) throws IOException {
        return ZLIB_RAW.compress(text);
    }

    /**
     * From:
     * http://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-
     * an-inputstream
     */
    public static class ByteBufferBackedInputStream extends InputStream {

        ByteBuffer buf;

        public ByteBufferBackedInputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        public int read() throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }
            return buf.get() & 0xFF;
        }

        public int read(byte[] bytes, int off, int len) throws IOException {
            if (!buf.hasRemaining()) {
                return -1;
            }

            len = Math.min(len, buf.remaining());
            buf.get(bytes, off, len);
            return len;
        }
    }

    public static class ByteBufferBackedOutputStream extends OutputStream {
        ByteBuffer buf;

        public ByteBufferBackedOutputStream(ByteBuffer buf) {
            this.buf = buf;
        }

        public void write(int b) throws IOException {
            buf.put((byte) b);
        }

        public void write(byte[] bytes, int off, int len) throws IOException {
            buf.put(bytes, off, len);
        }

    }

    /**
     * Use the same SPI interface as Snappy, for the case if leveldb ever gets
     * a compression plug-in type.
     */
    private static class ZLibSPI {

        private static final ThreadLocal<byte[]> BUFFERS = ThreadLocal.withInitial(() -> new byte[1024]);

        private final ThreadLocal<Deflater> deflaterThreadLocal;
        private final ThreadLocal<Inflater> inflaterThreadLocal;

        public ZLibSPI(final boolean raw) {
            this.deflaterThreadLocal = ThreadLocal.withInitial(() -> new Deflater(-1, raw));
            this.inflaterThreadLocal = ThreadLocal.withInitial(() -> new Inflater(raw));
        }

        private int copy(InputStream in, OutputStream out) throws IOException {
            byte[] buffer = BUFFERS.get();
            int read;
            int count = 0;

            while (-1 != (read = in.read(buffer))) {
                out.write(buffer, 0, read);
                count += read;
            }

            return count;
        }

        public int uncompress(ByteBuffer compressed, ByteArrayOutputStream uncompressed) throws IOException {
            Inflater inflater = this.inflaterThreadLocal.get();
            inflater.reset();

            byte[] data = new byte[compressed.remaining()];
            compressed.get(data);
            inflater.setInput(data);

            byte[] buffer = BUFFERS.get();
            int read;
            int count = 0;

            try {
                while ((read = inflater.inflate(buffer)) != 0) {
                    uncompressed.write(buffer, 0, read);
                    count += read;
                }
            } catch (DataFormatException e) {
                throw new IOException("Could not decompress data: ", e);
            }

            return count;
        }

        public int uncompress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException {
            Inflater inflater = this.inflaterThreadLocal.get();
            inflater.reset();

            return copy(
                    new InflaterInputStream(new ByteArrayInputStream(input, inputOffset,
                            length), inflater),
                    new ByteBufferBackedOutputStream(ByteBuffer.wrap(output,
                            outputOffset, output.length - outputOffset)));
        }

        public int compress(byte[] input, int inputOffset, int length, byte[] output, int outputOffset)
                throws IOException {
            Deflater deflater = this.deflaterThreadLocal.get();
            deflater.reset();

            // TODO: parameters of Deflater to match MCPE expectations.
            ByteBufferBackedOutputStream stream = new ByteBufferBackedOutputStream(ByteBuffer.wrap(output,
                    outputOffset, output.length - outputOffset));

            return copy(
                    new DeflaterInputStream(new ByteArrayInputStream(input, inputOffset,
                            length), deflater),
                    stream
            );
        }

        public byte[] compress(String text) throws IOException {
            Deflater deflater = this.deflaterThreadLocal.get();
            deflater.reset();

            byte[] input = text.getBytes();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // TODO: parameters of Deflater to match MCPE expectations.
            copy(new DeflaterInputStream(new ByteArrayInputStream(input, 0,
                    input.length), deflater), baos);
            return baos.toByteArray();
        }
    }
}