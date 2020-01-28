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
import io.netty.buffer.ByteBufUtil;

import java.nio.CharBuffer;
import java.nio.charset.*;

public final class Buffers {

    private Buffers() {
    }

    public static ByteBuf readLengthPrefixedBytes(ByteBuf buffer) {
        int length = VariableLengthQuantity.readVariableLengthInt(buffer);
        return buffer.readSlice(length);
    }

    public static void writeLengthPrefixedBytes(ByteBuf buffer, ByteBuf value) {
        VariableLengthQuantity.writeVariableLengthInt(value.readableBytes(), buffer);
        buffer.writeBytes(value);
    }

    public static ByteBuf encodeString(String string) {
        return ByteBufUtil.encodeString(ByteBufAllocator.DEFAULT, CharBuffer.wrap(string), StandardCharsets.UTF_8);
    }

    public static byte[] getBytes(ByteBuf buffer) {
        byte[] bytes = new byte[buffer.readableBytes()];
        buffer.getBytes(buffer.readerIndex(), bytes);
        return bytes;
    }
}
