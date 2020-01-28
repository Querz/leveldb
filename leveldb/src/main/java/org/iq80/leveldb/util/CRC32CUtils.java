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

import com.nukkitx.natives.crc32c.Crc32C;
import com.nukkitx.natives.util.Natives;
import io.netty.buffer.ByteBuf;

public class CRC32CUtils {

    private static final int MASK_DELTA = 0xa282ead8;

    public static final ThreadLocal<Crc32C> CRC32C = ThreadLocal.withInitial(Natives.CRC32C::get);

    private CRC32CUtils() {
    }

    public static int getChunkChecksum(int chunkTypeId, ByteBuf buffer) {
        return getChunkChecksum(chunkTypeId, buffer, buffer.readableBytes());
    }

    public static int getChunkChecksum(int chunkTypeId, ByteBuf buffer, int length) {
        Crc32C crc32c = CRC32C.get();

        crc32c.reset();
        crc32c.update(chunkTypeId);
        crc32c.update(buffer.internalNioBuffer(buffer.readerIndex(), buffer.readerIndex() + length));
        return mask((int) crc32c.getValue());
    }

    /**
     * Return a masked representation of crc.
     * <p/>
     * Motivation: it is problematic to compute the CRC of a string that
     * contains embedded CRCs.  Therefore we recommend that CRCs stored
     * somewhere (e.g., in files) should be masked before being stored.
     */
    public static int mask(int crc) {
        // Rotate right by 15 bits and add a constant.
        return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    }
}
