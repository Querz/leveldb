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
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.iq80.leveldb.WriteBatch;

import java.util.List;
import java.util.Map.Entry;

import static com.google.common.collect.Lists.newArrayList;

public class WriteBatchImpl implements WriteBatch {
    private final List<Entry<ByteBuf, ByteBuf>> batch = newArrayList();
    private int approximateSize;

    public int getApproximateSize() {
        return approximateSize;
    }

    public int size() {
        return batch.size();
    }

    @Override
    public WriteBatchImpl put(byte[] key, ByteBuf value) {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        batch.add(Maps.immutableEntry(Unpooled.wrappedBuffer(key), value));
        approximateSize += 12 + key.length + value.readableBytes();
        return this;
    }

    public WriteBatchImpl put(ByteBuf key, ByteBuf value) {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        batch.add(Maps.immutableEntry(key, value));
        approximateSize += 12 + key.readableBytes() + value.readableBytes();
        return this;
    }

    @Override
    public WriteBatchImpl delete(byte[] key) {
        Preconditions.checkNotNull(key, "key is null");
        batch.add(Maps.immutableEntry(Unpooled.wrappedBuffer(key), null));
        approximateSize += 6 + key.length;
        return this;
    }

    public WriteBatchImpl delete(ByteBuf key) {
        Preconditions.checkNotNull(key, "key is null");
        batch.add(Maps.immutableEntry(key, null));
        approximateSize += 6 + key.readableBytes();
        return this;
    }

    @Override
    public void close() {
    }

    public void forEach(Handler handler) {
        for (Entry<ByteBuf, ByteBuf> entry : batch) {
            ByteBuf key = entry.getKey();
            ByteBuf value = entry.getValue();
            if (value != null) {
                handler.put(key, value);
            } else {
                handler.delete(key);
            }
        }
    }

    public interface Handler {
        void put(ByteBuf key, ByteBuf value);

        void delete(ByteBuf key);
    }
}
