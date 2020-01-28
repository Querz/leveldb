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
import io.netty.buffer.Unpooled;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.util.Buffers;

public class CustomUserComparator implements UserComparator {
    private final DBComparator comparator;

    public CustomUserComparator(DBComparator comparator) {
        this.comparator = comparator;
    }

    @Override
    public String name() {
        return comparator.name();
    }

    @Override
    public ByteBuf findShortestSeparator(ByteBuf start, ByteBuf limit) {
        return Unpooled.wrappedBuffer(comparator.findShortestSeparator(Buffers.getBytes(start), Buffers.getBytes(limit)));
    }

    @Override
    public ByteBuf findShortSuccessor(ByteBuf key) {
        return Unpooled.wrappedBuffer(comparator.findShortSuccessor(Buffers.getBytes(key)));
    }

    @Override
    public int compare(ByteBuf o1, ByteBuf o2) {
        return comparator.compare(Buffers.getBytes(o1), Buffers.getBytes(o2));
    }
}
