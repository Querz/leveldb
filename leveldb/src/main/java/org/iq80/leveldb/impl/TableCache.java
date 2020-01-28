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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import io.netty.buffer.ByteBuf;
import org.iq80.leveldb.table.FileChannelTable;
import org.iq80.leveldb.table.MMapTable;
import org.iq80.leveldb.table.Table;
import org.iq80.leveldb.table.UserComparator;
import org.iq80.leveldb.util.Finalizer;
import org.iq80.leveldb.util.InternalTableIterator;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TableCache {
    private final LoadingCache<Long, TableAndFile> cache;
    private final Finalizer<Table> finalizer = new Finalizer<>(1);

    public TableCache(final Path databasePath, int tableCacheSize, final UserComparator userComparator, final boolean verifyChecksums) {
        Preconditions.checkNotNull(databasePath, "databaseName is null");

        cache = CacheBuilder.newBuilder()
                .maximumSize(tableCacheSize)
                .removalListener((RemovalListener<Long, TableAndFile>) notification -> {
                    Table table = notification.getValue().getTable();
                    finalizer.addCleanup(table, table.closer());
                })
                .build(CacheLoader.from(fileNumber -> {
                    try {
                        return new TableAndFile(databasePath, fileNumber, userComparator, verifyChecksums);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    public InternalTableIterator newIterator(FileMetaData file) {
        return newIterator(file.getNumber());
    }

    public InternalTableIterator newIterator(long number) {
        return new InternalTableIterator(getTable(number).iterator());
    }

    public long getApproximateOffsetOf(FileMetaData file, ByteBuf key) {
        return getTable(file.getNumber()).getApproximateOffsetOf(key);
    }

    private Table getTable(long number) {
        Table table;
        try {
            table = cache.get(number).getTable();
        } catch (ExecutionException e) {
            Throwable cause = e;
            if (e.getCause() != null) {
                cause = e.getCause();
            }
            throw new RuntimeException("Could not open table " + number, cause);
        }
        return table;
    }

    public void close() {
        cache.invalidateAll();
        finalizer.destroy();
    }

    public void evict(long number) {
        cache.invalidate(number);
    }

    public void clearBlockCaches() {
        for (Map.Entry<Long, TableAndFile> longTableAndFileEntry : cache.getAllPresent(new ArrayList<>()).entrySet()) {
            longTableAndFileEntry.getValue().getTable().clearBlockCache();
        }
    }

    private static final class TableAndFile {
        private final Table table;

        private TableAndFile(Path databasePath, long fileNumber, UserComparator userComparator, boolean verifyChecksums)
                throws IOException {
            String tableFileName = Filename.tableFileName(fileNumber);
            Path tablePath = databasePath.resolve(tableFileName);

            FileChannel fileChannel = FileChannel.open(tablePath);
            if (Iq80DBFactory.USE_MMAP) {
                table = new MMapTable(tablePath.toAbsolutePath(), fileChannel, userComparator, verifyChecksums);
            } else {
                table = new FileChannelTable(tablePath.toAbsolutePath(), fileChannel, userComparator, verifyChecksums);
            }
        }

        public Table getTable() {
            return table;
        }
    }
}
