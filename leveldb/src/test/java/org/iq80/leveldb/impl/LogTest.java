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

import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.FileAssert.fail;

public class LogTest {
    private static final LogMonitor NO_CORRUPTION_MONITOR = new LogMonitor() {
        @Override
        public void corruption(long bytes, String reason) {
            fail(String.format("corruption of %s bytes: %s", bytes, reason));
        }

        @Override
        public void corruption(long bytes, Throwable reason) {
            throw new RuntimeException(String.format("corruption of %s bytes: %s", bytes, reason), reason);
        }
    };

    private LogWriter writer;

    static ByteBuf toBuffer(String value) {
        return toBuffer(value, 1);
    }

    static ByteBuf toBuffer(String value, int times) {
        byte[] bytes = value.getBytes(UTF_8);
        ByteBuf buffer = Unpooled.buffer(bytes.length * times);
        for (int i = 0; i < times; i++) {
            buffer.writeBytes(bytes);
        }
        return buffer;
    }

    @Test
    public void testEmptyBlock()
            throws Exception {
        testLog();
    }

    @Test
    public void testSmallRecord()
            throws Exception {
        testLog(toBuffer("dain sundstrom"));
    }

    @Test
    public void testMultipleSmallRecords()
            throws Exception {
        List<ByteBuf> records = asList(
                toBuffer("Lagunitas  Little Sumpin’ Sumpin’"),
                toBuffer("Lagunitas IPA"),
                toBuffer("Lagunitas Imperial Stout"),
                toBuffer("Oban 14"),
                toBuffer("Highland Park"),
                toBuffer("Lagavulin"));

        testLog(records);
    }

    @Test
    public void testLargeRecord()
            throws Exception {
        testLog(toBuffer("dain sundstrom", 4000));
    }

    @Test
    public void testMultipleLargeRecords()
            throws Exception {
        List<ByteBuf> records = asList(
                toBuffer("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                toBuffer("Lagunitas IPA", 4000),
                toBuffer("Lagunitas Imperial Stout", 4000),
                toBuffer("Oban 14", 4000),
                toBuffer("Highland Park", 4000),
                toBuffer("Lagavulin", 4000));

        testLog(records);
    }

    @Test
    public void testReadWithoutProperClose()
            throws Exception {
        testLog(ImmutableList.of(toBuffer("something"), toBuffer("something else")), false);
    }

    private void testLog(ByteBuf... entries)
            throws IOException {
        testLog(asList(entries));
    }

    private void testLog(List<ByteBuf> records)
            throws IOException {
        testLog(records, true);
    }

    private void testLog(List<ByteBuf> records, boolean closeWriter)
            throws IOException {
        for (ByteBuf entry : records) {
            writer.addRecord(entry, false);
        }

        if (closeWriter) {
            writer.close();
        }

        // test readRecord

        try (LogReader reader = new LogReader(writer.getPath(), NO_CORRUPTION_MONITOR, true, 0)) {
            for (ByteBuf expected : records) {
                ByteBuf actual = reader.readRecord();
                NativeInteropTest.assertEquals(actual, expected);
            }
            assertNull(reader.readRecord());
        }
    }

    @BeforeMethod
    public void setUp()
            throws Exception {
        writer = Logs.createLogWriter(Files.createTempFile("table", ".log"), 42);
    }

    @AfterMethod
    public void tearDown()
            throws Exception {
        if (writer != null) {
            writer.delete();
        }
    }
}
