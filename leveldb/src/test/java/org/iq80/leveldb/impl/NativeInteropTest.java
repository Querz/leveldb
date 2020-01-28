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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.iq80.leveldb.*;
import org.iq80.leveldb.util.Buffers;
import org.iq80.leveldb.util.FileUtils;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class NativeInteropTest {
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final Path databaseDir;

    {
        try {
            databaseDir = Files.createTempDirectory("leveldb");
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private final DBFactory iq80factory = Iq80DBFactory.factory;
    private final DBFactory jnifactory;

    public NativeInteropTest() {
        DBFactory jnifactory = Iq80DBFactory.factory;
        try {
            ClassLoader cl = NativeInteropTest.class.getClassLoader();
            jnifactory = (DBFactory) cl.loadClass("org.fusesource.leveldbjni.JniDBFactory").newInstance();
        } catch (Throwable e) {
            // We cannot create a JniDBFactory on windows :( so just use a Iq80DBFactory for both
            // to avoid test failures.
        }
        this.jnifactory = jnifactory;
    }

    public static ByteBuf buffer(String value) {
        return Buffers.encodeString(value);
    }

    public static byte[] bytes(String value) {
        if (value == null) {
            return null;
        }
        try {
            return value.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static String asString(byte[] value) {
        if (value == null) {
            return null;
        }
        try {
            return new String(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void assertEquals(ByteBuf arg1, ByteBuf arg2) {
        assertTrue(arg1 != null && ByteBufUtil.equals(arg1, arg2),
                (arg1 == null ? "null" : arg1.toString(UTF_8)) + " != " + arg2.toString(UTF_8));
    }

    Path getTestDirectory(String name)
            throws IOException {
        Path rc = databaseDir.resolve(name);
        iq80factory.destroy(rc, new Options().createIfMissing(true));
        Files.createDirectories(rc);
        return rc;
    }

    @Test
    public void testCRUDviaIQ80()
            throws IOException, DBException {
        crud(iq80factory, iq80factory);
    }

    @Test
    public void testCRUDviaJNI()
            throws IOException, DBException {
        crud(jnifactory, jnifactory);
    }

    @Test
    public void testCRUDviaIQ80thenJNI()
            throws IOException, DBException {
        crud(iq80factory, jnifactory);
    }

    @Test
    public void testCRUDviaJNIthenIQ80()
            throws IOException, DBException {
        crud(jnifactory, iq80factory);
    }

    public void crud(DBFactory firstFactory, DBFactory secondFactory)
            throws IOException, DBException {
        Options options = new Options().createIfMissing(true);

        Path path = getTestDirectory(getClass().getName() + "_" + NEXT_ID.incrementAndGet());
        DB db = firstFactory.open(path, options);

        WriteOptions wo = new WriteOptions().sync(false);
        ReadOptions ro = new ReadOptions().fillCache(true).verifyChecksums(true);
        db.put(bytes("Tampa"), buffer("green"));
        db.put(bytes("London"), buffer("red"));
        db.put(bytes("New York"), buffer("blue"));

        db.close();
        db = secondFactory.open(path, options);

        assertEquals(db.get(bytes("Tampa"), ro), buffer("green"));
        assertEquals(db.get(bytes("London"), ro), buffer("red"));
        assertEquals(db.get(bytes("New York"), ro), buffer("blue"));

        db.delete(bytes("New York"), wo);

        assertEquals(db.get(bytes("Tampa"), ro), buffer("green"));
        assertEquals(db.get(bytes("London"), ro), buffer("red"));
        assertNull(db.get(bytes("New York"), ro));

        db.close();
        db = firstFactory.open(path, options);

        assertEquals(db.get(bytes("Tampa"), ro), buffer("green"));
        assertEquals(db.get(bytes("London"), ro), buffer("red"));
        assertNull(db.get(bytes("New York"), ro));

        db.close();
    }
}
