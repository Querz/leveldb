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

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Iq80DBFactory
        implements DBFactory {
    public static final boolean IS_64_BIT = is64bit();
    // We only use MMAP on 64 bit systems since it's really easy to run out of
    // virtual address space on a 32 bit system when all the data is getting mapped
    // into memory.  If you really want to use MMAP anyways, use -Dleveldb.mmap=true
    public static final boolean USE_MMAP = Boolean.parseBoolean(System.getProperty("leveldb.mmap", Boolean.toString(IS_64_BIT)));
    public static final String VERSION = getVersion();
    public static final Iq80DBFactory factory = new Iq80DBFactory();

    private static boolean is64bit() {
        boolean is64bit;
        if (System.getProperty("os.name").contains("Windows")) {
            is64bit = System.getenv("ProgramFiles(x86)") != null;
        } else {
            is64bit = System.getProperty("os.arch").contains("64");
        }
        return is64bit;
    }

    private static String getVersion() {
        String version = "unknown";
        InputStream is = Iq80DBFactory.class.getResourceAsStream("version.txt");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            version = reader.readLine();
        } catch (IOException e) {
            // ignore
        }
        return version;
    }

    public static byte[] bytes(String value) {
        if (value == null) {
            return null;
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String asString(byte[] value) {
        if (value == null) {
            return null;
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    @Override
    public DB open(File path, Options options) throws IOException {
        return new DbImpl(options, path);
    }

    @Override
    public void destroy(File path, Options options) throws IOException {
        // TODO: This should really only delete leveldb-created files.
        FileUtils.deleteRecursively(path);
    }

    @Override
    public void repair(File path, Options options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format("iq80 leveldb version %s", VERSION);
    }
}
