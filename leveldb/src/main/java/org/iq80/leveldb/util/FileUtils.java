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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public final class FileUtils {
    private static final int TEMP_DIR_ATTEMPTS = 10000;

    private FileUtils() {
    }

    public static boolean isSymbolicLink(Path file) {
        return Files.isSymbolicLink(file);
    }

    public static DirectoryStream<Path> listFiles(Path dir) throws IOException {
        return Files.newDirectoryStream(dir);
    }

    public static ImmutableList<File> listFiles(File dir, FilenameFilter filter) {
        File[] files = dir.listFiles(filter);
        if (files == null) {
            return ImmutableList.of();
        }
        return ImmutableList.copyOf(files);
    }

    public static File createTempDir(String prefix) {
        return createTempDir(new File(System.getProperty("java.io.tmpdir")), prefix);
    }

    public static File createTempDir(File parentDir, String prefix) {
        String baseName = "";
        if (prefix != null) {
            baseName += prefix + "-";
        }

        baseName += System.currentTimeMillis() + "-";
        for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
            File tempDir = new File(parentDir, baseName + counter);
            if (tempDir.mkdir()) {
                return tempDir;
            }
        }
        throw new IllegalStateException("Failed to create directory within "
                + TEMP_DIR_ATTEMPTS + " attempts (tried "
                + baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
    }

    public static boolean deleteDirectoryContents(Path directory) throws IOException {
        Preconditions.checkArgument(Files.isDirectory(directory), "Not a directory: %s", directory);

        // Don't delete symbolic link directories
        if (isSymbolicLink(directory)) {
            return false;
        }

        boolean success = true;
        for (Path file : listFiles(directory)) {
            success = deleteRecursively(file) && success;
        }
        return success;
    }

    public static boolean deleteRecursively(Path file) throws IOException {
        boolean success = true;
        if (Files.isDirectory(file)) {
            success = deleteDirectoryContents(file);
        }

        Files.deleteIfExists(file);
        return success;
    }

    public static boolean copyDirectoryContents(Path src, Path target) throws IOException {
        Preconditions.checkArgument(Files.isDirectory(src), "Source dir is not a directory: %s", src);

        // Don't delete symbolic link directories
        if (isSymbolicLink(src)) {
            return false;
        }

        Files.createDirectories(target);
        Preconditions.checkArgument(Files.isDirectory(target), "Target dir is not a directory: %s", src);

        boolean success = true;
        for (Path file : listFiles(src)) {
            success = copyRecursively(file, target.resolve(file.getFileName())) && success;
        }
        return success;
    }

    public static boolean copyRecursively(Path src, Path target) throws IOException {
        if (Files.isDirectory(src)) {
            return copyDirectoryContents(src, target);
        } else {
            try {
                Files.copy(src, target);
                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    public static Path newFile(String parent, String... paths) {
        Preconditions.checkNotNull(parent, "parent is null");
        Preconditions.checkNotNull(paths, "paths is null");

        return newFile(Paths.get(parent), ImmutableList.copyOf(paths));
    }

    public static Path newFile(Path parent, String... paths) {
        Preconditions.checkNotNull(parent, "parent is null");
        Preconditions.checkNotNull(paths, "paths is null");

        return newFile(parent, ImmutableList.copyOf(paths));
    }

    public static Path newFile(Path parent, Iterable<String> paths) {
        Preconditions.checkNotNull(parent, "parent is null");
        Preconditions.checkNotNull(paths, "paths is null");

        Path result = parent;
        for (String path : paths) {
            result = result.resolve(path);
        }
        return result;
    }
}
