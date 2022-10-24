/*
 * Copyright 2011-2022 Lime Mojito Pty Ltd
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package com.limemojito.trading.model.tick.dukascopy.cache;

import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.System.getProperty;

/**
 * This can be mixed with other cache strategies to pipeline  For example:
 * <p>
 * <code>DukascopyCache cache = new LocalDukascopyCache(new S3DukascopyCache(s3, "myBucket", new NoCacheDirectDukascopy()))</code>
 */
@Slf4j
public class LocalDukascopyCache extends FallbackDukascopyCache {
    /**
     * Property for overriding local cache location.  Defaults to "user.home"/.dukascopy/.
     */
    public static final String PROP_DIR = DirectDukascopy.class.getPackageName() + ".localCacheDir";

    private static final Path CACHE_DIR = new File(getProperty(PROP_DIR, getProperty("user.home")),
                                                   ".dukascopy").toPath();

    public LocalDukascopyCache() {
        this(new DirectDukascopy());
    }

    public LocalDukascopyCache(DukascopyCache fallback) {
        super(fallback);
        if (CACHE_DIR.toFile().mkdir()) {
            log.info("Created local cache at {}", CACHE_DIR);
        }
    }

    public long getCacheSizeBytes() throws IOException {
        try (Stream<Path> walk = Files.walk(CACHE_DIR)) {
            final Optional<Long> size = walk.map(Path::toFile)
                                            .map(File::length)
                                            .reduce(Long::sum);
            return size.orElse(0L);
        }
    }

    public void removeCache() throws IOException {
        log.info("Removing cache at {}", CACHE_DIR);
        try (Stream<Path> walk = Files.walk(CACHE_DIR)) {
            //noinspection ResultOfMethodCallIgnored
            walk.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }

    @Override
    protected void saveToCache(String dukascopyPath, InputStream input) throws IOException {
        Path cachePath = Path.of(CACHE_DIR.toString(), dukascopyPath);
        //noinspection ResultOfMethodCallIgnored
        cachePath.toFile().getParentFile().mkdirs();
        Files.copy(input, cachePath);
    }

    @Override
    protected InputStream checkCache(String dukascopyPath) throws IOException {
        final File file = Path.of(CACHE_DIR.toString(), dukascopyPath).toFile();
        if (file.isFile()) {
            return new FileInputStream(file);
        }
        return null;
    }

}
