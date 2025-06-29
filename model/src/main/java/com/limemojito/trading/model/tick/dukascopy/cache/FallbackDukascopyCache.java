/*
 * Copyright 2011-2024 Lime Mojito Pty Ltd
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

import com.amazonaws.util.IOUtils;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class FallbackDukascopyCache implements DukascopyCache {

    @Getter(AccessLevel.PROTECTED)
    private final DukascopyCache fallback;
    private final AtomicInteger cacheMiss;
    private final AtomicInteger cacheHit;
    private final AtomicInteger retrieveCount;

    public FallbackDukascopyCache(DukascopyCache fallback) {
        this.fallback = fallback;
        this.cacheMiss = new AtomicInteger();
        this.cacheHit = new AtomicInteger();
        this.retrieveCount = new AtomicInteger();
    }

    @Override
    public InputStream stream(String dukascopyPath) throws IOException {
        InputStream stream = checkCache(dukascopyPath);
        if (stream == null) {
            cacheMiss.incrementAndGet();
            stream = new ByteArrayInputStream(saveDataFromFallback(dukascopyPath));
        } else {
            cacheHit.incrementAndGet();
        }
        retrieveCount.incrementAndGet();
        return stream;
    }

    @Override
    public int getHitCount() {
        return cacheHit.get();
    }

    @Override
    public int getMissCount() {
        return cacheMiss.get();
    }

    @Override
    public int getRetrieveCount() {
        return retrieveCount.get();
    }

    @Override
    public String cacheStats() {
        final double toPercent = 100.0;
        return String.format(java.util.Locale.US, "%s %d %dh %dm %.2f%% -> (%s)",
                             getClass().getSimpleName(),
                             getRetrieveCount(),
                             getHitCount(),
                             getMissCount(),
                             getRetrieveCount() > 0 ? (getHitCount() / (double) getRetrieveCount()) * toPercent : 0,
                             fallback.cacheStats());
    }

    protected abstract void saveToCache(String dukascopyPath, InputStream input) throws IOException;

    /**
     * @param dukascopyPath path to check in cache
     * @return null if not present
     * @throws IOException on an io failure.
     */
    protected abstract InputStream checkCache(String dukascopyPath) throws IOException;

    private byte[] saveDataFromFallback(String dukascopyPath) throws IOException {
        try (InputStream fallbackStream = fallback.stream(dukascopyPath)) {
            final byte[] data = IOUtils.toByteArray(fallbackStream);
            try (InputStream input = new ByteArrayInputStream(data)) {
                saveToCache(dukascopyPath, input);
            }
            return data;
        }
    }
}
