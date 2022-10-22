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

package com.limemojito.trading.dukascopy.cache;

import com.limemojito.trading.dukascopy.DukascopyCache;
import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;

public abstract class FallbackDukascopyCache implements DukascopyCache {
    private final DukascopyCache fallback;
    @Getter
    private int cacheMiss;
    @Getter
    private int cacheHit;

    public FallbackDukascopyCache(DukascopyCache fallback) {
        this.fallback = fallback;
    }

    public int getTotalCacheAccesses() {
        return getCacheHit() + getCacheMiss();
    }

    @Override
    public InputStream stream(String dukascopyPath) throws IOException {
        InputStream stream = checkCache(dukascopyPath);
        if (stream == null) {
            cacheMiss++;
            try (InputStream input = fallback.stream(dukascopyPath)) {
                saveToCache(dukascopyPath, input);
            }
            stream = checkCache(dukascopyPath);
        } else {
            cacheHit++;
        }
        return stream;
    }

    protected abstract void saveToCache(String dukascopyPath, InputStream input) throws IOException;

    /**
     * @param dukascopyPath path to check in cache
     * @return null if not present
     * @throws IOException on an io failure.
     */
    protected abstract InputStream checkCache(String dukascopyPath) throws IOException;
}
