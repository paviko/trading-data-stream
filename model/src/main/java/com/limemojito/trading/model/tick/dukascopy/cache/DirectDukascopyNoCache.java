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

import com.google.common.util.concurrent.RateLimiter;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.util.concurrent.RateLimiter.create;
import static java.lang.Double.parseDouble;
import static java.lang.System.getProperty;

/**
 * This is no caching and a direct call to dukascopy.  Rate limited to work with dukascopy servers.
 */
@Slf4j
@RequiredArgsConstructor
@SuppressWarnings("UnstableApiUsage")
public class DirectDukascopyNoCache implements DukascopyCache {
    /**
     * Defaults to 2ps which plays nicely with Dukascopy.  Otherwise, they simply stop responding (500) if you hit the
     * servers too hard.
     */
    public static final String PROP_PERMITS = DirectDukascopyNoCache.class.getPackageName() + ".permits";
    /**
     * Defaults to <a href="https://datafeed.dukascopy.com/datafeed/">...</a> which plays nicely with Dukascopy.  Otherwise, they simply stop
     * responding if you hit the servers too hard.  Note the slash on the end is required.
     */
    public static final String PROP_URL = DirectDukascopyNoCache.class.getPackageName() + ".url";
    private static final double PERMITS_PER_SECOND = parseDouble(getProperty(PROP_PERMITS, "2.0"));
    private static final RateLimiter RATE_LIMITER = create(PERMITS_PER_SECOND);

    private static final String DUKASCOPY_URL = getProperty(PROP_URL, "https://datafeed.dukascopy.com/datafeed/");
    private final AtomicInteger retrievePathCounter = new AtomicInteger();

    @Override
    public InputStream stream(String dukascopyPath) throws IOException {
        // play nice with Dukascopy's free data.  And if you don't they stop sending data.
        final double waited = RATE_LIMITER.acquire();
        final String url = DUKASCOPY_URL + dukascopyPath;
        log.info("Loading from {}, waited {}s", url, waited);
        BufferedInputStream stream = new BufferedInputStream(new URL(url).openStream());
        retrievePathCounter.incrementAndGet();
        return stream;
    }

    @Override
    public int getRetrieveCount() {
        return retrievePathCounter.get();
    }

    @Override
    public int getHitCount() {
        return 0;
    }

    @Override
    public int getMissCount() {
        return getRetrieveCount();
    }

    @Override
    public String cacheStats() {
        return String.format("DirectDukascopyNoCache: %d retrieve(s)", getRetrieveCount());
    }
}
