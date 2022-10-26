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
import com.limemojito.trading.model.tick.dukascopy.DukascopyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("resource")
public class LocalDukascopyCacheTest {
    private final String dukascopyTickPath = "EURUSD/2018/06/05/05h_ticks.bi5";

    @Mock
    private DukascopyCache fallbackMock;
    private LocalDukascopyCache cache;

    @BeforeEach
    void setUp() throws IOException {
        cache = new LocalDukascopyCache(fallbackMock);
        cache.removeCache();
    }

    @AfterEach
    void cleanUp() throws IOException {
        assertThat(cache.getCacheSizeBytes()).isGreaterThan(33000L);
        cache.removeCache();
        verifyNoMoreInteractions(fallbackMock);
    }

    @Test
    public void shouldPullFromFallbackWhenMissingFromLocalCache() throws IOException {
        doReturn(validInputStream()).when(fallbackMock).stream(dukascopyTickPath);
        doReturn("mock cache").when(fallbackMock).cacheStats();

        try (InputStream stream = cache.stream(dukascopyTickPath)) {
            assertStreamResult(stream, 0, 1);
        }

        assertThat(cache.cacheStats()).isEqualTo("LocalDukascopyCache 1 0h 1m 0.00% -> (mock cache)");
    }

    @Test
    public void shouldHitCacheWhenAlreadyRetrieved() throws Exception {
        doReturn(validInputStream()).when(fallbackMock).stream(dukascopyTickPath);
        InputStream stream = cache.stream(dukascopyTickPath);
        stream.close();


        try (InputStream stream2 = cache.stream(dukascopyTickPath)) {
            assertStreamResult(stream2, 1, 1);
        }
    }

    private void assertStreamResult(InputStream stream, int hits, int misses) {
        assertThat(stream).isNotNull();
        assertThat(cache.getHitCount()).isEqualTo(hits);
        assertThat(cache.getMissCount()).isEqualTo(misses);
        assertThat(cache.getRetrieveCount()).isEqualTo(hits + misses);
    }

    private InputStream validInputStream() throws IOException {
        return new FileInputStream(DukascopyUtils.dukascopyClassResourceToTempFile("/" + dukascopyTickPath));
    }
}
