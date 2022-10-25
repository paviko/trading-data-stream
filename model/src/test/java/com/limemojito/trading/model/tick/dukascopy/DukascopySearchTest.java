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

package com.limemojito.trading.model.tick.dukascopy;

import com.limemojito.trading.model.TradingInputStream;
import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.tick.Tick;
import com.limemojito.trading.model.tick.dukascopy.cache.DirectDukascopyNoCache;
import com.limemojito.trading.model.tick.dukascopy.cache.LocalDukascopyCache;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.io.IOException;
import java.time.Instant;

import static com.limemojito.trading.model.bar.Bar.Period.H1;
import static com.limemojito.trading.model.bar.Bar.Period.H4;
import static com.limemojito.trading.model.bar.Bar.Period.M10;
import static com.limemojito.trading.model.bar.Bar.Period.M30;
import static com.limemojito.trading.model.bar.Bar.Period.M5;
import static com.limemojito.trading.model.tick.dukascopy.DukascopyUtils.setupValidator;
import static org.assertj.core.api.Assertions.assertThat;

public class DukascopySearchTest {

    private static final Validator VALIDATOR = setupValidator();
    private DukascopySearch search;

    @BeforeEach
    void setUp() {
        DukascopyCache cacheChain = new LocalDukascopyCache(new DirectDukascopyNoCache());
        DukascopyPathGenerator pathGenerator = new DukascopyPathGenerator();
        search = new DukascopySearch(VALIDATOR, cacheChain, pathGenerator);
    }

    @Test
    public void shouldSearchForTicksAndFilter() throws Exception {
        searchTicksExpect("EURUSD", "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 1268L);
        searchTicksExpect("USDJPY", "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 994L);
        searchTicksExpect("EURUSD", "2020-01-02T00:00:00Z", "2020-01-02T00:29:59Z", 717L);
    }

    @Test
    public void shouldHandleMultiStream() throws Exception {
        searchTicksExpect("EURUSD", "2020-01-03T00:00:00Z", "2020-01-04T00:59:59Z", 87468L);
    }

    @Test
    public void shouldHandleExpansionToEndOfSeconds() throws Exception {
        long expectedIncludingEnd = 1268L;
        searchTicksExpect("EURUSD", "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", expectedIncludingEnd);
        searchTicksExpect("EURUSD", "2020-01-02T00:00:00Z", "2020-01-02T00:59:59.999Z", expectedIncludingEnd);

    }

    @Test
    public void shouldAggregateBars() throws Exception {
        searchBarsExpect("EURUSD", H1, "2019-01-02T00:00:00Z", "2019-01-02T00:59:59Z", 1);
        searchBarsExpect("USDCHF", M5, "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 12);
        searchBarsExpect("USDCHF", M10, "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 6);
        searchBarsExpect("USDCHF", M30, "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 2);
        searchBarsExpect("USDCHF", H1, "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 1);
        // note that this is a partial bar.
        searchBarsExpect("USDCHF", H4, "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z", 1);
    }

    @Test
    public void shouldWorkWithMain() throws Exception {
        DukascopySearch.main("EURUSD", "M5", "2020-01-02T00:00:00Z", "2020-01-02T00:59:59Z");
    }

    private void searchBarsExpect(String symbol,
                                  Bar.Period period,
                                  String start, String end, int expected) throws IOException {
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks(symbol,
                                                                        period,
                                                                        Instant.parse(start),
                                                                        Instant.parse(end))) {
            assertThat(stream.stream().count()).isEqualTo(expected);
        }
    }

    private void searchTicksExpect(String symbol, String start, String end, long expected) throws IOException {
        try (TradingInputStream<Tick> stream = search.search(symbol,
                                                             Instant.parse(start),
                                                             Instant.parse(end))) {
            assertThat(stream.stream().count()).isEqualTo(expected);
        }
    }

}
