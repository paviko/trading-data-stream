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
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static com.limemojito.trading.model.bar.Bar.Period.H1;
import static com.limemojito.trading.model.bar.Bar.Period.H4;
import static com.limemojito.trading.model.bar.Bar.Period.M10;
import static com.limemojito.trading.model.bar.Bar.Period.M30;
import static com.limemojito.trading.model.bar.Bar.Period.M5;
import static com.limemojito.trading.model.tick.dukascopy.DukascopyUtils.setupValidator;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Slf4j
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
    @SuppressWarnings("resource")
    public void shouldFailIfEndPastTheBeginningOfTime() {
        assertThat(search.getBeginningOfTime()).isEqualTo("2010-01-01T00:00:00Z");
        search.setBeginningOfTime(Instant.parse("2018-01-01T00:00:00Z"));
        assertThat(search.getBeginningOfTime()).isEqualTo("2018-01-01T00:00:00Z");
        Instant start = Instant.parse("2009-01-02T00:59:59Z");
        Instant end = Instant.parse("2020-01-02T00:00:00Z");
        String expectedMessage = "Start 2009-01-02T00:59:59Z must be before 2018-01-01T00:00:00Z";
        assertPastTheBeginningOfTime(expectedMessage,
                                     () -> search.search("EURUSD",
                                                         start,
                                                         end));
        assertPastTheBeginningOfTime(expectedMessage,
                                     () -> search.search("AUDUSD",
                                                         start,
                                                         end,
                                                         tick -> {
                                                         }));
        assertPastTheBeginningOfTime(expectedMessage,
                                     () -> search.aggregateFromTicks("USDJPY",
                                                                     H1,
                                                                     start,
                                                                     end));
        assertPastTheBeginningOfTime(expectedMessage,
                                     () -> search.aggregateFromTicks("AUDUSD",
                                                                     H1,
                                                                     start,
                                                                     end,
                                                                     bar -> {
                                                                     }));
        assertPastTheBeginningOfTime(expectedMessage,
                                     () -> search.aggregateFromTicks("EURUSD",
                                                                     H1,
                                                                     start,
                                                                     end,
                                                                     bar -> {
                                                                     },
                                                                     tick -> {
                                                                     }));
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

    @Test
    public void shouldGetBarsWithVisitor() throws Exception {
        AtomicInteger barCounter = new AtomicInteger();
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("NZDUSD",
                                                                        M5,
                                                                        Instant.parse("2020-01-02T00:00:00Z"),
                                                                        Instant.parse("2020-01-02T00:59:59Z"),
                                                                        (bar) -> barCounter.incrementAndGet())) {
            for (Bar bar : stream) {
                log.info("Counting bar {} via visitor", bar);
            }
        }
        assertThat(barCounter.get()).isEqualTo(12);
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

    private static void assertPastTheBeginningOfTime(String expectedMessage, ThrowableAssert.ThrowingCallable method) {
        assertThatThrownBy(method)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(expectedMessage);
    }
}
