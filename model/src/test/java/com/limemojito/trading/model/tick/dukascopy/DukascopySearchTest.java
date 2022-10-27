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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
        assertThat(search.getTheBeginningOfTime()).isEqualTo("2010-01-01T00:00:00Z");
        search.setTheBeginningOfTime(Instant.parse("2018-01-01T00:00:00Z"));
        assertThat(search.getTheBeginningOfTime()).isEqualTo("2018-01-01T00:00:00Z");
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
    public void shouldAggregateAcrossNoDataSpans() throws Exception {
        try (TradingInputStream<Bar> eurusd = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        Instant.parse("2018-12-31T00:00:00Z"),
                                                                        Instant.parse("2019-01-01T10:00:00Z"))) {
            AtomicInteger count = new AtomicInteger();
            eurusd.forEach(bar -> {
                log.info("Found bar @ {}", bar.getStartInstant());
                count.incrementAndGet();
            });
            assertThat(count.get()).isEqualTo(23);
        }
    }

    @Test
    public void shouldBarCountForwards() throws Exception {
        int expectedBarCount = 10;
        List<Bar> bars;
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        Instant.parse("2019-01-04T18:00:00Z"),
                                                                        expectedBarCount)) {
            bars = stream.stream().collect(Collectors.toList());
        }
        assertThat(bars).hasSize(expectedBarCount);
        // this has run over a weekend gap
        assertThat(bars.get(0).getStartInstant()).isEqualTo("2019-01-04T18:00:00Z");
        assertThat(bars.get(expectedBarCount - 1).getStartInstant()).isEqualTo("2019-01-07T03:00:00Z");
        assertThat(bars.get(expectedBarCount - 1).getStartInstant()).isEqualTo("2019-01-07T03:00:00Z");
    }

    @Test
    public void shouldBarCountBackwardsThroughAWeekend() throws Exception {
        int expectedBarCount = 10;
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        expectedBarCount,
                                                                        Instant.parse("2019-06-17T01:59:59Z"))) {
            List<Bar> data = stream.stream().collect(Collectors.toList());
            data.forEach(bar -> log.info("Found bar @ {}", bar.getStartInstant()));
            assertThat(data.size()).isEqualTo(expectedBarCount);
            assertThat(data.get(0).getStartInstant()).isEqualTo("2019-06-14T16:00:00Z");
            assertThat(data.get(expectedBarCount - 1).getStartInstant()).isEqualTo("2019-06-17T01:00:00Z");
        }
    }

    @Test
    public void shouldStopAtTheBeginningOfTime() throws Exception {
        int expectedBarCount = 5;
        List<Bar> bars;
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        100,
                                                                        Instant.parse("2010-01-01T05:00:00Z"))) {
            bars = stream.stream().collect(Collectors.toList());
        }
        assertThat(bars).hasSize(expectedBarCount);
        // this has run over a weekend gap
        assertThat(bars.get(0).getStartInstant()).isEqualTo("2010-01-01T00:00:00Z");
        assertThat(bars.get(expectedBarCount - 1).getStartInstant()).isEqualTo("2010-01-01T04:00:00Z");
    }

    @Test
    public void shouldCountBackwardsWithBarVisitor() throws Exception {
        int expectedBarCount = 5;
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        expectedBarCount,
                                                                        Instant.parse("2019-04-08T18:00:00Z"),
                                                                        bar -> log.info("Visited {}", bar))) {
            assertThat(stream.stream().count()).isEqualTo(expectedBarCount);
        }
    }

    @Test
    public void shouldCountForwardsWithBarVisitor() throws Exception {
        int expectedBarCount = 5;
        try (TradingInputStream<Bar> stream = search.aggregateFromTicks("EURUSD",
                                                                        H1,
                                                                        Instant.parse("2019-04-08T13:00:00Z"),
                                                                        expectedBarCount,
                                                                        bar -> log.info("Visited {}", bar))) {
            assertThat(stream.stream().count()).isEqualTo(expectedBarCount);
        }
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
