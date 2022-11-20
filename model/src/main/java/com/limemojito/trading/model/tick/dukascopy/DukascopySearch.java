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
import com.limemojito.trading.model.TradingSearch;
import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.bar.BarInputStreamToCsv;
import com.limemojito.trading.model.bar.BarVisitor;
import com.limemojito.trading.model.bar.TickToBarInputStream;
import com.limemojito.trading.model.tick.Tick;
import com.limemojito.trading.model.tick.TickVisitor;
import com.limemojito.trading.model.tick.dukascopy.cache.DirectDukascopyNoCache;
import com.limemojito.trading.model.tick.dukascopy.cache.LocalDukascopyCache;
import com.limemojito.trading.model.tick.dukascopy.criteria.BarCriteria;
import com.limemojito.trading.model.tick.dukascopy.criteria.Criteria;
import com.limemojito.trading.model.tick.dukascopy.criteria.TickCriteria;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.validation.Validator;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class DukascopySearch implements TradingSearch {
    /**
     * Defaulting the beginning of Dukascopy searches to be 2010.  This puts a limit on recursive searching.
     */
    public static final String DEFAULT_BEGINNING_OF_TIME = "2010-01-01T00:00:00Z";
    @Setter
    @Getter
    private Instant theBeginningOfTime = Instant.parse(DEFAULT_BEGINNING_OF_TIME);
    private final Validator validator;
    private final DukascopyCache cache;
    private final DukascopyPathGenerator pathGenerator;

    /**
     * A simple search using local cache, generating bars
     *
     * @param args symbol, period, start (yyyy-MM-ddTHH:mm:ssZ), end (yyyy-MM-ddTHH:mm:ssZ)
     * @throws IOException on an IO failure.
     */
    @SuppressWarnings("MagicNumber")
    public static void main(String... args) throws IOException {
        final Validator theValidator = DukascopyUtils.setupValidator();
        DukascopySearch search = new DukascopySearch(theValidator,
                                                     new LocalDukascopyCache(new DirectDukascopyNoCache()),
                                                     new DukascopyPathGenerator());
        final String symbol = args[0];
        final Bar.Period period = Bar.Period.valueOf(args[1]);
        final Instant startTime = Instant.parse(args[2]);
        final Instant endTime = Instant.parse(args[3]);
        final int initialSize = 32 * 1024;
        final StringWriter stringWriter = new StringWriter(initialSize);
        try (TradingInputStream<Bar> ticks = search.aggregateFromTicks(symbol, period, startTime, endTime);
             BarInputStreamToCsv barCsv = new BarInputStreamToCsv(ticks, stringWriter)) {
            barCsv.convert();
        }
        System.out.println();
        System.out.println(stringWriter);
    }

    @Override
    public TradingInputStream<Tick> search(String symbol, Instant startTime, Instant endTime, TickVisitor tickVisitor) {
        final TickCriteria tickCriteria = buildTickCriteria(symbol, startTime, endTime);
        final List<String> paths = pathGenerator.generatePaths(symbol, startTime, endTime);
        return generateTickInputStreamFrom(tickCriteria, paths, tickVisitor);
    }

    @Override
    public TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                      Bar.Period period,
                                                      Instant startTime,
                                                      Instant endTime,
                                                      BarVisitor barVisitor,
                                                      TickVisitor tickVisitor) {
        BarCriteria barCriteria = buildBarCriteria(symbol, period, startTime, endTime);
        log.debug("Forming bar stream for {} {} {} -> {}", symbol, period, startTime, endTime);
        final List<TradingInputStream<Bar>> barInputStreams = new LinkedList<>();
        final List<List<String>> groupedPaths = pathGenerator.generatePathsGroupedByDay(symbol, startTime, endTime);
        for (List<String> dayOfPaths : groupedPaths) {
            final TradingInputStream<Tick> dayOfTicks = generateTickInputStreamFrom(barCriteria,
                                                                                    dayOfPaths,
                                                                                    tickVisitor);
            final TradingInputStream<Bar> oneDayOfBarStream = new TickToBarInputStream(validator,
                                                                                       period,
                                                                                       barVisitor,
                                                                                       dayOfTicks);
            barInputStreams.add(oneDayOfBarStream);
        }
        log.info("Returning bar stream for {} {} {} -> {}", symbol, period, startTime, endTime);
        return TradingInputStream.combine(barInputStreams.iterator(),
                                          bar -> bar.getStartInstant().compareTo(startTime) >= 0
                                                  && bar.getStartInstant().compareTo(endTime) <= 0);
    }

    private TickCriteria buildTickCriteria(String symbol, Instant startTime, Instant endTime) {
        assertCriteriaTimes(startTime, endTime);
        return new TickCriteria(symbol, startTime, endTime);
    }

    private BarCriteria buildBarCriteria(String symbol, Bar.Period period, Instant startTime, Instant endTime) {
        assertCriteriaTimes(startTime, endTime);
        return new BarCriteria(symbol, period, startTime, endTime);
    }

    private TradingInputStream<Tick> generateTickInputStreamFrom(Criteria criteria,
                                                                 List<String> paths,
                                                                 TickVisitor tickVisitor) {
        final Iterator<String> pathIterator = paths.iterator();
        final Iterator<TradingInputStream<Tick>> tickStreamIterator = new Iterator<>() {
            @Override
            public boolean hasNext() {
                return pathIterator.hasNext();
            }

            @Override
            public TradingInputStream<Tick> next() {
                return new DukascopyTickInputStream(validator, cache, pathIterator.next(), tickVisitor);
            }
        };
        log.info("Returning tick stream for {} {} -> {}",
                 criteria.getSymbol(),
                 paths.get(0),
                 paths.get(paths.size() - 1));
        return TradingInputStream.combine(tickStreamIterator, tick -> filterAgainst(criteria, tick));
    }

    private void assertCriteriaTimes(Instant startTime, Instant endTime) {
        Criteria.assertBeforeStart(startTime, endTime);
        if (startTime.isBefore(theBeginningOfTime)) {
            throw new IllegalArgumentException(String.format("Start %s must be after %s",
                                                             startTime,
                                                             theBeginningOfTime));
        }
    }

    private static boolean filterAgainst(Criteria barCriteria, Tick tick) {
        Instant tickInstant = tick.getInstant();
        Instant criteriaStart = barCriteria.getStart();
        Instant criteriaEnd = barCriteria.getEnd();
        return (tickInstant.equals(criteriaStart) || tickInstant.isAfter(criteriaStart))
                && (tickInstant.isBefore(criteriaEnd) || tickInstant.equals(criteriaEnd));
    }
}
