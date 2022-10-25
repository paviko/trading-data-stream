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
import lombok.RequiredArgsConstructor;
import lombok.Value;
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
    private final Validator validator;
    private final DukascopyCache cache;
    private final DukascopyPathGenerator pathGenerator;

    /**
     * A simple search using local cache, generating bars
     *
     * @param args symbol, period, start (yyyy-MM-ddTHH:mm:ssZ), end (yyyy-MM-ddTHH:mm:ssZ)
     */
    @SuppressWarnings("MagicNumber")
    public static void main(String... args) throws IOException {
        final Validator theValidator = DukascopyUtils.setupValidator();
        DukascopySearch search = new DukascopySearch(theValidator,
                                                     new LocalDukascopyCache(new DirectDukascopyNoCache()),
                                                     new DukascopyPathGenerator());
        final String symbol = args[0];
        final Bar.Period period = Bar.Period.valueOf(args[1]);
        final Instant startDate = Instant.parse(args[2]);
        final Instant endTime = Instant.parse(args[3]);
        final int initialSize = 32 * 1024;
        final StringWriter stringWriter = new StringWriter(initialSize);
        try (TradingInputStream<Bar> ticks = search.aggregateFromTicks(symbol, period, startDate, endTime);
             BarInputStreamToCsv barCsv = new BarInputStreamToCsv(ticks, stringWriter)) {
            barCsv.convert();
        }
        System.out.println();
        System.out.println(stringWriter);
    }

    /**
     * This search will generate all the paths to fetch and then fetch them as they are scanned.  This implementation is fully streaming.
     *
     * @param symbol      symbol to fetch
     * @param startDate   start of day search
     * @param endTime     end of day search
     * @param tickVisitor applied to each tick encountered
     * @return A combined stream of bars.
     */
    public TradingInputStream<Tick> search(String symbol, Instant startDate, Instant endTime, TickVisitor tickVisitor) {
        final List<String> paths = pathGenerator.generatePaths(symbol, startDate, endTime);
        return generateTickInputStreamFrom(new Criteria(symbol, startDate, endTime), paths, tickVisitor);
    }

    /**
     * This search will generate all the paths to fetch and then fetch them in 1D batches (24 tick files).  Note that this is the size of data
     * that will be aggregated to generate a bar up to 1D.
     *
     * @param symbol      symbol to fetch
     * @param period      period to aggregate to
     * @param startDate   start of day search
     * @param endTime     end of day search
     * @param barVisitor  applied to each bar encountered
     * @param tickVisitor applied to each tick encountered
     * @return A combined stream of bars.
     */
    @Override
    public TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                      Bar.Period period,
                                                      Instant startDate,
                                                      Instant endTime,
                                                      BarVisitor barVisitor, TickVisitor tickVisitor) {
        final List<TradingInputStream<Bar>> barInputStreams = new LinkedList<>();
        final List<List<String>> groupedPaths = pathGenerator.generatePathsGroupedByDay(symbol, startDate, endTime);
        for (List<String> dayOfPaths : groupedPaths) {
            final TradingInputStream<Tick> dayOfTicks = generateTickInputStreamFrom(new Criteria(symbol,
                                                                                                 startDate,
                                                                                                 endTime),
                                                                                    dayOfPaths, tickVisitor);
            final TradingInputStream<Bar> oneDayOfBarStream = new TickToBarInputStream(validator,
                                                                                       period,
                                                                                       barVisitor,
                                                                                       dayOfTicks);
            barInputStreams.add(oneDayOfBarStream);
        }
        log.info("Returning bar stream for {} {} {} -> {}", symbol, period, startDate, endTime);
        return TradingInputStream.combine(barInputStreams.iterator(),
                                          bar -> bar.getStartInstant().compareTo(startDate) >= 0
                                                  && bar.getStartInstant().compareTo(endTime) <= 0);
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
        log.info("Returning tick stream for {} {} -> {}", criteria.symbol, criteria.start, criteria.end);
        return TradingInputStream.combine(tickStreamIterator, tick -> filterAgainst(criteria, tick));
    }

    private static boolean filterAgainst(Criteria criteria, Tick tick) {
        Instant tickInstant = tick.getInstant();
        Instant criteriaStart = criteria.getStart();
        Instant criteriaEnd = criteria.getEnd();
        return (tickInstant.equals(criteriaStart) || tickInstant.isAfter(criteriaStart))
                && (tickInstant.isBefore(criteriaEnd) || tickInstant.equals(criteriaEnd));
    }

    @Value
    @SuppressWarnings("RedundantModifiersValueLombok")
    private static final class Criteria {
        private Criteria(String symbol, Instant start, Instant end) {
            this.symbol = symbol;
            this.start = start;
            // if 12:45:33 we need to expand to cover the end of second.
            this.end = end.getNano() == 0 ? end.plusSeconds(1).minusNanos(1) : end;
        }

        private final String symbol;
        private final Instant start;
        private final Instant end;
    }
}
