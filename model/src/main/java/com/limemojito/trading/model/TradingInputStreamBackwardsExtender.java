/*
 * Copyright 2011-2023 Lime Mojito Pty Ltd
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

package com.limemojito.trading.model;

import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.bar.BarVisitor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@Slf4j
public final class TradingInputStreamBackwardsExtender<Model> implements TradingInputStream<Model> {

    private final Iterator<Model> dataIterator;

    /**
     * Extend searches to complete stream.
     *
     * @param symbol         Symbol to search.
     * @param period         Period to search.
     * @param barCountBefore Number of bars to retrieve before end time
     * @param endTime        Start time to search.
     * @param barVisitor     Visitor to apply
     * @param tradingSearch  Search engine to use.
     * @return a bar input stream
     * @throws IOException on an io failure.
     * @see #extend(String, Bar.Period, int, Instant, BarVisitor, TradingSearch)
     */
    public static TradingInputStream<Bar> extend(String symbol,
                                                 Bar.Period period,
                                                 int barCountBefore,
                                                 Instant endTime,
                                                 BarVisitor barVisitor,
                                                 TradingSearch tradingSearch) throws IOException {
        return new TradingInputStreamBackwardsExtender<>(barCountBefore, new Search<>() {
            private Instant start;
            private Instant end;

            @Override
            public void sort(List<Bar> data) {
                data.sort(Comparator.comparing(Bar::getStartMillisecondsUtc));
            }

            @Override
            public boolean prepare(int searchCount) {
                final Duration duration = period.getDuration().multipliedBy(barCountBefore);
                final Instant theBeginningOfTime = tradingSearch.getTheBeginningOfTime();
                start = endTime.minus(duration.multipliedBy(searchCount + 1));
                if (start.compareTo(theBeginningOfTime) < 0) {
                    log.warn("Reached the beginning of Time {}", theBeginningOfTime);
                    start = theBeginningOfTime;
                }
                end = endTime.minus(duration.multipliedBy(searchCount)).minusNanos(1);
                return start.equals(theBeginningOfTime);
            }

            @Override
            public TradingInputStream<Bar> perform() throws IOException {
                return tradingSearch.aggregateFromTicks(symbol,
                                                        period,
                                                        start,
                                                        end,
                                                        barVisitor);
            }
        });
    }

    private interface Search<Model> {
        boolean prepare(int searchCount);

        TradingInputStream<Model> perform() throws IOException;

        void sort(List<Model> data);
    }

    private TradingInputStreamBackwardsExtender(int maxCount, Search<Model> search) throws IOException {
        /*
        note that each search here adds to the end, so CD-AB for a backwards search with forwards order.
        We check that we don't fall off the end of the data map at the beginning of time.
        */
        final List<Model> data = new ArrayList<>(maxCount);
        boolean finalSearch = false;
        int searchCount = 0;
        while (data.size() < maxCount && !finalSearch) {
            finalSearch = search.prepare(searchCount++);
            try (TradingInputStream<Model> searchData = search.perform()) {
                searchData.stream().forEach(data::add);
            }
        }
        // sort and remove excess data
        search.sort(data);
        log.debug("Retrieved {} data items in backwards search.  Cleaning to {} items", data.size(), maxCount);
        final int numToRemove = Math.max(0, data.size() - maxCount);
        dataIterator = data.subList(numToRemove, data.size()).iterator();
    }

    @Override
    public Model next() throws NoSuchElementException {
        return dataIterator.next();
    }

    @Override
    public boolean hasNext() {
        return dataIterator.hasNext();
    }

    @Override
    public void close() throws IOException {
        //ignored
    }
}
