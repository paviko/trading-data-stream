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

package com.limemojito.trading.model;

import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.bar.BarVisitor;
import com.limemojito.trading.model.tick.Tick;
import com.limemojito.trading.model.tick.TickVisitor;

import java.io.IOException;
import java.time.Instant;

/**
 * Interface to different search engines.
 * Note all bar streams are from oldest to youngest, ie (2010 -> 2018)
 */
public interface TradingSearch {

    /**
     * Sets the limit of searching.
     *
     * @param theBeginningOfTime past this point searches will end data.
     */
    void setTheBeginningOfTime(Instant theBeginningOfTime);

    /**
     * Gets the limit of searching.
     *
     * @return The point where searches past will stop
     */
    Instant getTheBeginningOfTime();

    /**
     * Retrieve a steam of ticks.
     *
     * @param symbol    Symbol to search on.
     * @param startTime Time to begin search at
     * @param endTime   Time to end search at (inclusive)
     * @return Tick data matching the search request.
     * @throws IOException              on a data failure.
     * @throws IllegalArgumentException if the start time is < the beginningOfTime.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Tick> search(String symbol,
                                            Instant startTime,
                                            Instant endTime) throws IOException {
        return search(symbol, startTime, endTime, TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of ticks.
     *
     * @param symbol      Symbol to search on.
     * @param startTime   Time to begin search at
     * @param endTime     Time to end search at (inclusive)
     * @param tickVisitor Visitor to apply as each tick is found.
     * @return Tick data matching the search request.
     * @throws IOException              on a data failure.
     * @throws IllegalArgumentException if the start time is < the beginningOfTime.
     * @see #getTheBeginningOfTime()
     */
    TradingInputStream<Tick> search(String symbol,
                                    Instant startTime,
                                    Instant endTime,
                                    TickVisitor tickVisitor) throws IOException;

    /**
     * Retrieve a steam of bars by aggregating ticks.
     *
     * @param symbol    Symbol to search on.
     * @param period    Period to aggregate ticks to.
     * @param startTime Time to begin search at
     * @param endTime   Time to end search at (inclusive)
     * @return Bar data matching the search request.
     * @throws IOException              on a data failure.
     * @throws IllegalArgumentException if the start time is < the beginningOfTime.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       Instant endTime) throws IOException {
        return aggregateFromTicks(symbol, period, startTime, endTime, BarVisitor.NO_VISITOR, TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks.
     *
     * @param symbol     Symbol to search on.
     * @param period     Period to aggregate ticks to.
     * @param startTime  Time to begin search at
     * @param endTime    Time to end search at (inclusive)
     * @param barVisitor Visitor to apply as each bar is formed.
     * @return Bar data matching the search request.
     * @throws IOException              on a data failure.
     * @throws IllegalArgumentException if the start time is < the beginningOfTime.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       Instant endTime,
                                                       BarVisitor barVisitor) throws IOException {
        return aggregateFromTicks(symbol, period, startTime, endTime, barVisitor, TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks.
     *
     * @param symbol      Symbol to search on.
     * @param period      Period to aggregate ticks to.
     * @param startTime   Time to begin search at
     * @param endTime     Time to end search at (inclusive)
     * @param barVisitor  Visitor to apply as each bar is formed.
     * @param tickVisitor Visitor to apply as each tick is found.
     * @return Bar data matching the search request.
     * @throws IOException              on a data failure.
     * @throws IllegalArgumentException if the start time is < the beginningOfTime.
     * @see #getTheBeginningOfTime()
     */
    TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                               Bar.Period period,
                                               Instant startTime,
                                               Instant endTime,
                                               BarVisitor barVisitor,
                                               TickVisitor tickVisitor) throws IOException;

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count backwards in time.
     *
     * @param symbol         Symbol to search on.
     * @param period         Period to aggregate ticks to.
     * @param barCountBefore Number of bars to find before the end time.
     * @param endTime        The time that the bar start instants must be before.
     * @return Bar data matching the search request or best effort before the end of time.
     * @throws IOException on a data failure.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       int barCountBefore,
                                                       Instant endTime) throws IOException {
        return aggregateFromTicks(symbol,
                                  period,
                                  barCountBefore,
                                  endTime,
                                  BarVisitor.NO_VISITOR,
                                  TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count.
     *
     * @param symbol         Symbol to search on.
     * @param period         Period to aggregate ticks to.
     * @param barCountBefore Number of bars to find before the end time.
     * @param endTime        The time that the bar start instants must be before.
     * @return Bar data matching the search request or best effort before the end of time.
     * @throws IOException on a data failure.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       int barCountBefore,
                                                       Instant endTime,
                                                       BarVisitor barVisitor) throws IOException {
        return aggregateFromTicks(symbol,
                                  period,
                                  barCountBefore,
                                  endTime,
                                  barVisitor,
                                  TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count.
     *
     * @param symbol         Symbol to search on.
     * @param period         Period to aggregate ticks to.
     * @param barCountBefore Number of bars to find before the end time.
     * @param endTime        The time that the bar start instants must be before.
     * @param barVisitor     Visitor to apply as each bar is formed.
     * @param tickVisitor    Visitor to apply as each tick is found.
     * @return Bar data matching the search request or best effort before the end of time.
     * @throws IOException on a data failure.
     * @see #getTheBeginningOfTime()
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       int barCountBefore,
                                                       Instant endTime,
                                                       BarVisitor barVisitor,
                                                       TickVisitor tickVisitor) throws IOException {
        return TradingInputStreamBackwardsExtender.extend(symbol,
                                                          period,
                                                          barCountBefore,
                                                          endTime,
                                                          barVisitor,
                                                          tickVisitor,
                                                          this);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count forwards in time.
     *
     * @param symbol        Symbol to search on.
     * @param period        Period to aggregate ticks to.
     * @param startTime     The time that the bar start instants must be after (inclusive).
     * @param barCountAfter Number of bars to find before the end time.
     * @return Bar data matching the search request or best effort before the current time.
     * @throws IOException on a data failure.
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       int barCountAfter) throws IOException {
        return aggregateFromTicks(symbol,
                                  period,
                                  startTime,
                                  barCountAfter,
                                  BarVisitor.NO_VISITOR,
                                  TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count forwards in time.
     *
     * @param symbol        Symbol to search on.
     * @param period        Period to aggregate ticks to.
     * @param startTime     The time that the bar start instants must be after (inclusive).
     * @param barCountAfter Number of bars to find before the end time.
     * @param barVisitor    Visitor to apply as each bar is formed.
     * @return Bar data matching the search request or best effort before the current time.
     * @throws IOException on a data failure.
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       int barCountAfter,
                                                       BarVisitor barVisitor) throws IOException {
        return aggregateFromTicks(symbol,
                                  period,
                                  startTime,
                                  barCountAfter,
                                  barVisitor,
                                  TickVisitor.NO_VISITOR);
    }

    /**
     * Retrieve a steam of bars by aggregating ticks, limited count forwards in time.
     *
     * @param symbol        Symbol to search on.
     * @param period        Period to aggregate ticks to.
     * @param startTime     The time that the bar start instants must be after (inclusive).
     * @param barCountAfter Number of bars to find before the end time.
     * @param barVisitor    Visitor to apply as each bar is formed.
     * @param tickVisitor   Visitor to apply as each tick is found.
     * @return Bar data matching the search request or best effort before the current time.
     * @throws IOException on a data failure.
     */
    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       int barCountAfter,
                                                       BarVisitor barVisitor,
                                                       TickVisitor tickVisitor) throws IOException {
        return TradingInputStreamForwardsExtender.extend(symbol,
                                                         period,
                                                         startTime,
                                                         barCountAfter,
                                                         barVisitor,
                                                         tickVisitor,
                                                         this);
    }
}
