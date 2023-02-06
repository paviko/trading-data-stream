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
import com.limemojito.trading.model.stream.TradingInputForwardSearchStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Slf4j
public final class TradingInputStreamForwardsExtender {

    /**
     * Extend searches to complete stream.
     *
     * @param symbol        Symbol to search.
     * @param period        Period to search.
     * @param startTime     Start time to search.
     * @param barCountAfter Number of bars to retrieve after start time
     * @param barVisitor    Visitor to apply
     * @param tradingSearch Search engine to use.
     * @return a bar input stream
     * @throws IOException on an io failure.
     */
    public static TradingInputStream<Bar> extend(String symbol,
                                                 Bar.Period period,
                                                 Instant startTime,
                                                 int barCountAfter,
                                                 BarVisitor barVisitor,
                                                 TradingSearch tradingSearch) throws IOException {
        return new TradingInputForwardSearchStream<>(barCountAfter, (searchCount) -> {
            final Duration duration = period.getDuration().multipliedBy(barCountAfter);
            final Instant start = startTime.plus(duration.multipliedBy(searchCount));
            final Instant end = startTime.plus(duration.multipliedBy(searchCount + 1)).minusNanos(1);
            return tradingSearch.aggregateFromTicks(symbol,
                                                    period,
                                                    start,
                                                    end,
                                                    barVisitor);
        });
    }
}
