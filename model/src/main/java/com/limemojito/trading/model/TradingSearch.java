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

public interface TradingSearch {

    default TradingInputStream<Tick> search(String symbol,
                                            Instant startTime,
                                            Instant endTime) throws IOException {
        return search(symbol, startTime, endTime, TickVisitor.NO_VISITOR);
    }

    TradingInputStream<Tick> search(String symbol,
                                    Instant startTime,
                                    Instant endTime,
                                    TickVisitor tickVisitor) throws IOException;

    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       Instant endTime) throws IOException {
        return aggregateFromTicks(symbol, period, startTime, endTime, BarVisitor.NO_VISITOR, TickVisitor.NO_VISITOR);
    }

    default TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                                       Bar.Period period,
                                                       Instant startTime,
                                                       Instant endTime,
                                                       BarVisitor barVisitor) throws IOException {
        return aggregateFromTicks(symbol, period, startTime, endTime, barVisitor, TickVisitor.NO_VISITOR);
    }

    TradingInputStream<Bar> aggregateFromTicks(String symbol,
                                               Bar.Period period,
                                               Instant startTime,
                                               Instant endTime,
                                               BarVisitor barVisitor,
                                               TickVisitor tickVisitor) throws IOException;
}
