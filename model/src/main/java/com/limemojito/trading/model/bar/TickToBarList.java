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

package com.limemojito.trading.model.bar;

import com.limemojito.trading.model.TradingInputStream;
import com.limemojito.trading.model.bar.Bar.Period;
import com.limemojito.trading.model.tick.Tick;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Validator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TickToBarList implements AutoCloseable {
    private final TradingInputStream<Tick> dukascopyInputStream;
    private final Period period;
    private final BarVisitor visitor;
    private final Validator validator;

    public TickToBarList(Validator validator, Period period, TradingInputStream<Tick> tickInputStream) {
        this(validator, period, tickInputStream, BarVisitor.NO_VISITOR);
    }

    public TickToBarList(Validator validator,
                         Period period,
                         TradingInputStream<Tick> tickInputStream,
                         BarVisitor visitor) {
        this.period = period;
        this.visitor = visitor;
        this.validator = validator;
        this.dukascopyInputStream = tickInputStream;
    }

    public List<Bar> convert() {
        final List<Bar> barList = new ArrayList<>();
        final TickBarNotifyingAggregator aggregator = new TickBarNotifyingAggregator(validator,
                                                                                     bar -> newBar(barList, bar),
                                                                                     period);
        aggregator.loadStart();
        while (dukascopyInputStream.hasNext()) {
            final Tick tick = dukascopyInputStream.next();
            aggregator.add(tick);
        }
        aggregator.loadEnd();
        return barList;
    }

    @Override
    public void close() throws IOException {
        dukascopyInputStream.close();
    }

    private void newBar(List<Bar> barList, Bar bar) {
        barList.add(bar);
        visitor.visit(bar);
    }
}
