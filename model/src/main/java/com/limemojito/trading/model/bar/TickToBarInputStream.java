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
import com.limemojito.trading.model.tick.Tick;

import javax.validation.Validator;
import java.io.IOException;
import java.util.Iterator;

import static com.limemojito.trading.model.bar.BarVisitor.NO_VISITOR;

/**
 * Note that this class aggregates ticks in memory to generate the bars.
 *
 * @see TickToBarList
 */
public class TickToBarInputStream implements TradingInputStream<Bar> {
    private final TickToBarList delegate;
    private Iterator<Bar> converted;

    /**
     * @param validator       to validate objects
     * @param period          period to aggregate to
     * @param tickInputStream stream to aggregate
     */
    public TickToBarInputStream(Validator validator,
                                Bar.Period period,
                                TradingInputStream<Tick> tickInputStream) {
        this(validator, period, NO_VISITOR, tickInputStream);
    }

    /**
     * @param validator       to validate objects
     * @param period          period to aggregate to
     * @param barVisitor      visit to occur on each bar generated.
     * @param tickInputStream stream to aggregate
     */
    public TickToBarInputStream(Validator validator,
                                Bar.Period period,
                                BarVisitor barVisitor,
                                TradingInputStream<Tick> tickInputStream) {
        delegate = new TickToBarList(validator, period, tickInputStream, barVisitor);
    }


    @Override
    public Bar next() {
        lazyConvert();
        return converted.next();
    }

    @Override
    public boolean hasNext() {
        lazyConvert();
        return converted.hasNext();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        if (converted != null) {
            converted = null;
        }
    }

    private void lazyConvert() {
        if (converted == null) {
            converted = delegate.convert().iterator();
        }
    }
}
