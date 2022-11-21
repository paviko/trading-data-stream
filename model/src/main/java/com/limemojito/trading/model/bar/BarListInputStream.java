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

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Adapts an in memory list of bars to an trading input stream.
 */
public class BarListInputStream implements TradingInputStream<Bar> {
    private final Iterator<Bar> iterator;
    private final BarVisitor barVisitor;

    public BarListInputStream(List<Bar> barList, BarVisitor barVisitor) {
        this.iterator = barList.iterator();
        this.barVisitor = barVisitor;
    }

    @Override
    public Bar next() throws NoSuchElementException {
        Bar bar = iterator.next();
        barVisitor.visit(bar);
        return bar;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public void close() {

    }
}
