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

import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class TradingInputStreamCombiner<Model> implements TradingInputStream<Model> {
    private final Iterator<TradingInputStream<Model>> inputStreamsIterator;
    private final Predicate<Model> filter;
    private TradingInputStream<Model> inputStream;
    private Model peek;

    public TradingInputStreamCombiner(Iterator<TradingInputStream<Model>> inputStreamsIterator) {
        this(inputStreamsIterator, a -> true);
    }

    public TradingInputStreamCombiner(Iterator<TradingInputStream<Model>> inputStreamsIterator,
                                      Predicate<Model> filter) {
        this.inputStreamsIterator = inputStreamsIterator;
        this.filter = filter;
    }

    @Override
    @SneakyThrows
    public Model next() {
        if (peek != null) {
            Model next = peek;
            peek = null;
            return next;
        }
        Model next = scanForNextInStreams();
        if (next == null) {
            throw new NoSuchElementException("No more objects");
        }
        return next;
    }

    @Override
    @SneakyThrows
    public boolean hasNext() {
        if (peek != null) {
            return true;
        }
        peek = scanForNextInStreams();
        return (peek != null);
    }

    @Override
    public void close() throws IOException {
        if (inputStream != null) {
            inputStream.close();
        }
    }

    private Model scanForNextInStreams() throws IOException {
        Model found = null;
        while (found == null) {
            inputStreamWithData();
            if (inputStream == null) {
                // at end of data
                break;
            }
            Model next = inputStream.next();
            if (filter.test(next)) {
                found = next;
            }
        }
        return found;
    }

    private void inputStreamWithData() throws IOException {
        if (inputStream == null || !inputStream.hasNext()) {
            do {
                if (inputStream != null) {
                    inputStream.close();
                    inputStream = null;
                }
                if (inputStreamsIterator.hasNext()) {
                    inputStream = inputStreamsIterator.next();
                }
            }
            while ((inputStream == null || !inputStream.hasNext()) && inputStreamsIterator.hasNext());
        }
    }

}
