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

import com.google.common.collect.Streams;
import lombok.RequiredArgsConstructor;

import java.io.Closeable;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface TradingInputStream<Model> extends Closeable, Iterable<Model> {
    /**
     * @return null on end of stream.
     */
    Model next() throws NoSuchElementException;

    boolean hasNext();

    /**
     * Iterates over all the ticks in the stream
     */
    default Iterator<Model> iterator() {
        return new TradingInputStream.ModelIterator<>(this);
    }

    default Stream<Model> stream() {
        return Streams.stream(this);
    }

    @RequiredArgsConstructor
    class ModelIterator<Model> implements Iterator<Model> {
        private final TradingInputStream<Model> inputStream;

        @Override
        public boolean hasNext() {
            return inputStream.hasNext();
        }

        @Override
        public Model next() throws NoSuchElementException {
            return inputStream.next();
        }
    }

    static <Model> TradingInputStream<Model> combine(Collection<TradingInputStream<Model>> inputStreams) {
        return combine(inputStreams.iterator());
    }

    static <Model> TradingInputStream<Model> combine(Iterator<TradingInputStream<Model>> inputStreams) {
        return combine(inputStreams, a -> true);
    }

    static <Model> TradingInputStream<Model> combine(Collection<TradingInputStream<Model>> inputStreams,
                                                     Predicate<Model> filter) {
        return combine(inputStreams.iterator(), filter);
    }

    static <Model> TradingInputStream<Model> combine(Iterator<TradingInputStream<Model>> inputStreams,
                                                     Predicate<Model> filter) {
        return new TradingInputStreamCombiner<>(inputStreams, filter);
    }
}
