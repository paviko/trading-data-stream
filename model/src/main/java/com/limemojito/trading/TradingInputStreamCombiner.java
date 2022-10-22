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

package com.limemojito.trading;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class TradingInputStreamCombiner<Model> implements TradingInputStream<Model> {
    private final Iterator<TradingInputStream<Model>> inputStreams;
    private TradingInputStream<Model> currentStream;

    @Override
    @SneakyThrows
    public Model next() {
        scanForNextInStreams();
        if (currentStream == null) {
            throw new NoSuchElementException("No more objects");
        }
        return currentStream.next();
    }

    @Override
    @SneakyThrows
    public boolean hasNext() {
        scanForNextInStreams();
        return currentStream != null && currentStream.hasNext();
    }

    @Override
    public void close() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
    }

    private void scanForNextInStreams() throws IOException {
        while (currentStream == null && inputStreams.hasNext()) {
            currentStream = inputStreams.next();
            if (!currentStream.hasNext()) {
                currentStream.close();
                currentStream = null;
            }
        }
    }
}
