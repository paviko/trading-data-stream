/*
 * Copyright 2011-2024 Lime Mojito Pty Ltd
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

package com.limemojito.trading.model.stream;

import com.limemojito.trading.model.TradingInputStream;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@Slf4j
public final class TradingInputBackwardsSearchStream<Model> implements TradingInputStream<Model> {

    private final Iterator<Model> dataIterator;

    public interface Search<Model> {
        boolean prepare(int searchCount);

        TradingInputStream<Model> perform() throws IOException;

        void sort(List<Model> data);
    }

    public TradingInputBackwardsSearchStream(int maxCount, Search<Model> search) throws IOException {
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
