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

package com.limemojito.trading.model.example;

import com.limemojito.trading.model.TradingInputStream;
import com.limemojito.trading.model.TradingSearch;
import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.bar.Bar.Period;
import com.limemojito.trading.model.bar.BarInputStreamToCsv;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.time.Instant;

@Component
@RequiredArgsConstructor
@Slf4j
public class CachePrimerRunner implements ApplicationRunner {

    private final DukascopyCache cache;
    private final DukascopyCacheLoader cacheLoader;


    @Override
    public void run(ApplicationArguments args) throws Exception {
        String symbol = getRequiredValue(args, "symbol");
        Period period = Period.valueOf(getRequiredValue(args, "period"));
        Instant start = getRequiredValueInstant(args, "start");
        Instant end = getRequiredValueInstant(args, "end");
        log.info("Performing bar aggregation to CSV on {} {} {} -> {} => {}", symbol, period, start, end, outputFile);
        cacheLoader.load(symbol, period, start, end);
        cacheLoader.waitForCompletion();
        log.info(cache.cacheStats());
    }

    private Instant getRequiredValueInstant(ApplicationArguments args, String name) {
        return Instant.parse(getRequiredValue(args, name));
    }

    private static String getRequiredValue(ApplicationArguments args, String name) {
        if (!args.containsOption(name)) {
            throw new IllegalArgumentException("Missing command line argument --" + name + "=????");
        }
        return args.getOptionValues(name).get(0);
    }
}
