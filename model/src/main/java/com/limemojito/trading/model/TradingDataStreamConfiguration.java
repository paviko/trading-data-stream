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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.limemojito.trading.model.bar.Bar;
import com.limemojito.trading.model.bar.TickBarNotifyingAggregator;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import com.limemojito.trading.model.tick.dukascopy.DukascopyPathGenerator;
import com.limemojito.trading.model.tick.dukascopy.DukascopySearch;
import com.limemojito.trading.model.tick.dukascopy.DukascopyUtils;
import com.limemojito.trading.model.tick.dukascopy.cache.DirectDukascopyNoCache;
import com.limemojito.trading.model.tick.dukascopy.cache.LocalDukascopyCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import javax.validation.Validator;

/**
 * An example Spring configuration with a local file cache.  You can replace the local cache chain with an S3->local->no-cache as well.
 *
 * @see com.limemojito.trading.model.tick.dukascopy.cache.S3DukascopyCache
 */
@Configuration
public class TradingDataStreamConfiguration {
    @Bean
    public DukascopyPathGenerator pathGenerator() {
        return new DukascopyPathGenerator();
    }

    @Bean
    public DukascopyCache localCacheChain(ObjectMapper mapper) {
        return new LocalDukascopyCache(mapper, new DirectDukascopyNoCache());
    }

    @Bean
    public TradingSearch tickSearch(DukascopyPathGenerator pathGenerator, DukascopyCache cache, Validator validator) {
        return new DukascopySearch(validator, cache, pathGenerator);
    }

    @Scope("prototype")
    @Bean
    public TickBarNotifyingAggregator tickBarAggregator(Validator validator,
                                                        TickBarNotifyingAggregator.BarNotifier notifier,
                                                        @Value("${tick-to-bar.aggregation.period}") Bar.Period aggregationPeriod) {
        return new TickBarNotifyingAggregator(validator, notifier, aggregationPeriod);
    }

    /**
     * A helper method for a quick standalone configuration (not in a spring container).  This should NOT be used in a spring container as it
     * generates a Validator, object mapper, etc and is primarily suited for testing.
     *
     * @see DukascopyUtils#standaloneSetup()
     */
    public static TradingSearch standaloneSearch() {
        return DukascopyUtils.standaloneSetup();
    }
}
