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

import com.limemojito.trading.model.tick.Tick;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Validator;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class TickBarNotifyingAggregator {
    private final Map<String, BarTickStreamAggregator> streamSymbolToBars;
    private final BarNotifier barNotifier;
    private final Bar.Period aggPeriod;
    private final Validator validator;

    public interface BarNotifier {
        void notify(Bar bar);

        default void flush() {
        }
    }

    public TickBarNotifyingAggregator(Validator validator,
                                      BarNotifier barNotifier,
                                      Bar.Period aggregationPeriod) {
        this.streamSymbolToBars = new ConcurrentHashMap<>();
        this.barNotifier = barNotifier;
        this.aggPeriod = aggregationPeriod;
        this.validator = validator;
    }

    public void loadStart() {
        // pause any timing processes, etc for the bulk load.
    }

    public void add(Tick tick) {
        final BarTickStreamAggregator aggregator = fetchAndSendPrevious(tick, aggPeriod);
        log.trace("Adding tick to aggregator");
        aggregator.add(tick);
    }

    public void loadEnd() {
        final Iterator<Map.Entry<String, BarTickStreamAggregator>> iterator = streamSymbolToBars.entrySet().iterator();
        while (iterator.hasNext()) {
            log.trace("Writing last bar that was being aggregated.");
            final Map.Entry<String, BarTickStreamAggregator> entry = iterator.next();
            final Bar bar = entry.getValue().toBar();
            send(bar);
            iterator.remove();
        }
        barNotifier.flush();
    }

    private BarTickStreamAggregator fetchAndSendPrevious(Tick tick, Bar.Period period) {
        final String streamSymbol = tick.getPartitionKey();
        final long timeIndex = tick.getMillisecondsUtc() / aggPeriod.getDurationMilliseconds();
        log.trace("Fetching {} aggregator for {}:{}", aggPeriod, streamSymbol, timeIndex);
        BarTickStreamAggregator aggregator = streamSymbolToBars.computeIfAbsent(streamSymbol,
                                                                                key -> newAggregator(tick.getStreamId(),
                                                                                                     tick.getSymbol(),
                                                                                                     timeIndex,
                                                                                                     period));
        // check that this tick is past the current aggregator.
        if (tick.getMillisecondsUtc() > aggregator.getEndMillisecondsUtc()) {
            aggregator = createNewAggregatorFor(period,
                                                timeIndex,
                                                tick,
                                                aggregator);
        }
        return aggregator;
    }

    private BarTickStreamAggregator createNewAggregatorFor(Bar.Period period,
                                                           long timeIndex,
                                                           Tick tick,
                                                           BarTickStreamAggregator replacing) {
        // as we are assumed ordered processing, the previous bar is now DONE.
        final BarTickStreamAggregator currentAggregator = newAggregator(tick.getStreamId(),
                                                                        tick.getSymbol(),
                                                                        timeIndex,
                                                                        period);
        if (streamSymbolToBars.replace(tick.getPartitionKey(), replacing, currentAggregator)) {
            send(replacing.toBar());
            return currentAggregator;
        }
        // replace failed, so we're at the supplied replacing.
        return replacing;
    }

    private void send(Bar bar) {
        final String partitionKey = bar.getPartitionKey();
        log.trace("Writing bar {}", partitionKey);
        barNotifier.notify(bar);
    }

    private BarTickStreamAggregator newAggregator(UUID streamId, String symbol, long timeIndex, Bar.Period period) {
        final long startMillisecondsUtc = timeIndex * period.getDurationMilliseconds();
        final BarTickStreamAggregator aggregator = new BarTickStreamAggregator(validator,
                                                                               streamId,
                                                                               symbol,
                                                                               startMillisecondsUtc,
                                                                               period);
        log.debug("Created new aggregator {}: {} {} @ {}", streamId, symbol, period, aggregator.getStartDateInstant());
        return aggregator;
    }
}
