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

package com.limemojito.trading.model.tick.dukascopy.criteria;

import com.limemojito.trading.model.bar.Bar;
import lombok.Value;

import java.time.Instant;

import static com.limemojito.trading.model.tick.dukascopy.criteria.Criteria.assertBeforeStart;

@Value
@SuppressWarnings("RedundantModifiersValueLombok")
public class BarCriteria implements Criteria {
    public BarCriteria(String symbol, Bar.Period period, Instant start, Instant end) {
        this.symbol = symbol;
        this.period = period;
        assertBeforeStart(start, end);
        this.start = Criteria.roundStart(period, start);
        this.end = Criteria.roundEndInstant(period, end);
    }

    private final String symbol;
    private final Bar.Period period;
    private final Instant start;
    private final Instant end;
}
