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

import com.limemojito.trading.model.ModelPrototype;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.util.Collections;
import java.util.List;

import static com.limemojito.trading.model.StreamData.REALTIME_UUID;
import static com.limemojito.trading.model.TickDataLoader.getValidator;
import static com.limemojito.trading.model.bar.Bar.Period.H1;
import static com.limemojito.trading.model.bar.Bar.Period.M5;
import static com.limemojito.trading.model.bar.BarTest.assertBar;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SmallToLargeBarAggregatorTest {

    private final Validator validator = getValidator();

    @Test
    public void shouldAggregateOneBarsOk() {
        final List<Bar> m5Bars = ModelPrototype.createBarListDescending(REALTIME_UUID, "EURUSD", M5, 1528174800000L, 1);

        List<Bar> aggregated1H = new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars);

        assertThat(aggregated1H).hasSize(1);
        assertBar(aggregated1H.get(0),
                  "2018-06-05T05:00:00Z",
                  "2018-06-05T05:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
    }

    @Test
    public void shouldFailOnBarsInWrongOrder() {
        final List<Bar> m5Bars = ModelPrototype.createBarListDescending(REALTIME_UUID, "EURUSD", M5, 1528174800000L, 3);
        Collections.reverse(m5Bars);
        assertThatThrownBy(() -> new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars)).isInstanceOf(
                                                                                                        IllegalStateException.class)
                                                                                                .hasMessage(
                                                                                                        "BarAggregator requires bars sorted in descending time");
    }

    @Test
    public void shouldFailOnBarsMixedSymbols() {
        final List<Bar> m5Bars = List.of(ModelPrototype.createBar(REALTIME_UUID, "EURUSD", M5, 1528174800000L),
                                         ModelPrototype.createBar(REALTIME_UUID, "AUDUSD", M5, 1528174800000L));
        assertThatThrownBy(() -> new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars)).isInstanceOf(
                                                                                                        IllegalStateException.class)
                                                                                                .hasMessage(
                                                                                                        "BarAggregator does not support bars with different symbols.  First was EURUSD");
    }

    @Test
    public void shouldAggregateZeroBarsOk() {
        List<Bar> aggregated1H = new SmallToLargeBarAggregator(validator).aggregate(H1, Collections.emptyList());

        assertThat(aggregated1H).hasSize(0);
    }

    @Test
    public void shouldAggregateTwelveBarsOk() {
        final List<Bar> m5Bars = ModelPrototype.createBarListDescending(REALTIME_UUID,
                                                                        "EURUSD",
                                                                        M5,
                                                                        1528174800000L,
                                                                        12);

        List<Bar> aggregated1H = new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars);

        assertThat(aggregated1H).hasSize(1);
        assertBar(aggregated1H.get(0),
                  "2018-06-05T05:00:00Z",
                  "2018-06-05T05:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
    }

    @Test
    public void shouldAggregateBarsAcrossPeriodBoundaries() {
        final List<Bar> m5Bars = ModelPrototype.createBarListDescending(REALTIME_UUID,
                                                                        "EURUSD",
                                                                        M5,
                                                                        1528174800000L,
                                                                        24);

        List<Bar> aggregated1H = new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars);

        assertThat(aggregated1H).hasSize(2);
        assertBar(aggregated1H.get(0),
                  "2018-06-05T06:00:00Z",
                  "2018-06-05T06:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
        assertBar(aggregated1H.get(1),
                  "2018-06-05T05:00:00Z",
                  "2018-06-05T05:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
    }

    @Test
    public void shouldAggregateToOddNumberOfPartialBars() {
        final List<Bar> m5Bars = ModelPrototype.createBarListDescending(REALTIME_UUID,
                                                                        "EURUSD",
                                                                        M5,
                                                                        1528174800000L,
                                                                        18);

        List<Bar> aggregated1H = new SmallToLargeBarAggregator(validator).aggregate(H1, m5Bars);

        assertThat(aggregated1H).hasSize(2);
        // oldest bar first, so we fill the past bar to capacity before the latest one.
        assertBar(aggregated1H.get(0),
                  "2018-06-05T06:00:00Z",
                  "2018-06-05T06:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
        assertBar(aggregated1H.get(1),
                  "2018-06-05T05:00:00Z",
                  "2018-06-05T05:59:59.999Z",
                  "EURUSD",
                  H1,
                  116568,
                  116939,
                  116500,
                  116935
        );
    }
}
