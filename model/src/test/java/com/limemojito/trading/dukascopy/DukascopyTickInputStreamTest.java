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

package com.limemojito.trading.dukascopy;

import com.limemojito.trading.StreamData;
import com.limemojito.trading.Tick;
import com.limemojito.trading.TickDataLoader;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.limemojito.trading.StreamData.StreamSource.Historical;
import static com.limemojito.trading.StreamData.StreamType.Realtime;
import static org.assertj.core.api.Assertions.assertThat;

public class DukascopyTickInputStreamTest {

    private static final String EURUSD = "EURUSD";

    @Test
    public void shouldStreamData() throws Exception {
        final String path = "EURUSD/2018/06/05/05h_ticks.bi5";

        final List<Tick> tickList = TickDataLoader.loadTickData(path);

        final int expectedSize = 5594;
        assertThat(tickList).hasSize(expectedSize);
        assertTickEquals(tickList.get(0), 1530766801080L, 116573, 116568, 1.76F, 4.76F);
        assertTickEquals(tickList.get(expectedSize - 1), 1530770399812L, 116939, 116935, 1.25F, 1.0F);
    }

    @Test
    public void shouldStreamWithAnnoyingZeroBasedMonthPathAndZeroLengthFile() throws Exception {
        final String path = "USDJPY/2016/00/01/00h_ticks.bi5";

        final List<Tick> tickList = TickDataLoader.loadTickData(path);
        assertThat(tickList).isEmpty();
    }

    private void assertTickEquals(Tick tick, long time, int ask, int bid, float askVolume, float bidVolume) {
        assertThat(tick.getStreamId()).isEqualTo(StreamData.REALTIME_UUID);
        assertThat(tick.getStreamType()).isEqualTo(Realtime);
        assertThat(tick.getMillisecondsUtc()).isEqualTo(time);
        assertThat(tick.getSymbol()).isEqualTo(EURUSD);
        assertThat(tick.getAsk()).isEqualTo(ask);
        assertThat(tick.getBid()).isEqualTo(bid);
        assertThat(tick.getAskVolume()).isEqualTo(askVolume);
        assertThat(tick.getBidVolume()).isEqualTo(bidVolume);
        assertThat(tick.getSource()).isEqualTo(Historical);
    }
}
