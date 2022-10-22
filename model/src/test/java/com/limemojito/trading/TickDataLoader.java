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

import com.limemojito.trading.dukascopy.DukascopyTickInputStream;
import com.limemojito.trading.dukascopy.DukascopyTickToBarCsv;

import javax.validation.Validator;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TickDataLoader {

    private static final Validator VALIDATOR = DukascopyTickToBarCsv.setupValidator();

    public static Validator getValidator() {
        return VALIDATOR;
    }

    public static List<Tick> loadTickData(String path) throws IOException {
        final List<Tick> tickList = new LinkedList<>();
        try (InputStream resourceAsStream = TickDataLoader.class.getResourceAsStream("/" + path)) {
            assertThat(resourceAsStream).isNotNull();
            try (TradingInputStream<Tick> streamer = new DukascopyTickInputStream(VALIDATOR, path, resourceAsStream)) {
                while (streamer.hasNext()) {
                    Tick tick = streamer.next();
                    tickList.add(tick);
                }
            }
        }
        return tickList;
    }
}
