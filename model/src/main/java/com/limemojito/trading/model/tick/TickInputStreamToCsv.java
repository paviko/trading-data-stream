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

package com.limemojito.trading.model.tick;

import com.limemojito.trading.model.TradingInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.ZonedDateTime;

import static com.limemojito.trading.model.bar.BarInputStreamToCsv.formatToString;
import static java.nio.charset.StandardCharsets.UTF_8;

@Slf4j
public class TickInputStreamToCsv implements AutoCloseable {
    private final TradingInputStream<Tick> tickInputStream;
    private final Writer writer;

    public TickInputStreamToCsv(TradingInputStream<Tick> tickInputStream,
                                OutputStream outputStream) {
        this(tickInputStream, new OutputStreamWriter(outputStream, UTF_8));
    }

    public TickInputStreamToCsv(TradingInputStream<Tick> tickInputStream,
                                Writer writer) {
        this.tickInputStream = tickInputStream;
        this.writer = writer;
    }

    public void convert() throws IOException {
        log.info("Begin convert to CSV");
        try (CSVPrinter printer = new CSVPrinter(writer, CSVFormat.EXCEL)) {
            printer.printRecord("Epoch Time (UTC)", "Ask", "Ask Volume", "Bid", "Bid Volume");
            printer.printComment("Generated at " + ZonedDateTime.now());
            long count = 0L;
            while (tickInputStream.hasNext()) {
                Tick tick = tickInputStream.next();
                count++;
                printer.printRecord(formatToString(tick.getDateTimeUtc()),
                                    tick.getAsk(),
                                    tick.getAskVolume(),
                                    tick.getBid(),
                                    tick.getBidVolume());
            }
            log.info("Converted {} ticks", count);
        }
    }

    @Override
    public void close() throws IOException {
        tickInputStream.close();
        writer.close();
    }
}
