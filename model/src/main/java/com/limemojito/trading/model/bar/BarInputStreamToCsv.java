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

import com.limemojito.trading.model.TradingInputStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

@Slf4j
public class BarInputStreamToCsv implements AutoCloseable {
    private final TradingInputStream<Bar> barInputStream;
    private final Writer outputWriter;

    public BarInputStreamToCsv(TradingInputStream<Bar> barInputStream, OutputStream outputStream) {
        this(barInputStream, new OutputStreamWriter(outputStream, UTF_8));
    }

    public BarInputStreamToCsv(TradingInputStream<Bar> barInputStream, Writer outputWriter) {
        this.barInputStream = barInputStream;
        this.outputWriter = outputWriter;
    }

    public void convert() throws IOException {
        log.info("Begin convert to CSV");
        int barCount = 0;
        try (CSVPrinter printer = new CSVPrinter(outputWriter, CSVFormat.EXCEL)) {
            printer.printRecord("Time", "Symbol", "Period", "Open", "High", "Low", "Close");
            printer.printComment("Generated " + ZonedDateTime.now());
            while (barInputStream.hasNext()) {
                Bar bar = barInputStream.next();
                printer.printRecord(formatToString(bar.getStartDateTimeUtc()),
                                    bar.getSymbol(),
                                    bar.getPeriod(),
                                    bar.getOpen(),
                                    bar.getHigh(),
                                    bar.getLow(),
                                    bar.getClose());
                barCount++;
            }
        }
        log.info("Converted {} bars", barCount);
    }

    @Override
    public void close() throws IOException {
        barInputStream.close();
        outputWriter.close();
    }

    public static String formatToString(LocalDateTime dateTimeUtc) {
        return ISO_LOCAL_DATE.format(dateTimeUtc) + " " + ISO_LOCAL_TIME.format(dateTimeUtc);
    }
}
