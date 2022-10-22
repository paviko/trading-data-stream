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

import com.limemojito.trading.Tick;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import javax.validation.Validator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.limemojito.trading.dukascopy.DukascopyTickToBarCsv.setupValidator;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;

@Slf4j
public class DukascopyTickToCsv implements AutoCloseable {
    private final String path;
    private final DukascopyTickInputStream dukascopyInputStream;
    private final OutputStream outputStream;

    public DukascopyTickToCsv(Validator validator, String path, InputStream dukascopyInputStream, OutputStream outputStream) {
        this.path = path;
        this.dukascopyInputStream = new DukascopyTickInputStream(validator, path, dukascopyInputStream);
        this.outputStream = outputStream;
    }

    public static void main(String... args) throws IOException {
        final String path = args[0];
        final String inputPath = args[1];
        final String outputPath = args[2];
        final Validator validator = setupValidator();
        try (FileInputStream fileInputStream = new FileInputStream(inputPath); FileOutputStream fileOutputStream = new FileOutputStream(outputPath)) {
            try (DukascopyTickToCsv dukascopyTickToCsv = new DukascopyTickToCsv(validator, path, fileInputStream, fileOutputStream)) {
                DukascopyTickToCsv.Conversion results = dukascopyTickToCsv.convert();
                log.info("Converted {} ticks", results.getTickCount());
            }
        }
    }

    static String formatToString(LocalDateTime dateTimeUtc) {
        return ISO_LOCAL_DATE.format(dateTimeUtc) + " " + ISO_LOCAL_TIME.format(dateTimeUtc);
    }

    /**
     * Copies the path of the class resource to an appropriately configured temp path
     *
     * @param path Path of dukascopy resource assumed to be from root of classpath.
     * @return The location of the temp copy.
     * @throws IOException on an io error.
     */
    public static File dukascopyClassResourceToTempFile(String path) throws IOException {
        final String tempPath = System.getProperty("java.io.tmpdir");
        final File outputPath = new File(tempPath, path);
        log.info("Copying {} to {}", "classpath:" + path, outputPath.getAbsolutePath());
        //noinspection ResultOfMethodCallIgnored
        outputPath.mkdirs();
        try (InputStream resourceAsStream = DukascopyTickToCsv.class.getResourceAsStream(path)) {
            if (resourceAsStream == null) {
                throw new IOException(format("Could not open classpath resource %s", path));
            }
            long size = Files.copy(resourceAsStream, outputPath.toPath(), REPLACE_EXISTING);
            log.debug("Copied {} bytes", size);
        }
        return outputPath;
    }

    public Conversion convert() throws IOException {
        log.info("Converting {} to CSV", path);
        try (CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(outputStream, UTF_8), CSVFormat.EXCEL)) {
            printer.printRecord("Epoch Time (UTC)", "Ask", "Ask Volume", "Bid", "Bid Volume");
            printer.printComment("Generated at " + ZonedDateTime.now());
            int count = 0;
            while (dukascopyInputStream.hasNext()) {
                Tick tick = dukascopyInputStream.next();
                count++;
                printer.printRecord(formatToString(tick.getDateTimeUtc()), tick.getAsk(), tick.getAskVolume(), tick.getBid(), tick.getBidVolume());
            }
            return new Conversion(count);
        }
    }

    @Override
    public void close() throws IOException {
        dukascopyInputStream.close();
        outputStream.close();
    }

    @Value
    @SuppressWarnings("VisibilityModifier")
    public static class Conversion {
        int tickCount;
    }
}
