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

import com.limemojito.trading.Bar;
import com.limemojito.trading.Bar.Period;
import com.limemojito.trading.BarInputStreamToCsv;
import com.limemojito.trading.TradingInputStream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Configuration;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

@Slf4j
public class DukascopyTickToBarCsv implements AutoCloseable {
    private final BarInputStreamToCsv delegate;
    private int tickCount;
    private int barCount;

    public DukascopyTickToBarCsv(Validator validator, String path, InputStream inputStream, Period period, OutputStream outputStream) {
        final TradingInputStream<Bar> barInputStream = new DukascopyBarInputStream(validator,
                                                                                   period,
                                                                                   bar -> barCount++,
                                                                                   new DukascopyTickInputStream(validator,
                                                                                                                path,
                                                                                                                inputStream,
                                                                                                                tick -> tickCount++)
        );
        this.delegate = new BarInputStreamToCsv(barInputStream, outputStream);
    }

    @SuppressWarnings("MagicNumber")
    public static void main(String... args) throws IOException {
        final String path = args[0];
        final String inputPath = args[1];
        final String period = args[2];
        final String outputPath = args[3];
        final Validator validator = setupValidator();
        try (FileInputStream fileInputStream = new FileInputStream(inputPath); FileOutputStream fileOutputStream = new FileOutputStream(outputPath)) {
            try (DukascopyTickToBarCsv dukascopyTickToCsv = new DukascopyTickToBarCsv(validator,
                                                                                      path,
                                                                                      fileInputStream,
                                                                                      Period.valueOf(period),
                                                                                      fileOutputStream)) {
                DukascopyTickToBarCsv.Conversion results = dukascopyTickToCsv.convert();
                log.info("Converted {} ticks to {} bars", results.getTickCount(), results.getBarCount());
            }
        }
    }

    public static Validator setupValidator() {
        Configuration<?> config = Validation.byDefaultProvider().configure();
        ValidatorFactory factory = config.buildValidatorFactory();
        Validator validator = factory.getValidator();
        factory.close();
        return validator;
    }

    /**
     * Copies the path of the class resource to an appropriately configured temp path
     *
     * @param path Path of dukascopy resource assumed to be from root of classpath.
     * @return The location of the temp copy.
     * @throws IOException on an io failure.
     */
    public static File dukascopyClassResourceToTempFile(String path) throws IOException {
        return DukascopyTickToCsv.dukascopyClassResourceToTempFile(path);
    }

    public Conversion convert() throws IOException {
        delegate.convert();
        return new Conversion(tickCount, barCount);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Value
    @SuppressWarnings("VisibilityModifier")
    public static class Conversion {
        int tickCount;
        int barCount;
    }

}
