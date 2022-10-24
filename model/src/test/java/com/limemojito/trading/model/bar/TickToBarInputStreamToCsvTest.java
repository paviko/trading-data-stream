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
import com.limemojito.trading.model.tick.dukascopy.DukascopyTickInputStream;
import com.limemojito.trading.model.tick.dukascopy.DukascopyUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.Test;

import javax.validation.Validator;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

import static com.limemojito.trading.model.bar.Bar.Period.H1;
import static com.limemojito.trading.model.bar.Bar.Period.M10;
import static com.limemojito.trading.model.bar.Bar.Period.M15;
import static com.limemojito.trading.model.bar.Bar.Period.M30;
import static com.limemojito.trading.model.bar.Bar.Period.M5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TickToBarInputStreamToCsvTest {

    private final Validator validator = DukascopyUtils.setupValidator();

    @Test
    public void shouldCopyResourceToTempLocation() throws Exception {
        final String path = "/EURUSD/2018/06/05/05h_ticks.bi5";
        File tempLocation = DukascopyUtils.dukascopyClassResourceToTempFile(path);
        assertThat(tempLocation).isFile();
        assertThat(tempLocation).hasSize(33117L);
        assertThat(tempLocation.getAbsolutePath()).isEqualTo(new File(System.getProperty("java.io.tmpdir"),
                                                                      path).getAbsolutePath());
    }

    @Test
    public void shouldFailIfClasspathResourceMissing() {
        final String path = "/EURBCD/2018/06/05/05h_ticks.bi5";
        assertThatThrownBy(() -> DukascopyUtils.dukascopyClassResourceToTempFile(path)).isInstanceOf(IOException.class)
                                                                                       .hasMessage(
                                                                                               "Could not open classpath resource /EURBCD/2018/06/05/05h_ticks.bi5");

    }

    @Test
    public void shouldProcessToM5BarCsvOk() throws Exception {
        performConversionOk(M5, "/EURUSD/2018/06/05/05h_M5_bars.csv");
    }

    @Test
    public void shouldProcessToM10BarCsvOk() throws Exception {
        performConversionOk(M10, "/EURUSD/2018/06/05/05h_M10_bars.csv");
    }

    @Test
    public void shouldProcessToM15BarCsvOk() throws Exception {
        performConversionOk(M15, "/EURUSD/2018/06/05/05h_M15_bars.csv");
    }


    @Test
    public void shouldProcessToM30BarCsvOk() throws Exception {
        performConversionOk(M30, "/EURUSD/2018/06/05/05h_M30_bars.csv");
    }

    @Test
    public void shouldProcessToH1BarCsvOk() throws Exception {
        performConversionOk(H1, "/EURUSD/2018/06/05/05h_H1_bars.csv");
    }

    private void performConversionOk(Bar.Period period, String expectedBarData) throws IOException {
        String tickPath = "/EURUSD/2018/06/05/05h_ticks.bi5";
        File tempLocation = DukascopyUtils.dukascopyClassResourceToTempFile(tickPath);
        File csvOutput = new File(System.getProperty("java.io.tmpdir"), "eurusd-2018-06-05-05h.bar.csv");
        InputStream expectedCsvData = getClass().getResourceAsStream(expectedBarData);
        assertThat(expectedCsvData).isNotNull();

        try (
                FileInputStream inputStream = new FileInputStream(tempLocation);
                FileWriter outputWriter = new FileWriter(csvOutput);
                DukascopyTickInputStream ticks = new DukascopyTickInputStream(validator,
                                                                              tickPath,
                                                                              inputStream);
                TradingInputStream<Bar> barInput = new TickToBarInputStream(validator, period, ticks);
                BarInputStreamToCsv barCsv = new BarInputStreamToCsv(barInput, outputWriter)
        ) {
            barCsv.convert();
        }
        assertThat(csvOutput).isFile();
        assertThat(csvOutput).hasContent(new String(IOUtils.toByteArray(expectedCsvData), UTF_8));
    }
}
