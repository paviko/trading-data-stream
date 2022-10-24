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

import static com.limemojito.trading.model.tick.dukascopy.DukascopyUtils.setupValidator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TickInputStreamToCsvTest {

    private final Validator validator = setupValidator();

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
    public void shouldProcessToCsvOk() throws Exception {
        final String path = "/EURUSD/2018/06/05/05h_ticks.bi5";
        File tempLocation = DukascopyUtils.dukascopyClassResourceToTempFile(path);
        File csvOutput = new File(System.getProperty("java.io.tmpdir"), "eurusd-2018-06-05-05h.csv");
        InputStream expectedCsvData = getClass().getResourceAsStream("/EURUSD/2018/06/05/05h_ticks.csv");
        assertThat(expectedCsvData).isNotNull();

        try (
                FileInputStream inputStream = new FileInputStream(tempLocation);
                FileWriter outputWriter = new FileWriter(csvOutput);
                TradingInputStream<Tick> ticks = new DukascopyTickInputStream(validator,
                                                                              path,
                                                                              inputStream);
                TickInputStreamToCsv csv = new TickInputStreamToCsv(ticks, outputWriter)
        ) {
            csv.convert();
        }

        assertThat(csvOutput).isFile();
        assertThat(csvOutput).hasSize(269623L);
        assertThat(csvOutput).hasContent(new String(IOUtils.toByteArray(expectedCsvData), UTF_8));
    }
}
