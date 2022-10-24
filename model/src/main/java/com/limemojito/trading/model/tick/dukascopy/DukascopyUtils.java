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

package com.limemojito.trading.model.tick.dukascopy;

import com.limemojito.trading.model.tick.TickInputStreamToCsv;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Configuration;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;

import static java.lang.String.format;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

@Slf4j
public class DukascopyUtils {
    public static Validator setupValidator() {
        Configuration<?> config = Validation.byDefaultProvider().configure();
        ValidatorFactory factory = config.buildValidatorFactory();
        Validator validator = factory.getValidator();
        factory.close();
        log.info("Configured Validator");
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
        final String tempPath = System.getProperty("java.io.tmpdir");
        final File outputPath = new File(tempPath, path);
        log.info("Copying {} to {}", "classpath:" + path, outputPath.getAbsolutePath());
        //noinspection ResultOfMethodCallIgnored
        outputPath.mkdirs();
        try (InputStream resourceAsStream = TickInputStreamToCsv.class.getResourceAsStream(path)) {
            if (resourceAsStream == null) {
                throw new IOException(format("Could not open classpath resource %s", path));
            }
            long size = Files.copy(resourceAsStream, outputPath.toPath(), REPLACE_EXISTING);
            log.debug("Copied {} bytes", size);
        }
        return outputPath;
    }
}
