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

package com.limemojito.trading.dukascopy.cache;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.limemojito.trading.dukascopy.DukascopyCache;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * s3, then another cache.
 * Marked as a Service for Spring usage.  Note that properties of support classes can be set as spring properties.
 */
@Service
@Slf4j
public class S3DukascopyCache extends FallbackDukascopyCache {

    private final AmazonS3 s3;
    private final String bucketName;

    /**
     * Fallback to direct cache  .
     * @param s3 S3 client
     * @param bucketName bucketName to store data in (Dukascopy path structure).
     */
    public S3DukascopyCache(AmazonS3 s3, String bucketName) {
        this(s3, bucketName, new NoCacheDirectDukascopy());
    }

    public S3DukascopyCache(AmazonS3 s3, String bucketName, DukascopyCache fallback) {
        super(fallback);
        this.s3 = s3;
        this.bucketName = bucketName;
    }

    @Override
    protected void saveToCache(String dukascopyPath, InputStream input) throws IOException {
        final byte[] bytes = IOUtils.toByteArray(input);
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(bytes.length);
        metadata.setContentType("application/octet-stream");
        metadata.setContentDisposition(dukascopyPath);
        try (ByteArrayInputStream s3Input = new ByteArrayInputStream(bytes)) {
            log.info("Saving to s3://{}/{}", bucketName, dukascopyPath);
            s3.putObject(bucketName, dukascopyPath, s3Input, metadata);
        }
    }

    @Override
    protected InputStream checkCache(String dukascopyPath) {
        if (s3.doesObjectExist(bucketName, dukascopyPath)) {
            log.info("Retrieving s3://{}/{}", bucketName, dukascopyPath);
            return s3.getObject(bucketName, dukascopyPath).getObjectContent();
        } else {
            return null;
        }
    }
}

