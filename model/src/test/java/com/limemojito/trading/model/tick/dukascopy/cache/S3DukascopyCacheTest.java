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

package com.limemojito.trading.model.tick.dukascopy.cache;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import com.limemojito.trading.model.tick.dukascopy.DukascopyUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class S3DukascopyCacheTest {
    private final String dukascopyTickPath = "EURUSD/2018/06/05/05h_ticks.bi5";

    private final String bucketName = "bucketName";
    @Mock
    private AmazonS3 s3;
    @Mock
    private DukascopyCache fallbackMock;
    private S3DukascopyCache cache;
    @Captor
    private ArgumentCaptor<PutObjectRequest> putRequestCaptor;

    @BeforeEach
    void setUp() {
        cache = new S3DukascopyCache(s3, bucketName, fallbackMock);
    }

    @AfterEach
    void verifyMocks() {
        verifyNoMoreInteractions(s3, fallbackMock);
    }

    @Test
    public void shouldPullFromS3Ok() throws IOException {
        doReturn(true).when(s3).doesObjectExist(bucketName, dukascopyTickPath);
        doReturn(validObject()).when(s3).getObject(bucketName, dukascopyTickPath);

        try (InputStream stream = cache.stream(dukascopyTickPath)) {
            assertStreamResult(stream, 1, 0);
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void shouldFallbackWhenMissingFromS3() throws Exception {
        doReturn(false).when(s3).doesObjectExist(bucketName, dukascopyTickPath);
        doReturn(validInputStream()).when(fallbackMock).stream(dukascopyTickPath);
        doReturn(new PutObjectResult()).when(s3).putObject(putRequestCaptor.capture());

        try (InputStream stream = cache.stream(dukascopyTickPath)) {
            assertStreamResult(stream, 0, 1);
        }
        verify(s3).putObject(putRequestCaptor.getValue());
        assertPutRequest(putRequestCaptor.getValue());
    }

    private void assertPutRequest(PutObjectRequest request) {
        assertThat(request.getBucketName()).isEqualTo(bucketName);
        assertThat(request.getKey()).isEqualTo(dukascopyTickPath);
        assertThat(request.getMetadata().getContentLength()).isEqualTo(33117L);
        assertThat(request.getMetadata().getContentType()).isEqualTo("application/octet-stream");
        assertThat(request.getMetadata().getContentDisposition()).isEqualTo(dukascopyTickPath);
    }

    private void assertStreamResult(InputStream stream, int hits, int misses) {
        assertThat(stream).isNotNull();
        assertThat(cache.getCacheHitCount()).isEqualTo(hits);
        assertThat(cache.getCacheMissCount()).isEqualTo(misses);
        assertThat(cache.getRetrieveCount()).isEqualTo(hits + misses);
    }

    private S3Object validObject() throws IOException {
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(validInputStream());
        return s3Object;
    }

    private InputStream validInputStream() throws IOException {
        return new FileInputStream(DukascopyUtils.dukascopyClassResourceToTempFile("/" + dukascopyTickPath));
    }
}
