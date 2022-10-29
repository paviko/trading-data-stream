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

package com.limemojito.trading.model.example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.limemojito.trading.model.TradingSearch;
import com.limemojito.trading.model.tick.dukascopy.DukascopyCache;
import com.limemojito.trading.model.tick.dukascopy.DukascopyPathGenerator;
import com.limemojito.trading.model.tick.dukascopy.DukascopySearch;
import com.limemojito.trading.model.tick.dukascopy.cache.DirectDukascopyNoCache;
import com.limemojito.trading.model.tick.dukascopy.cache.LocalDukascopyCache;
import com.limemojito.trading.model.tick.dukascopy.cache.S3DukascopyCache;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;

import javax.validation.Validator;

@SpringBootApplication
public class DataStreamCli {

    @Bean
    public DirectDukascopyNoCache direct() {
        return new DirectDukascopyNoCache();
    }

    @Profile("s3")
    @Bean
    public AmazonS3 s3() {
        return AmazonS3Client.builder().build();
    }

    /**
     * Configures a local <- s3 <- direct cache chain.
     *
     * @param s3         amazon S3 client only enabled on s3 profile (--spring.profiles.active=s3)
     * @param bucketName Name of s3 bucket to use as 2nd level cache. (--bucket-name=my-tick-bucket)
     * @param direct     Direct access bean.
     * @return a configured Local -&gt; S3 -&gt; Direct cache.
     */
    @Profile("s3")
    @Bean
    @Primary
    public DukascopyCache localS3Direct(AmazonS3 s3,
                                        @Value("${bucket-name}") String bucketName,
                                        DirectDukascopyNoCache direct) {
        return new LocalDukascopyCache(new S3DukascopyCache(s3, bucketName, direct));
    }

    /**
     * Only enabled when the profile is not s3 (including default).
     *
     * @param direct Direct access bean.
     * @return a configured Local -&gt; Direct cache.
     */
    @Profile("!s3")
    @Bean
    @Primary
    public DukascopyCache localDirect(DirectDukascopyNoCache direct) {
        return new LocalDukascopyCache(direct);
    }

    @Bean
    public TradingSearch tradingSearch(Validator validator, DukascopyCache cache) {
        return new DukascopySearch(validator, cache, new DukascopyPathGenerator());
    }

    public static void main(String[] args) {
        SpringApplication.run(DataStreamCli.class, args);
    }
}
