/*
 * Copyright 2011-2024 Lime Mojito Pty Ltd
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

package com.limemojito.trading.model;

import java.time.Instant;
import java.time.LocalDateTime;

import static java.time.ZoneOffset.UTC;

public class UtcTimeUtils {

    public static long toEpochMillis(LocalDateTime dateTime) {
        return toEpochMillis(dateTime.toInstant(UTC));
    }

    public static long toEpochMillis(Instant instant) {
        return instant.toEpochMilli();
    }

    public static LocalDateTime toLocalDateTimeUtc(long epochMillis) {
        return toInstant(epochMillis).atZone(UTC).toLocalDateTime();
    }

    public static Instant toInstant(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis);
    }
}
