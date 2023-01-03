/*
 * Copyright 2011-2023 Lime Mojito Pty Ltd
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public interface StreamData<DataType extends StreamData<?>> extends Comparable<DataType> {
    UUID REALTIME_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

    static int compareTo(StreamData<?> me, StreamData<?> other) {
        return compare(me.getStreamId(), other.getStreamId());
    }

    static boolean isRealtime(UUID streamId) {
        return REALTIME_UUID.equals(streamId);
    }

    static boolean isBacktest(UUID streamId) {
        return !isRealtime(streamId);
    }

    static int compare(UUID streamId, UUID other) {
        return streamId.equals(other) ? 0 : type(streamId).compareTo(type(other));
    }

    static StreamType type(UUID uid) {
        return REALTIME_UUID.equals(uid) ? StreamType.Realtime : StreamType.Backtest;
    }

    UUID getStreamId();

    StreamSource getSource();

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    default String getModelVersion() {
        return ModelVersion.VERSION;
    }

    /**
     * A string representing the partition key to uses with these objects for streams, parallel processing, etc.
     *
     * @return A string key that safely partitions the stream into ordered objects with stream identity.
     */
    @JsonIgnore
    String getPartitionKey();

    @JsonIgnore
    default StreamType getStreamType() {
        return type(getStreamId());
    }

    boolean isInSameStream(DataType other);

    @SuppressWarnings("NullableProblems")
    int compareTo(DataType other);

    enum StreamType {
        Backtest, Realtime
    }

    enum StreamSource {
        Live, Historical;

        /**
         * If we are combining sources then Live is displaced by historical data.  We value Live more but Historical contaminates Live.
         *
         * @param left  Source on the left
         * @param right Source on the right
         * @return Outcome when combined.
         */
        public static StreamSource aggregate(StreamSource left, StreamSource right) {
            if ((left == Live && right == Historical)) {
                return Historical;
            }
            return left;
        }
    }
}
