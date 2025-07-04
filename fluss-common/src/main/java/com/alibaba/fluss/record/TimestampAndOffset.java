/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.record;

/** Timestamp and related offset in log records. */
public final class TimestampAndOffset {
    private final long timestamp;
    private final long offset;

    public TimestampAndOffset(long timestamp, long offset) {
        this.timestamp = timestamp;
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(timestamp);
        result = 31 * result + Long.hashCode(offset);
        return result;
    }

    @Override
    public String toString() {
        return String.format("TimestampOffset(offset = %d, timestamp = %d)", offset, timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimestampAndOffset that = (TimestampAndOffset) o;
        return timestamp == that.timestamp && offset == that.offset;
    }
}
