/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.bdsolution.loadsmetric.util;

import java.math.BigInteger;

/**
 * Utility for sampling and computing usage.
 */
public class TimeTracker {
    public static final int UNAVAILABLE = -1;
    private final long minimumTimeInterval;

    // used time since system is on (ms)
    private BigInteger cumulativeTime = BigInteger.ZERO;

    // used time read last time (ms)
    private BigInteger lastCumulativeTime = BigInteger.ZERO;

    // Unix timestamp while reading the time (ms)
    private long sampleTime;
    private long lastSampleTime;
    private float usage;
    private String type;
    private BigInteger jiffyLengthInMillis;

    public TimeTracker(String type, long jiffyLengthInMillis) {
        this.type = type;
        this.jiffyLengthInMillis = BigInteger.valueOf(jiffyLengthInMillis);
        this.usage = UNAVAILABLE;
        this.sampleTime = UNAVAILABLE;
        this.lastSampleTime = UNAVAILABLE;
        minimumTimeInterval =  10 * jiffyLengthInMillis;
    }

    /**
     * Return percentage of time spent over the time since last update.
     * time spent is based on elapsed jiffies multiplied by amount of
     * time for 1 core. Thus, if you use 2 cores completely you would have spent
     * twice the actual time between updates and this will return 200%.
     *
     * @return Return percentage of usage since last update, {@link
     * TimeTracker#UNAVAILABLE} if there haven't been 2 updates more than
     * {@link TimeTracker#minimumTimeInterval} apart
     */
    public float getUsagePercent() {
        if (lastSampleTime == UNAVAILABLE ||
                lastSampleTime > sampleTime) {
            // lastSampleTime > sampleTime may happen when the system time is changed
            lastSampleTime = sampleTime;
            lastCumulativeTime = cumulativeTime;
            return usage;
        }
        // When lastSampleTime is sufficiently old, update usage.
        // Also take a sample of the current time and cumulative time for the
        // use of the next calculation.
        if (sampleTime > lastSampleTime + minimumTimeInterval) {
            usage =
                    ((cumulativeTime.subtract(lastCumulativeTime)).floatValue())
                            * 100F / ((float) (sampleTime - lastSampleTime));
            lastSampleTime = sampleTime;
            lastCumulativeTime = cumulativeTime;
        }
        return usage;
    }

    /**
     * Obtain the cumulative time since the system is on.
     * @return cumulative time in milliseconds
     */
    public long getCumulativeTime() {
        return cumulativeTime.longValue();
    }

    /**
     * Apply delta to accumulators.
     * @param newValue new sample value
     * @param newTime new sample time
     */
    public void updateElapsedJiffies(BigInteger newValue, long newTime) {
        cumulativeTime = newValue.compareTo(cumulativeTime) >= 0 ?
                newValue : cumulativeTime;
        sampleTime = newTime;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SampleTime " + this.sampleTime)
                .append(" CummulativeTime " + this.cumulativeTime)
                .append(" LastSampleTime " + this.lastSampleTime)
                .append(" LastCummulativeTime " + this.lastCumulativeTime)
                .append(this.type + "Usage " + this.usage)
                .append(" JiffyLengthMillisec " + this.jiffyLengthInMillis);
        return sb.toString();
    }
}