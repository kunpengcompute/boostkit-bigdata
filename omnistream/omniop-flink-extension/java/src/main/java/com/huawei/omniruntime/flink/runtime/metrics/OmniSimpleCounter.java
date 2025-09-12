/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics;

import org.apache.flink.metrics.SimpleCounter;

/**
 * OmniSimpleCounter is a specialized counter that extends the SimpleCounter class and implements
 * the MetricCloseable interface. It provides functionality to manage a native reference to a counter
 * and allows for closing the counter when it is no longer needed.
 * <p>
 * This class is used to track counts in a native environment, providing methods to get the current
 * count and close the counter.
 *
 * @since 2025-04-16
 */
public class OmniSimpleCounter extends SimpleCounter implements MetricCloseable {
    private long nativeRef = 0L;
    private volatile boolean isClosed = false;
    private volatile long originalCount = 0L;

    public OmniSimpleCounter(long nativeRef) {
        this.nativeRef = nativeRef;
    }

    /**
     * get count.
     *
     * @return count
     */
    public long getCount() {
        // jni call to get the count
        originalCount = getNativeCounter(nativeRef);
        return originalCount;
    }

    /**
     * close the counter.
     */
    public void close() {
        // jni call to close the counter
        isClosed = true;
    }

    /**
     * get native reference.
     *
     * @param nativeRef native reference
     * @return native counter
     */
    public native long getNativeCounter(long nativeRef);
}
