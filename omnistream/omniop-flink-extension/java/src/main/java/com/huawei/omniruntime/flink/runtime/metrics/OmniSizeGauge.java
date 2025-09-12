/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics;

import org.apache.flink.metrics.Gauge;

/**
 * OmniSizeGauge is a specialized gauge that extends the Gauge interface and implements
 * the MetricCloseable interface. It provides functionality to manage a native reference to a size
 * and allows for closing the gauge when it is no longer needed.
 * <p>
 * This class is used to track sizes in a native environment, providing methods to get the current
 * size and close the gauge.
 *
 * @since 2025-04-16
 */

public class OmniSizeGauge implements Gauge<Integer>, MetricCloseable {
    private long nativeRef = 0L;
    private volatile boolean isClosed = false;
    private volatile int originalValue = 0;

    public OmniSizeGauge(long nativeRef) {
        this.nativeRef = nativeRef;
    }

    /**
     * get size.
     *
     * @return value
     */
    @Override
    public Integer getValue() {
        if (isClosed) {
            return originalValue;
        } else {
            originalValue = getNativeSize(nativeRef);
        }
        return originalValue;
    }

    /**
     * close the gauge.
     */
    public void close() {
        // jni call to close the counter
        isClosed = true;
    }

    /**
     * get native reference.
     *
     * @param nativeRef native reference
     * @return native size
     */
    public native int getNativeSize(long nativeRef);
}
