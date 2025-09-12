/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics;

import org.apache.flink.runtime.metrics.TimerGauge;

/**
 * OmniTimeGauge is a specialized timer gauge that extends the TimerGauge class and implements
 * the MetricCloseable interface. It provides functionality to manage a native reference to a timer
 * and allows for closing the timer when it is no longer needed.
 * <p>
 * This class is used to track time intervals in a native environment, providing methods to get the
 * current value, count, accumulated count, and maximum single measurement.
 *
 * @since 2025-04-16
 */
public class OmniTimeGauge extends TimerGauge implements MetricCloseable {
    long nativeRef = 0L;
    private boolean isClosed = false;
    private volatile long originalValue = 0L;
    private volatile long originalCount = 0L;
    private volatile long originalAccumulatedCount = 0L;
    private volatile long originalMaxSingleMeasurement = 0L;
    private volatile boolean originalIsMeasuring = false;


    public OmniTimeGauge(long nativeRef) {
        this.nativeRef = nativeRef;
    }


    /**
     * update the value.
     */
    @Override
    public synchronized void update() {
        // update by native call
        if (isClosed) {
            return;
        }
        updateNative(nativeRef);
    }


    /**
     * get value.
     *
     * @return value
     */
    @Override
    public synchronized Long getValue() {
        // get value by native call
        if (isClosed) {
            return originalValue;
        } else {
            originalValue = getNativeValue(nativeRef);
        }
        return originalValue;
    }

    /**
     * longest consecutive back pressured period.
     *
     * @return the longest marked period as measured by the given * TimerGauge. For example the
     */
    public synchronized long getMaxSingleMeasurement() {
        // get max single measurement by native call
        if (isClosed) {
            return originalMaxSingleMeasurement;
        } else {
            originalMaxSingleMeasurement = getNativeMaxSingleMeasurement(nativeRef);
        }
        return originalMaxSingleMeasurement;
    }

    /**
     * get accumulated count.
     *
     * @return the accumulated period by the given * TimerGauge.
     */
    public synchronized long getAccumulatedCount() {
        // get accumulated count by native call
        if (isClosed) {
            return originalAccumulatedCount;
        } else {
            originalAccumulatedCount = getNativeAccumulatedCount(nativeRef);
        }
        return originalAccumulatedCount;
    }

    /**
     * get count.
     *
     * @return count by calling JNI
     */
    public synchronized long getCount() {
        // get count by native call
        if (isClosed) {
            return originalCount;
        } else {
            originalCount = getNativeCount(nativeRef);
        }
        return originalCount;
    }

    /**
     * check if measuring.
     *
     * @return true if measuring by calling JNI
     */
    @Override
    public synchronized boolean isMeasuring() {
        // check if measuring by native call
        if (isClosed) {
            return originalIsMeasuring;
        } else {
            originalIsMeasuring = getNativeIsMeasuring(nativeRef);
        }
        return originalIsMeasuring;
    }

    /**
     * close the timer.
     */
    public void close() {
        isClosed = true;
    }


    /**
     * updateNative JNI
     *
     * @param nativeRef native reference
     */
    private native void updateNative(long nativeRef);

    /**
     * getNativeValue JNI
     *
     * @param nativeRef native reference
     * @return native value
     */
    private native long getNativeValue(long nativeRef);

    /**
     * getNativeCount JNI
     *
     * @param nativeRef native reference
     * @return native count
     */
    private native long getNativeCount(long nativeRef);

    /**
     * getNativeAccumulatedCount JNI
     *
     * @param nativeRef native reference
     * @return native accumulated count
     */
    private native long getNativeAccumulatedCount(long nativeRef);

    /**
     * getNativeIsMeasuring JNI
     *
     * @param nativeRef native reference
     * @return true if measuring
     */
    private native boolean getNativeIsMeasuring(long nativeRef);

    /**
     * getNativeMaxSingleMeasurement JNI
     *
     * @param nativeRef native reference
     * @return native max single measurement
     */
    private native long getNativeMaxSingleMeasurement(long nativeRef);
}

