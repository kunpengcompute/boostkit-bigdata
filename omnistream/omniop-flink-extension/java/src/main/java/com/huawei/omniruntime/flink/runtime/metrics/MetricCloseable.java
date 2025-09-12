/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics;

/**
 * Interface for classes that can be closed to release resources.
 * This is typically used for metrics that need to clean up native resources.
 *
 * @since 2025-04-16
 */
public interface MetricCloseable {
    /**
     * Closes the metric and releases any associated resources.
     * This method should be called when the metric is no longer needed.
     */
    default void close() {
    }
}
