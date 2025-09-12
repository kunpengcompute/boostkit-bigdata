/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import org.apache.flink.metrics.Metric;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a metric group for managing and registering metrics in the OmniRuntime environment.
 * This class provides functionality to register metrics and retrieve information about the metric group.
 *
 * @since 2025-04-16
 */
public class OmniMetricGroup {
    long omniTaskMetricRef = 0L;
    String metricGroupName = null;
    private Map<String, Metric> registeredMetrics = new HashMap<String, Metric>();

    public OmniMetricGroup(long omniTaskMetricRef, String metricGroupName) {
        this.omniTaskMetricRef = omniTaskMetricRef;
        this.metricGroupName = metricGroupName;
    }

    /**
     * Registers a metric with the specified name in this metric group.
     *
     * @param name   The name of the metric to register.
     * @param metric The metric to register.
     */
    public void registerMetric(String name, Metric metric) {
        registeredMetrics.put(name, metric);
    }

    /**
     * Retrieves the native reference of the Omni task metric.
     *
     * @return The native reference of the Omni task metric.
     */
    public long getOmniTaskMetricRef() {
        return omniTaskMetricRef;
    }

    /**
     * Retrieves the registered metrics in this metric group.
     *
     * @return A map of registered metrics with their names as keys.
     */
    public Map<String, Metric> getRegisteredMetrics() {
        return registeredMetrics;
    }

    /**
     * Retrieves the name of this metric group.
     *
     * @return The name of this metric group.
     */
    public String getMetricGroupName() {
        return metricGroupName;
    }
}
