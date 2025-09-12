/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a metric group for managing and registering metrics in the
 * OmniRuntime environment.
 * This class provides functionality to register metrics and retrieve
 * information about the metric group.
 *
 * @since 2025-04-16
 */
public class OmniTaskMetricGroup {
    private Map<String, OmniInternalOperatorIOMetricGroup> operators = new HashMap<>();

    private OmniTaskIOMetricGroup ioMetrics;

    /**
     * set the task metric group.
     *
     * @param ioMetrics the task metric group
     */
    public void setOmniTaskIOMetricGroup(OmniTaskIOMetricGroup ioMetrics) {
        this.ioMetrics = ioMetrics;
    }

    /**
     * get the task metric group.
     *
     * @return the task metric group
     */
    public OmniTaskIOMetricGroup getOmniTaskIOMetricGroup() {
        return ioMetrics;
    }

    /**
     * get the operator metric group.
     *
     * @param operatorName the operator name
     * @param operator the operator metric group
     */
    public void addOperator(String operatorName, OmniInternalOperatorIOMetricGroup operator) {
        operators.put(operatorName, operator);
    }

    /**
     * close the metric group.
     */
    public void close() {
        for (OmniInternalOperatorIOMetricGroup operator : operators.values()) {
            operator.close();
        }
        if (ioMetrics != null) {
            ioMetrics.close();
        }
    }
}
