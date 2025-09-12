/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createOmniSimpleCounter;

import com.huawei.omniruntime.flink.runtime.metrics.MetricCloseable;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * OmniInternalOperatorIOMetricGroup is a specialized metric group for tracking input and output
 * metrics of an operator in the OmniRuntime environment. It extends the OmniMetricGroup class
 * and provides functionality to manage and register various IO-related metrics.
 * This class includes counters for the number of records and bytes in and out, as well as rate
 * meters for the number of records in and out.
 *
 * @since 2025-04-16
 */
public class OmniInternalOperatorIOMetricGroup extends OmniMetricGroup {
    private Counter numRecordsIn;
    private Counter numRecordsOut;

    private Meter numRecordsInRate;
    private Meter numRecordsOutRate;

    private Counter numBytesIn;
    private Counter numBytesOut;


    public OmniInternalOperatorIOMetricGroup(long omniTaskMetricRef, String operatorName) {
        super(omniTaskMetricRef, "OmniInternalOperatorIOMetricGroup_" + operatorName);

        this.numRecordsIn = createOmniSimpleCounter(omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_RECORDS_IN);
        registerMetric(MetricNames.IO_NUM_RECORDS_IN, numRecordsIn);

        this.numRecordsOut = createOmniSimpleCounter(omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_RECORDS_OUT);
        registerMetric(MetricNames.IO_NUM_RECORDS_OUT, numRecordsOut);

        this.numBytesIn = createOmniSimpleCounter(omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_BYTES_IN);
        registerMetric(MetricNames.IO_NUM_BYTES_IN, numBytesIn);

        this.numBytesOut = createOmniSimpleCounter(omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_BYTES_OUT);
        registerMetric(MetricNames.IO_NUM_BYTES_OUT, numBytesOut);

        this.numRecordsInRate = new MeterView(numRecordsIn);
        registerMetric(MetricNames.IO_NUM_RECORDS_IN_RATE, numRecordsInRate);

        this.numRecordsOutRate = new MeterView(numRecordsOut);
        registerMetric(MetricNames.IO_NUM_RECORDS_OUT_RATE, numRecordsOutRate);
    }

    public Counter getNumRecordsInCounter() {
        return numRecordsIn;
    }

    public Counter getNumRecordsOutCounter() {
        return numRecordsOut;
    }

    public Meter getNumRecordsInRateMeter() {
        return numRecordsInRate;
    }

    public Meter getNumRecordsOutRate() {
        return numRecordsOutRate;
    }

    public Counter getNumBytesInCounter() {
        return numBytesIn;
    }

    public Counter getNumBytesOutCounter() {
        return numBytesOut;
    }

    /**
     * close resources.
     */
    public void close() {
        if (numRecordsIn != null) {
            if (numRecordsIn instanceof MetricCloseable) {
                ((MetricCloseable) numRecordsIn).close();
            }
        }
        if (numRecordsOut != null) {
            if (numRecordsOut instanceof MetricCloseable) {
                ((MetricCloseable) numRecordsOut).close();
            }
        }
        if (numBytesIn != null) {
            if (numBytesIn instanceof MetricCloseable) {
                ((MetricCloseable) numBytesIn).close();
            }
        }
        if (numBytesOut != null) {
            if (numBytesOut instanceof MetricCloseable) {
                ((MetricCloseable) numBytesOut).close();
            }
        }
    }
}
