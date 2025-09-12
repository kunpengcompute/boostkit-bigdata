/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.metrics.groups;

import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createOmniOmniDescriptiveStatisticsHistogram;
import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createOmniSimpleCounter;
import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createSizeGauge;
import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createOmniSumCounter;
import static com.huawei.omniruntime.flink.runtime.metrics.utils.OmniMetricHelper.createOmniTimeGauge;

import com.huawei.omniruntime.flink.runtime.metrics.MetricCloseable;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSumCounter;
import com.huawei.omniruntime.flink.runtime.metrics.OmniSizeGauge;

import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.TimerGauge;

/**
 * OmniTaskIOMetricGroup is a specialized metric group for tracking input and output
 * metrics of a task in the OmniRuntime environment. It extends the OmniMetricGroup class
 * and provides functionality to manage and register various IO-related metrics.
 * <p>
 * This class includes counters for the number of records and bytes in and out, as well as rate
 * meters for the number of records in and out.
 *
 * @since 2025-04-16
 */
public class OmniTaskIOMetricGroup extends OmniMetricGroup {
    private Counter numBytesIn;
    private Counter numBytesOut;
    private OmniSumCounter numRecordsIn;
    private OmniSumCounter numRecordsOut;
    private Counter numBuffersOut;
    private Counter numMailsProcessed;
    private Meter numBytesInRate;
    private Meter numBytesOutRate;
    private Meter numRecordsInRate;
    private Meter numRecordsOutRate;
    private Meter numBuffersOutRate;
    private TimerGauge idleTimePerSecond;
    private Gauge<Double> busyTimePerSecond;
    private Gauge<Long> backPressuredTimePerSecond;
    private TimerGauge softBackPressuredTimePerSecond;
    private TimerGauge hardBackPressuredTimePerSecond;
    private Gauge<Long> maxSoftBackPressuredTime;
    private Gauge<Long> maxHardBackPressuredTime;
    private Gauge<Long> accumulatedBackPressuredTime;
    private Gauge<Long> accumulatedIdleTime;
    private Gauge<Double> accumulatedBusyTime;
    private Meter mailboxThroughput;
    private Histogram mailboxLatency;
    private OmniSizeGauge mailboxSize;

    private volatile boolean busyTimeEnabled;

    private long taskStartTime;

    public OmniTaskIOMetricGroup(long omniTaskMetricRef) {
        super(omniTaskMetricRef, "OmniTaskIOMetricGroup");
        createOmniMetricsInstance();
    }

    /**
     * create omni metrics instance and register metrics.
     */
    private void createOmniMetricsInstance() {
        this.numBytesIn = createOmniSimpleCounter(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_BYTES_IN);
        registerMetric(MetricNames.IO_NUM_BYTES_IN, numBytesIn);

        this.numBytesOut = createOmniSimpleCounter(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_BYTES_OUT);
        registerMetric(MetricNames.IO_NUM_BYTES_OUT, numBytesOut);

        this.numBytesInRate = new MeterView(numBytesIn);
        registerMetric(MetricNames.IO_NUM_BYTES_IN_RATE, numBytesInRate);

        this.numBytesOutRate = new MeterView(numBytesOut);
        registerMetric(MetricNames.IO_NUM_BYTES_OUT_RATE, numBytesOutRate);

        this.numRecordsIn = createOmniSumCounter(this.metricGroupName, MetricNames.IO_NUM_RECORDS_IN);
        registerMetric(MetricNames.IO_NUM_RECORDS_IN, numRecordsIn);

        this.numRecordsOut = createOmniSumCounter(this.metricGroupName, MetricNames.IO_NUM_RECORDS_OUT);
        registerMetric(MetricNames.IO_NUM_RECORDS_OUT, numRecordsOut);

        this.numRecordsInRate = new MeterView(numRecordsIn);
        registerMetric(MetricNames.IO_NUM_RECORDS_IN_RATE, numRecordsInRate);

        this.numRecordsOutRate = new MeterView(numRecordsOut);
        registerMetric(MetricNames.IO_NUM_RECORDS_OUT_RATE, numRecordsOutRate);

        this.numBuffersOut = createOmniSimpleCounter(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.IO_NUM_BUFFERS_OUT);
        registerMetric(MetricNames.IO_NUM_BUFFERS_OUT, numBuffersOut);

        this.numBuffersOutRate = new MeterView(numBuffersOut);
        registerMetric(MetricNames.IO_NUM_BUFFERS_OUT_RATE, numBuffersOutRate);

        this.idleTimePerSecond = createOmniTimeGauge(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.TASK_IDLE_TIME);
        registerMetric(MetricNames.TASK_IDLE_TIME, idleTimePerSecond);

        this.softBackPressuredTimePerSecond = createOmniTimeGauge(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.TASK_SOFT_BACK_PRESSURED_TIME);
        registerMetric(MetricNames.TASK_SOFT_BACK_PRESSURED_TIME, softBackPressuredTimePerSecond);

        this.hardBackPressuredTimePerSecond = createOmniTimeGauge(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.TASK_HARD_BACK_PRESSURED_TIME);
        registerMetric(MetricNames.TASK_HARD_BACK_PRESSURED_TIME, hardBackPressuredTimePerSecond);

        this.backPressuredTimePerSecond = this::getBackPressuredTimeMsPerSecond;
        registerMetric(MetricNames.TASK_BACK_PRESSURED_TIME, backPressuredTimePerSecond);

        this.maxSoftBackPressuredTime = softBackPressuredTimePerSecond::getMaxSingleMeasurement;
        registerMetric(MetricNames.TASK_MAX_SOFT_BACK_PRESSURED_TIME, maxSoftBackPressuredTime);

        this.maxHardBackPressuredTime = hardBackPressuredTimePerSecond::getMaxSingleMeasurement;
        registerMetric(MetricNames.TASK_MAX_HARD_BACK_PRESSURED_TIME, maxHardBackPressuredTime);

        this.busyTimePerSecond = this::getBusyTimePerSecond;
        registerMetric(MetricNames.TASK_BUSY_TIME, busyTimePerSecond);

        this.accumulatedBusyTime = this::getAccumulatedBusyTime;
        registerMetric(MetricNames.ACC_TASK_BUSY_TIME, accumulatedBusyTime);

        creatBackPressureAndMailMetrics();
    }

    private void creatBackPressureAndMailMetrics() {
        this.accumulatedBackPressuredTime = this::getAccumulatedBackPressuredTimeMs;
        registerMetric(MetricNames.ACC_TASK_BACK_PRESSURED_TIME, accumulatedBackPressuredTime);

        this.accumulatedIdleTime = idleTimePerSecond::getAccumulatedCount;
        registerMetric(MetricNames.ACC_TASK_IDLE_TIME, accumulatedIdleTime);

        this.numMailsProcessed = createOmniSimpleCounter(this.omniTaskMetricRef, this.metricGroupName,
                MetricNames.MAILBOX_THROUGHPUT);
        this.mailboxThroughput = new MeterView(numMailsProcessed);
        registerMetric(MetricNames.MAILBOX_THROUGHPUT, mailboxThroughput);

        this.mailboxLatency = createOmniOmniDescriptiveStatisticsHistogram(this.omniTaskMetricRef, 60,
                this.metricGroupName, MetricNames.MAILBOX_LATENCY);
        registerMetric(MetricNames.MAILBOX_LATENCY, mailboxLatency);

        this.mailboxSize = createSizeGauge(this.omniTaskMetricRef, this.metricGroupName, MetricNames.MAILBOX_SIZE);
        registerMetric(MetricNames.MAILBOX_SIZE, mailboxSize);
    }

    public TimerGauge getSoftBackPressuredTimePerSecond() {
        return softBackPressuredTimePerSecond;
    }

    public TimerGauge getHardBackPressuredTimePerSecond() {
        return hardBackPressuredTimePerSecond;
    }

    /**
     * get back pressured time per second.
     *
     * @return back pressured time per second
     */
    public long getBackPressuredTimeMsPerSecond() {
        return getSoftBackPressuredTimePerSecond().getValue() + getHardBackPressuredTimePerSecond().getValue();
    }

    private double getBusyTimePerSecond() {
        double busyTime = idleTimePerSecond.getValue() + getBackPressuredTimeMsPerSecond();
        return busyTimeEnabled ? 1000.0 - Math.min(busyTime, 1000.0) : Double.NaN;
    }

    private double getAccumulatedBusyTime() {
        // future_do start time should come from c++
        if (busyTimeEnabled) {
            return Math.max(System.currentTimeMillis() - taskStartTime
                            - idleTimePerSecond.getAccumulatedCount() - getAccumulatedBackPressuredTimeMs(), 0);
        } else {
            return Double.NaN;
        }
    }

    /**
     * get accumulated back pressured time.
     *
     * @return accumulated back pressured time
     */
    public long getAccumulatedBackPressuredTimeMs() {
        return getSoftBackPressuredTimePerSecond().getAccumulatedCount()
                + getHardBackPressuredTimePerSecond().getAccumulatedCount();
    }

    /**
     * add numRecordsOut counter.
     *
     * @param numRecordsOutCounter numRecordsOut counter
     */
    public void reuseRecordsOutputCounter(Counter numRecordsOutCounter) {
        this.numRecordsOut.addCounter(numRecordsOutCounter);
    }

    /**
     * add numRecordsIn counter.
     *
     * @param numRecordsInCounter numRecordsIn counter
     */
    public void reuseRecordsInputCounter(Counter numRecordsInCounter) {
        this.numRecordsIn.addCounter(numRecordsInCounter);
    }

    /**
     * close resources.
     */
    public void close() {
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
        if (numBuffersOut != null) {
            if (numBuffersOut instanceof MetricCloseable) {
                ((MetricCloseable) numBuffersOut).close();
            }
        }
        if (idleTimePerSecond != null) {
            if (idleTimePerSecond instanceof MetricCloseable) {
                ((MetricCloseable) idleTimePerSecond).close();
            }
        }

        closeMailBoxMetrics();
    }

    private void closeMailBoxMetrics() {
        if (softBackPressuredTimePerSecond != null) {
            if (softBackPressuredTimePerSecond instanceof MetricCloseable) {
                ((MetricCloseable) softBackPressuredTimePerSecond).close();
            }
        }
        if (hardBackPressuredTimePerSecond != null) {
            if (hardBackPressuredTimePerSecond instanceof MetricCloseable) {
                ((MetricCloseable) hardBackPressuredTimePerSecond).close();
            }
        }
        if (numMailsProcessed != null) {
            if (numMailsProcessed instanceof MetricCloseable) {
                ((MetricCloseable) numMailsProcessed).close();
            }
        }
        if (mailboxLatency != null) {
            if (mailboxLatency instanceof MetricCloseable) {
                ((MetricCloseable) mailboxLatency).close();
            }
        }
        if (mailboxSize != null) {
            if (mailboxSize instanceof MetricCloseable) {
                ((MetricCloseable) mailboxSize).close();
            }
        }
    }
}
