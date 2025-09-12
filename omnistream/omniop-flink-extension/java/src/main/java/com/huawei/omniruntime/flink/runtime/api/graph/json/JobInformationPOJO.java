/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json;

import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;

import java.io.IOException;

/**
 * JobInformationPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class JobInformationPOJO {
    /**
     * Id of the job.
     */
    private JobIDPOJO jobId;

    /**
     * Job name.
     */
    private String jobName;

    private long autoWatermarkInterval;

    /**
     * default none args constructor
     */
    public JobInformationPOJO() {}

    /**
     * constructor
     *
     * @param jobInformation jobInformation
     * @param cl classloader
     */
    public JobInformationPOJO(JobInformation jobInformation, ClassLoader cl) {
        this.jobId = new JobIDPOJO(jobInformation.getJobId());
        this.jobName = jobInformation.getJobName();
        try {
            this.autoWatermarkInterval = jobInformation.getSerializedExecutionConfig()
                .deserializeValue(cl).getAutoWatermarkInterval();
        } catch (IOException | ClassNotFoundException e) {
            throw new StreamTaskException("Could not instantiate dExecutionConfig.", e);
        }
    }

    public JobIDPOJO getJobId() {
        return jobId;
    }

    public void setJobId(JobIDPOJO jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public long getAutoWatermarkInterval() {
        return autoWatermarkInterval;
    }

    public void setAutoWatermarkInterval(long autoWatermarkInterval) {
        this.autoWatermarkInterval = autoWatermarkInterval;
    }

    @Override
    public String toString() {
        return "JobInformationPOJO{"
                + "jobId=" + jobId
                + ", jobName='" + jobName + '\''
                + '}';
    }
}
