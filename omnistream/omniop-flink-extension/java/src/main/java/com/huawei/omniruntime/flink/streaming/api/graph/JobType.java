package com.huawei.omniruntime.flink.streaming.api.graph;

public enum JobType {
    NULL(0),
    SQL(1),
    STREAM(2),
    SQL_STREAM(3);
    private final int value;
    // 构造函数
    JobType(int value) {
        this.value = value;
    }


    public int getValue() {
        return value;
    }

    public static JobType fromValue(int value) {
        for (JobType jobType : JobType.values()) {
            if (jobType.value == value) {
                return jobType;
            }
        }
        throw new IllegalArgumentException("Invalid value: " + value);
    }

    public JobType getCombinationsJobType(JobType jobType) {
        return fromValue(this.getValue() | jobType.getValue());
    }
}
