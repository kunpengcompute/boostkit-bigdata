package com.huawei.omniruntime.flink.streaming.runtime.io;

public class TNELProcessState {
    private int resultStatus;
    private long inputNumber;

    public TNELProcessState(int resultStatus, long inputNumber) {
        this.resultStatus = resultStatus;
        this.inputNumber = inputNumber;
    }

    public int getResultStatus() {
        return resultStatus;
    }

    public long getInputNumber() {
        return inputNumber;
    }
}