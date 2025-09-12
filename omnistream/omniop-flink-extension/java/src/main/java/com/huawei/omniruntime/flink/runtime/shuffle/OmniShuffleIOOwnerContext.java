package com.huawei.omniruntime.flink.runtime.shuffle;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

/**
 * OmniShuffleIOOwnerContext
 *
 * @since 2025-04-27
 */
public class OmniShuffleIOOwnerContext {
    private final String ownerName;
    private final ExecutionAttemptID executionAttemptID;

    public OmniShuffleIOOwnerContext(String ownerName, ExecutionAttemptID executionAttemptID) {
        this.ownerName = ownerName;
        this.executionAttemptID = executionAttemptID;
    }

    public String getOwnerName() {
        return ownerName;
    }
}
