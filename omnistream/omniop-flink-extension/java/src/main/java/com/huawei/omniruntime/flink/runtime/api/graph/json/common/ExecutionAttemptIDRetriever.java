/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.api.graph.json.common;

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.AbstractID;

import java.lang.reflect.Field;

/**
 * ExecutionAttemptIDRetriever
 *
 * @version 1.0.0
 * @since 2025/02/20
 */
public class ExecutionAttemptIDRetriever {
    /**
     * getExecutionAttemptId
     *
     * @param executionAttemptID executionAttemptID
     * @return AbstractID
     */
    public static AbstractID getExecutionAttemptId(ExecutionAttemptID executionAttemptID) {
        if (executionAttemptID == null) {
            return null; // Or throw an exception, depending on your needs
        }

        try {
            // Get the private 'executionAttemptId' field
            Field executionAttemptIdField = ExecutionAttemptID.class.getDeclaredField("executionGraphId");

            // Make the field accessible since it's private
            executionAttemptIdField.setAccessible(true);
            checkState(executionAttemptIdField.get(executionAttemptID) instanceof AbstractID,
                    "executionAttemptIdField.get(executionAttemptID) is not AbstractID");
            // Get the value of the 'executionAttemptId' field, which is an AbstractID
            return (AbstractID) executionAttemptIdField.get(executionAttemptID);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            // Handle exceptions appropriately. Rethrowing is often a good strategy.
            throw new IllegalStateException("Error accessing executionAttemptId: " + e.getMessage(), e);
        }
    }
}
