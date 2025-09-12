package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.AbstractIDPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ExecutionAttemptIDRetriever;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.AbstractID;

import java.io.Serializable;

/**
 * ExecutionAttemptIDPOJO
 *
 * @version 1.0.0
 * @since 2025/02/20
 */
public class ExecutionAttemptIDPOJO implements Serializable {
    private static final long serialVersionUID = -2377710916486194977L;

    private AbstractIDPOJO executionAttemptId;

    public ExecutionAttemptIDPOJO() {
        // Default constructor
    }

    public ExecutionAttemptIDPOJO(ExecutionAttemptID executionAttemptID) {
        AbstractID abstractID = ExecutionAttemptIDRetriever.getExecutionAttemptId(executionAttemptID);
        this.executionAttemptId = new AbstractIDPOJO(abstractID);
    }


    public ExecutionAttemptIDPOJO(AbstractIDPOJO executionAttemptId) {
        this.executionAttemptId = executionAttemptId;
    }

    /**
     * getExecutionAttemptId
     *
     * @return {@link AbstractIDPOJO }
     */
    public AbstractIDPOJO getExecutionAttemptId() {
        return executionAttemptId;
    }

    /**
     * setExecutionAttemptId
     *
     * @param executionAttemptId execution attempt ID
     */
    public void setExecutionAttemptId(AbstractIDPOJO executionAttemptId) {
        this.executionAttemptId = executionAttemptId;
    }

    @Override
    public String toString() {
        return "ExecutionAttemptIDPOJO{"
                + "executionAttemptId=" + executionAttemptId
                + '}';
    }
}
