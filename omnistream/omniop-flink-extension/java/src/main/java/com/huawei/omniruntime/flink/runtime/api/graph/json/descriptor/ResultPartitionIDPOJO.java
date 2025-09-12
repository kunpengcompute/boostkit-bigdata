package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

/**
 * ResultPartitionIDPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class ResultPartitionIDPOJO {
    private IntermediateResultPartitionIDPOJO partitionId;
    private ExecutionAttemptIDPOJO producerId;

    public ResultPartitionIDPOJO() {

    }

    public ResultPartitionIDPOJO(ResultPartitionID resultPartitionID) {
        this.partitionId = new IntermediateResultPartitionIDPOJO(resultPartitionID.getPartitionId());
        this.producerId = new ExecutionAttemptIDPOJO(resultPartitionID.getProducerId());
    }

    public ResultPartitionIDPOJO(IntermediateResultPartitionIDPOJO partitionId, ExecutionAttemptIDPOJO producerId) {
        this.partitionId = partitionId;
        this.producerId = producerId;
    }

    public IntermediateResultPartitionIDPOJO getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(IntermediateResultPartitionIDPOJO partitionId) {
        this.partitionId = partitionId;
    }

    public ExecutionAttemptIDPOJO getProducerId() {
        return producerId;
    }

    public void setProducerId(ExecutionAttemptIDPOJO producerId) {
        this.producerId = producerId;
    }

    @Override
    public String toString() {
        return "ResultPartitionIDPOJO{"
                + "partitionId=" + partitionId
                + ", producerId=" + producerId
                + '}';
    }
}