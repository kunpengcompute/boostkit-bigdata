package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.AbstractIDPOJO;

import java.util.Objects;

/**
 * PartitionDescriptorPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class PartitionDescriptorPOJO {
    /**
     * this.isPipelined = isPipelined;
     * this.hasBackPressure = hasBackPressure;
     * this.isBounded = isBounded;
     * this.isPersistent = isPersistent;
     * this.isReconnectable = isReconnectable;
     * <p>
     * BLOCKING(false, false, false, false, true),
     * BLOCKING_PERSISTENT(false, false, false, true, true),
     * PIPELINED(true, true, false, false, false),
     * PIPELINED_BOUNDED(true, true, true, false, false),
     * PIPELINED_APPROXIMATE(true, true, true, false, true);
     */
    private final int numberOfSubpartitions;
    private AbstractIDPOJO resultId;
    private int totalNumberOfPartitions;
    private IntermediateResultPartitionIDPOJO partitionId;
    private int partitionType;  // bit 0
    private int connectionIndex;

    public PartitionDescriptorPOJO() {
        this.numberOfSubpartitions = 0; // Initialize final variable in default constructor
    }

    public PartitionDescriptorPOJO(
            AbstractIDPOJO resultId,
            int totalNumberOfPartitions,
            IntermediateResultPartitionIDPOJO partitionId,
            int partitionType,
            int numberOfSubpartitions,
            int connectionIndex) {
        this.resultId = resultId;
        this.totalNumberOfPartitions = totalNumberOfPartitions;
        this.partitionId = partitionId;
        this.partitionType = partitionType;
        this.numberOfSubpartitions = numberOfSubpartitions;
        this.connectionIndex = connectionIndex;
    }

    public AbstractIDPOJO getResultId() {
        return resultId;
    }

    public void setResultId(AbstractIDPOJO resultId) {
        this.resultId = resultId;
    }

    public int getTotalNumberOfPartitions() {
        return totalNumberOfPartitions;
    }

    public void setTotalNumberOfPartitions(int totalNumberOfPartitions) {
        this.totalNumberOfPartitions = totalNumberOfPartitions;
    }

    public IntermediateResultPartitionIDPOJO getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(IntermediateResultPartitionIDPOJO partitionId) {
        this.partitionId = partitionId;
    }

    public int getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(int partitionType) {
        this.partitionType = partitionType;
    }

    public int getNumberOfSubpartitions() {
        return numberOfSubpartitions;
    }


    public int getConnectionIndex() {
        return connectionIndex;
    }

    public void setConnectionIndex(int connectionIndex) {
        this.connectionIndex = connectionIndex;
    }

    @Override
    public String toString() {
        return "PartitionDescriptorPOJO{"
                + "resultId=" + resultId
                + ", totalNumberOfPartitions=" + totalNumberOfPartitions
                + ", partitionId=" + partitionId
                + ", partitionType=" + partitionType
                + ", numberOfSubpartitions=" + numberOfSubpartitions
                + ", connectionIndex=" + connectionIndex
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof PartitionDescriptorPOJO);
        PartitionDescriptorPOJO that = (PartitionDescriptorPOJO) o;
        return totalNumberOfPartitions == that.totalNumberOfPartitions
                && partitionType == that.partitionType
                && numberOfSubpartitions == that.numberOfSubpartitions
                && connectionIndex == that.connectionIndex && Objects.equals(resultId, that.resultId)
                && Objects.equals(partitionId, that.partitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                resultId,
                totalNumberOfPartitions,
                partitionId,
                partitionType,
                numberOfSubpartitions,
                connectionIndex);
    }
}