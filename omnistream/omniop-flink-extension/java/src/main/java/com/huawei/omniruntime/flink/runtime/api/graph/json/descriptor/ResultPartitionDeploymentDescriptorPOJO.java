package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.AbstractIDPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ResultPartitionTypeConverter;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;

/**
 * ResultPartitionDeploymentDescriptorPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class ResultPartitionDeploymentDescriptorPOJO {
    private ShuffleDescriptorPOJO shuffleDescriptor;
    private int maxParallelism;
    private boolean notifyPartitionDataAvailable;

    private AbstractIDPOJO resultId;

    /**
     * The total number of partitions for the result.
     */
    private int totalNumberOfPartitions;

    /**
     * The ID of the partition.
     */
    private IntermediateResultPartitionIDPOJO partitionId;

    /**
     * The type of the partition.
     */
    private int partitionType;

    /**
     * The number of subpartitions.
     */
    private int numberOfSubpartitions;


    public ResultPartitionDeploymentDescriptorPOJO() {
    }

    public ResultPartitionDeploymentDescriptorPOJO(ResultPartitionDeploymentDescriptor rpd) {
        this.shuffleDescriptor = new ShuffleDescriptorPOJO(rpd.getShuffleDescriptor());
        this.maxParallelism = rpd.getMaxParallelism();
        this.notifyPartitionDataAvailable = true;
        if (rpd.getResultId() != null) {
            this.resultId = new AbstractIDPOJO(rpd.getResultId().getUpperPart(), rpd.getResultId().getLowerPart());
        } else {
            this.resultId = new AbstractIDPOJO(-1, -1);
        }

        this.totalNumberOfPartitions = rpd.getTotalNumberOfPartitions();
        this.partitionId = new IntermediateResultPartitionIDPOJO(rpd.getPartitionId());
        this.partitionType = ResultPartitionTypeConverter.encode(rpd.getPartitionType());
        this.numberOfSubpartitions = rpd.getNumberOfSubpartitions();
    }

    public ResultPartitionDeploymentDescriptorPOJO(ShuffleDescriptorPOJO shuffleDescriptor, int maxParallelism,
                                                   boolean notifyPartitionDataAvailable, AbstractIDPOJO resultId,
                                                   int totalNumberOfPartitions,
                                                   IntermediateResultPartitionIDPOJO partitionId,
                                                   int partitionType, int numberOfSubpartitions) {
        this.shuffleDescriptor = shuffleDescriptor;
        this.maxParallelism = maxParallelism;
        this.notifyPartitionDataAvailable = notifyPartitionDataAvailable;
        this.resultId = resultId;
        this.totalNumberOfPartitions = totalNumberOfPartitions;
        this.partitionId = partitionId;
        this.partitionType = partitionType;
        this.numberOfSubpartitions = numberOfSubpartitions;
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

    public void setNumberOfSubpartitions(int numberOfSubpartitions) {
        this.numberOfSubpartitions = numberOfSubpartitions;
    }

    public ShuffleDescriptorPOJO getShuffleDescriptor() {
        return shuffleDescriptor;
    }

    public void setShuffleDescriptor(ShuffleDescriptorPOJO shuffleDescriptor) {
        this.shuffleDescriptor = shuffleDescriptor;
    }

    public int getMaxParallelism() {
        return maxParallelism;
    }

    public void setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
    }

    public boolean isNotifyPartitionDataAvailable() {
        return notifyPartitionDataAvailable;
    }

    public void setNotifyPartitionDataAvailable(boolean notifyPartitionDataAvailable) {
        this.notifyPartitionDataAvailable = notifyPartitionDataAvailable;
    }

    @Override
    public String toString() {
        return "ResultPartitionDeploymentDescriptorPOJO{"
                + "shuffleDescriptor=" + shuffleDescriptor
                + ", maxParallelism=" + maxParallelism
                + ", notifyPartitionDataAvailable=" + notifyPartitionDataAvailable
                + ", resultId=" + resultId
                + ", totalNumberOfPartitions=" + totalNumberOfPartitions
                + ", partitionId=" + partitionId
                + ", partitionType=" + partitionType
                + ", numberOfSubpartitions=" + numberOfSubpartitions
                + '}';
    }
}
