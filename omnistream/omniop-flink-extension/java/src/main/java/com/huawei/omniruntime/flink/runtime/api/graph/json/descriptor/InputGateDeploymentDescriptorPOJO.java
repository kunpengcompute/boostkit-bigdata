package com.huawei.omniruntime.flink.runtime.api.graph.json.descriptor;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.IntermediateDataSetIDPOJO;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Deployment descriptor for a single input gate instance.
 * <p>Each input gate consumes partitions of a single intermediate result. The consumed subpartition
 * index is the same for each consumed partition.
 *
 * @version 1.0.0
 * @since 2025/04/24
 * @see SingleInputGate
 */
public class InputGateDeploymentDescriptorPOJO {
    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    private IntermediateDataSetIDPOJO consumedResultId;

    /**
     * The type of the partition the input gate is going to consume.
     */
    private int consumedPartitionType;

    /**
     * The index of the consumed subpartition of each consumed partition. This index depends on the
     * {@link DistributionPattern} and the subtask indices of the producing and consuming task.
     */
    private int consumedSubpartitionIndex;

    /**
     * An input channel for each consumed subpartition.
     */
    private List<ShuffleDescriptorPOJO> inputChannels;

    public InputGateDeploymentDescriptorPOJO() {
    }

    public InputGateDeploymentDescriptorPOJO(
            IntermediateDataSetIDPOJO consumedResultId,
            int consumedPartitionType,
            int consumedSubpartitionIndex,
            List<ShuffleDescriptorPOJO> inputChannels) {
        this.consumedResultId = consumedResultId;
        this.consumedPartitionType = consumedPartitionType;
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;
        this.inputChannels = inputChannels;
    }

    public InputGateDeploymentDescriptorPOJO(InputGateDeploymentDescriptor inputGateDeploymentDescriptor) {
        this.consumedResultId = new IntermediateDataSetIDPOJO(inputGateDeploymentDescriptor.getConsumedResultId());
        this.consumedPartitionType = inputGateDeploymentDescriptor.getConsumedPartitionType().ordinal();
        this.consumedSubpartitionIndex = inputGateDeploymentDescriptor.getConsumedSubpartitionIndex();
        ShuffleDescriptor[] shuffleDescriptors = inputGateDeploymentDescriptor.getShuffleDescriptors();
        if (shuffleDescriptors != null) {
            this.inputChannels = Arrays.stream(shuffleDescriptors)
                    .map(ShuffleDescriptorPOJO::new)
                    .collect(Collectors.toList());
        }
    }

    public IntermediateDataSetIDPOJO getConsumedResultId() {
        return consumedResultId;
    }

    public void setConsumedResultId(IntermediateDataSetIDPOJO consumedResultId) {
        this.consumedResultId = consumedResultId;
    }

    /**
     * Returns the type of this input channel's consumed result partition.
     *
     * @return consumed result partition type
     */
    public int getConsumedPartitionType() {
        return consumedPartitionType;
    }

    public void setConsumedPartitionType(int consumedPartitionType) {
        this.consumedPartitionType = consumedPartitionType;
    }

    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    public void setConsumedSubpartitionIndex(int consumedSubpartitionIndex) {
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;
    }

    public List<ShuffleDescriptorPOJO> getInputChannels() {
        return inputChannels;
    }

    public void setInputChannels(List<ShuffleDescriptorPOJO> inputChannels) {
        this.inputChannels = inputChannels;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof InputGateDeploymentDescriptorPOJO, "o is not InputGateDeploymentDescriptorPOJO");
        InputGateDeploymentDescriptorPOJO that = (InputGateDeploymentDescriptorPOJO) o;
        return consumedPartitionType == that.consumedPartitionType
                && consumedSubpartitionIndex == that.consumedSubpartitionIndex
                && Objects.equals(consumedResultId, that.consumedResultId)
                && Objects.equals(inputChannels, that.inputChannels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumedResultId, consumedPartitionType, consumedSubpartitionIndex, inputChannels);
    }

    @Override
    public String toString() {
        return "InputGateDeploymentDescriptorPOJO{"
                + "consumedResultId=" + consumedResultId
                + ", consumedPartitionType=" + consumedPartitionType
                + ", consumedSubpartitionIndex=" + consumedSubpartitionIndex
                + ", inputChannels=" + inputChannels
                + '}';
    }
}