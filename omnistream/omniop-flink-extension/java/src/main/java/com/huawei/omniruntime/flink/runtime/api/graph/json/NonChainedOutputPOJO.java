package com.huawei.omniruntime.flink.runtime.api.graph.json;

import static org.apache.flink.util.Preconditions.checkState;

import com.huawei.omniruntime.flink.runtime.api.graph.json.common.IntermediateDataSetIDPOJO;
import com.huawei.omniruntime.flink.runtime.api.graph.json.common.ResultPartitionTypeConverter;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Objects;

/**
 * NonChainedOutputPOJO
 *
 * @version 1.0.0
 * @since 2025/04/24
 */

public class NonChainedOutputPOJO {
    /**
     * ID of the producer {@link StreamNode}.
     */
    private int sourceNodeId;

    /**
     * Parallelism of the consumer vertex.
     */
    private int consumerParallelism;

    /**
     * Max parallelism of the consumer vertex.
     */
    private int consumerMaxParallelism;

    /**
     * Buffer flush timeout of this output.
     */
    private long bufferTimeout;

    /**
     * ID of the produced intermediate dataset.
     */
    private IntermediateDataSetIDPOJO dataSetId;

    /**
     * Whether this intermediate dataset is a persistent dataset or not.
     */
    private boolean isPersistentDataSet;

    /** The side-output tag (if any). */
    // private final OutputTag<?> outputTag;

    /**
     * The corresponding data partitioner.
     */
    private StreamPartitionerPOJO partitioner;

    /**
     * Target {@link ResultPartitionType}.
     */
    private int partitionType;

    public NonChainedOutputPOJO() {
    }

    public NonChainedOutputPOJO(int sourceNodeId,
                                int consumerParallelism,
                                int consumerMaxParallelism,
                                long bufferTimeout,
                                IntermediateDataSetIDPOJO dataSetId,
                                boolean isPersistentDataSet,
                                StreamPartitionerPOJO partitioner,
                                int partitionType) {
        this.sourceNodeId = sourceNodeId;
        this.consumerParallelism = consumerParallelism;
        this.consumerMaxParallelism = consumerMaxParallelism;
        this.bufferTimeout = bufferTimeout;
        this.dataSetId = dataSetId;
        this.isPersistentDataSet = isPersistentDataSet;
        this.partitioner = partitioner;
        this.partitionType = partitionType;
    }

    public NonChainedOutputPOJO(NonChainedOutput nonChainedOutput) {
        this.sourceNodeId = nonChainedOutput.getSourceNodeId();
        this.consumerParallelism = nonChainedOutput.getConsumerParallelism();
        this.consumerMaxParallelism = nonChainedOutput.getConsumerMaxParallelism();
        this.bufferTimeout = nonChainedOutput.getBufferTimeout();
        this.dataSetId = new IntermediateDataSetIDPOJO(nonChainedOutput.getDataSetId());
        this.partitioner = new StreamPartitionerPOJO(nonChainedOutput.getPartitioner());
        this.partitionType = ResultPartitionTypeConverter.encode(nonChainedOutput.getPartitionType());
    }

    public int getSourceNodeId() {
        return sourceNodeId;
    }

    public void setSourceNodeId(int sourceNodeId) {
        this.sourceNodeId = sourceNodeId;
    }

    public int getConsumerParallelism() {
        return consumerParallelism;
    }

    public void setConsumerParallelism(int consumerParallelism) {
        this.consumerParallelism = consumerParallelism;
    }

    public int getConsumerMaxParallelism() {
        return consumerMaxParallelism;
    }

    public void setConsumerMaxParallelism(int consumerMaxParallelism) {
        this.consumerMaxParallelism = consumerMaxParallelism;
    }

    public long getBufferTimeout() {
        return bufferTimeout;
    }

    public void setBufferTimeout(long bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }

    public IntermediateDataSetIDPOJO getDataSetId() {
        return dataSetId;
    }

    public void setDataSetId(IntermediateDataSetIDPOJO dataSetId) {
        this.dataSetId = dataSetId;
    }

    public boolean getIsPersistentDataSet() {
        return isPersistentDataSet;
    }

    public void setIsPersistentDataSet(boolean isPersistentDataSet) {
        isPersistentDataSet = isPersistentDataSet;
    }

    public StreamPartitionerPOJO getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(StreamPartitionerPOJO partitioner) {
        this.partitioner = partitioner;
    }

    public int getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(int partitionType) {
        this.partitionType = partitionType;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        checkState(o instanceof NonChainedOutputPOJO);
        NonChainedOutputPOJO that = (NonChainedOutputPOJO) o;
        return sourceNodeId == that.sourceNodeId
                && consumerParallelism == that.consumerParallelism
                && consumerMaxParallelism == that.consumerMaxParallelism
                && bufferTimeout == that.bufferTimeout
                && isPersistentDataSet == that.isPersistentDataSet
                && partitionType == that.partitionType
                && Objects.equals(dataSetId, that.dataSetId)
                && Objects.equals(partitioner, that.partitioner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sourceNodeId,
                consumerParallelism,
                consumerMaxParallelism,
                bufferTimeout,
                dataSetId,
                isPersistentDataSet,
                partitioner,
                partitionType);
    }

    @Override
    public String toString() {
        return "NonChainedOutputPOJO{"
                + "sourceNodeId=" + sourceNodeId
                + ", consumerParallelism=" + consumerParallelism
                + ", consumerMaxParallelism=" + consumerMaxParallelism
                + ", bufferTimeout=" + bufferTimeout
                + ", dataSetId=" + dataSetId
                + ", isPersistentDataSet=" + isPersistentDataSet
                + ", partitioner=" + partitioner
                + ", partitionType=" + partitionType
                + '}';
    }
}
