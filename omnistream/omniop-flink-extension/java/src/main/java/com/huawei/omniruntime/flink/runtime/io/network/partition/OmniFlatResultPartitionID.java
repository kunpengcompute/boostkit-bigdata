/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.omniruntime.flink.runtime.io.network.partition;

import static org.apache.flink.util.Preconditions.checkState;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.AbstractID;

import java.lang.reflect.Field;

/**
 * OmniFlatResultPartitionID
 *
 * @version 1.0.0
 * @since 2025/04/24
 */
public class OmniFlatResultPartitionID {
    // IntermediateResultPartitionID
    private final long intermediateDataSetIDUpper;
    private final long intermediateDataSetIDLower;
    private final int partitionNum;

    // ExecutionAttemptID
    private final long producerIdUppper;
    private final long producerIdLower;

    public OmniFlatResultPartitionID(long intermediateDataSetIDUpper, long intermediateDataSetIDLower, int partitionNum,
                                     long producerIdUppper, long producerIdLower) {
        this.intermediateDataSetIDUpper = intermediateDataSetIDUpper;
        this.intermediateDataSetIDLower = intermediateDataSetIDLower;
        this.partitionNum = partitionNum;
        this.producerIdUppper = producerIdUppper;
        this.producerIdLower = producerIdLower;
    }

    public OmniFlatResultPartitionID(ResultPartitionID resultPartitionID) {
        this.intermediateDataSetIDUpper = resultPartitionID.getPartitionId().getIntermediateDataSetID().getUpperPart();
        this.intermediateDataSetIDLower = resultPartitionID.getPartitionId().getIntermediateDataSetID().getLowerPart();
        this.partitionNum = resultPartitionID.getPartitionId().getPartitionNumber();
        ExecutionAttemptID producerId = resultPartitionID.getProducerId();

        try {
            Class<?> valueClass = producerId.getClass();
            Field executionAttemptId = valueClass.getDeclaredField("executionAttemptId");
            executionAttemptId.setAccessible(true);
            checkState(executionAttemptId.get(executionAttemptId) instanceof AbstractID);
            AbstractID abstractID = (AbstractID) executionAttemptId.get(executionAttemptId);
            this.producerIdUppper = abstractID.getUpperPart();
            this.producerIdLower = abstractID.getLowerPart();
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public long getIntermediateDataSetIDUpper() {
        return intermediateDataSetIDUpper;
    }

    public long getIntermediateDataSetIDLower() {
        return intermediateDataSetIDLower;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public long getProducerIdUppper() {
        return producerIdUppper;
    }

    public long getProducerIdLower() {
        return producerIdLower;
    }
}
