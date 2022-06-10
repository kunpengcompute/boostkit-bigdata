/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.spark.vectorized;

import java.io.Serializable;

/**
 * hold partitioning info needed by splitter
 */
public class PartitionInfo implements Serializable {
    private final String partitionName;
    private final int partitionNum;
    private final int numCols;
    private final String inputTypes;

    /**
     * Init PartitionInfo
     *
     * @param partitionName Partitioning name. "single" for SinglePartitioning, "rr" for
     *     RoundRobinPartitioning, "hash" for HashPartitioning, "range" for RangePartitioning
     * @param partitionNum partition number
     */
    public PartitionInfo(String partitionName, int partitionNum, int numCols, String inputTypes) {
        this.partitionName = partitionName;
        this.partitionNum = partitionNum;
        this.numCols = numCols;
        this.inputTypes = inputTypes;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public int getNumCols() {
        return numCols;
    }

    public String getInputTypes() {
        return inputTypes;
    }
}
