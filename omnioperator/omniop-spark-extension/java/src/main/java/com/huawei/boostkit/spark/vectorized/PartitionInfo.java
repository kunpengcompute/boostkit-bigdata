/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
