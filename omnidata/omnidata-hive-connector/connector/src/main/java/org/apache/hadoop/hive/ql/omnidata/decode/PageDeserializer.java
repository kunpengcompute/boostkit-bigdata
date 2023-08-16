/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.decode;

import com.huawei.boostkit.omnidata.decode.Deserializer;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import io.airlift.slice.SliceInput;
import io.hetu.core.transport.execution.buffer.SerializedPage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Hive page deserializer
 */
public class PageDeserializer implements Deserializer<Queue<VectorizedRowBatch>> {

    private final PageDecoding decoding;

    private final DecodeType[] columnTypes;

    private VectorizedRowBatchCtx vCtx;

    private Object[] partitionValues = null;

    private NdpPredicateInfo ndpPredicateInfo;

    /**
     * Initialize Hive PageDeserializer
     *
     * @param columnTypes      DecodeType[]
     * @param vCtx             VectorizedRowBatchCtx
     * @param conf             Hive conf
     * @param fileSplit        FileSplit
     * @param ndpPredicateInfo OmniData pushdown info
     * @throws HiveException hive exception
     */
    public PageDeserializer(DecodeType[] columnTypes, VectorizedRowBatchCtx vCtx, Configuration conf,
                            FileSplit fileSplit, NdpPredicateInfo ndpPredicateInfo) throws HiveException {
        this.columnTypes = columnTypes;
        decoding = new PageDecoding();
        this.vCtx = vCtx;
        this.ndpPredicateInfo = ndpPredicateInfo;
        try {
            int partitionColumnCount = this.vCtx.getPartitionColumnCount();
            if (partitionColumnCount > 0) {
                partitionValues = new Object[partitionColumnCount];
                // set partitionValues
                VectorizedRowBatchCtx.getPartitionValues(this.vCtx, conf, fileSplit, partitionValues);
            }
        } catch (IOException e) {
            throw new HiveException(e);
        }
    }

    @Override
    public Queue<VectorizedRowBatch> deserialize(SerializedPage page) {
        if (page.isCompressed() || page.isEncrypted()) {
            throw new UnsupportedOperationException(
                    "unsupported HiveDeserializer isMarkerPage or compressed or encrypted page ");
        }
        SliceInput input = page.getSlice().getInput();
        // Number of columns
        int numberOfBlocks = input.readInt();
        checkArgument(numberOfBlocks >= 0, "decode failed, numberOfBlocks < 0");
        List<ColumnVector[]> columnVectors = new ArrayList<>();

        for (int i = 0; i < numberOfBlocks; i++) {
            ColumnVector[] result = decoding.decode(Optional.of(columnTypes[i]), input);
            if (result == null) {
                return null;
            }
            columnVectors.add(result);
        }
        return ndpPredicateInfo.getIsPushDownAgg()
                ? createVectorizedRowBatchWithAgg(columnVectors, numberOfBlocks)
                : createVectorizedRowBatch(columnVectors, numberOfBlocks);
    }

    private Queue<VectorizedRowBatch> createVectorizedRowBatchWithAgg(List<ColumnVector[]> columnVectors,
                                                                      int channelCount) {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        for (int i = 0; i < columnVectors.get(0).length; i++) {
            ColumnVector[] result = new ColumnVector[channelCount];
            for (int j = 0; j < channelCount; j++) {
                result[j] = columnVectors.get(j)[i];
            }
            int positionCount = columnVectors.get(0)[i].isNull.length;
            VectorizedRowBatch rowBatch = new VectorizedRowBatch(channelCount, positionCount);
            // agg: copy columnVectors to rowBatch.cols
            System.arraycopy(result, 0, rowBatch.cols, 0, channelCount);
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }

    private Queue<VectorizedRowBatch> createVectorizedRowBatch(List<ColumnVector[]> columnVectors,
                                                               int channelCount) {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        for (int i = 0; i < columnVectors.get(0).length; i++) {
            int positionCount = columnVectors.get(0)[i].isNull.length;
            // creates a vectorized row batch and the column vectors
            VectorizedRowBatch rowBatch = vCtx.createVectorizedRowBatch();
            for (int j = 0; j < channelCount; j++) {
                int columnId = ndpPredicateInfo.getOutputColumns().get(j);
                rowBatch.cols[columnId] = columnVectors.get(j)[i];
            }
            rowBatch.size = positionCount;
            if (partitionValues != null) {
                vCtx.addPartitionColsToBatch(rowBatch, partitionValues);
            }
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }

}