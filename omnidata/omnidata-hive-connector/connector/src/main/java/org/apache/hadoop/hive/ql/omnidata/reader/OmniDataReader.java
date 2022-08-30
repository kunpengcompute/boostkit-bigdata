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

package org.apache.hadoop.hive.ql.omnidata.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.decode.PageDeserializer;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.omnidata.physical.NdpVectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataReader for filter and agg optimization
 *
 * @since 2022-03-07
 */
public class OmniDataReader implements Callable<Queue<VectorizedRowBatch>> {

    private static final Logger LOG = LoggerFactory.getLogger(OmniDataReader.class);

    private NdpPredicateInfo ndpPredicateInfo;

    private NdpVectorizedRowBatchCtx ndpVectorizedRowBatchCtx;

    private TypeInfo[] rowColumnTypeInfos;

    private OmniDataAdapter omniDataAdapter;

    private Object[] partitionValues = null;

    public OmniDataReader(Configuration conf, FileSplit fileSplit, PageDeserializer deserializer,
                          NdpPredicateInfo ndpPredicateInfo, TypeInfo[] rowColumnTypeInfos) {
        this.ndpPredicateInfo = ndpPredicateInfo;
        this.rowColumnTypeInfos = rowColumnTypeInfos;
        this.omniDataAdapter = new OmniDataAdapter(conf, fileSplit, ndpPredicateInfo, deserializer);
        if (ndpPredicateInfo.getNdpVectorizedRowBatchCtx() != null) {
            this.ndpVectorizedRowBatchCtx = ndpPredicateInfo.getNdpVectorizedRowBatchCtx();
            int partitionColumnCount = ndpVectorizedRowBatchCtx.getPartitionColumnCount();
            if (partitionColumnCount > 0) {
                partitionValues = new Object[partitionColumnCount];
                // set partitionValues
                NdpVectorizedRowBatchCtx.getPartitionValues(ndpVectorizedRowBatchCtx, conf, fileSplit, partitionValues,
                        this.rowColumnTypeInfos);
            }
        }
    }

    @Override
    public Queue<VectorizedRowBatch> call() throws UnknownHostException {
        Queue<ColumnVector[]> pages = omniDataAdapter.getBatchFromOmniData();
        return ndpPredicateInfo.getIsPushDownAgg()
                ? createVectorizedRowBatchWithAgg(pages)
                : createVectorizedRowBatch(pages);
    }

    private Queue<VectorizedRowBatch> createVectorizedRowBatchWithAgg(Queue<ColumnVector[]> pages) {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        while (!pages.isEmpty()) {
            ColumnVector[] columnVectors = pages.poll();
            int channelCount = columnVectors.length;
            int positionCount = columnVectors[0].isNull.length;
            VectorizedRowBatch rowBatch = new VectorizedRowBatch(channelCount, positionCount);
            // agg: copy columnVectors to rowBatch.cols
            System.arraycopy(columnVectors, 0, rowBatch.cols, 0, channelCount);
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }

    private Queue<VectorizedRowBatch> createVectorizedRowBatch(Queue<ColumnVector[]> pages) {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        while (!pages.isEmpty()) {
            ColumnVector[] columnVectors = pages.poll();
            int channelCount = columnVectors.length;
            int positionCount = columnVectors[0].isNull.length;
            // creates a vectorized row batch and the column vectors
            VectorizedRowBatch rowBatch = ndpVectorizedRowBatchCtx.createVectorizedRowBatch(rowColumnTypeInfos);
            for (int i = 0; i < channelCount; i++) {
                int columnId = ndpPredicateInfo.getOutputColumns().get(i);
                rowBatch.cols[columnId] = columnVectors[i];
            }
            rowBatch.size = positionCount;
            if (partitionValues != null) {
                ndpVectorizedRowBatchCtx.addPartitionColsToBatch(rowBatch, partitionValues, rowColumnTypeInfos);
            }
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }

}