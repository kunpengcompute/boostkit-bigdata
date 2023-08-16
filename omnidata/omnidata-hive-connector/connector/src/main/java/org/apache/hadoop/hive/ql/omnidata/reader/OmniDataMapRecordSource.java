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

import com.huawei.boostkit.omnidata.decode.type.DecodeType;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.tez.MapRecordSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.OmniDataUtils;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class inherits MapRecordSource and connect Hive to the OmniData Server
 *
 * @since 2023/1/4
 */
public class OmniDataMapRecordSource extends MapRecordSource {
    private static final Logger LOG = LoggerFactory.getLogger(OmniDataMapRecordSource.class);

    /**
     * Maximum number of concurrent connections in the omnidata thread pool
     */
    private static final int MAX_THREAD_NUMS = 16;

    private AbstractMapOperator mapOp;

    private JobConf jconf;

    private ExecutorService executorService;

    private CompletionService<Queue<VectorizedRowBatch>> rowBatchCompletionService;

    private VectorizedRowBatchCtx vCtx;

    private List<FileSplit> fileSplits = new ArrayList<>();

    private NdpPredicateInfo ndpPredicateInfo;

    /**
     * Save InputSplit to fileSplits
     * Create a thread pool (executorService) based on parameters.
     *
     * @param jconf            Omnidat configuration file
     * @param mapOp            Abstract Map operator. Common code of MapOperator and VectorMapOperator
     * @param mapWork          MapWork represents all the information used to run a map task on the cluster
     * @param ndpPredicateInfo OmniData pushdown info
     * @param inputSplits      split file
     */
    public void initOmniData(JobConf jconf, AbstractMapOperator mapOp, MapWork mapWork,
                             NdpPredicateInfo ndpPredicateInfo, List<FileSplit> inputSplits) {
        this.jconf = jconf;
        this.mapOp = mapOp;
        this.ndpPredicateInfo = ndpPredicateInfo;
        fileSplits = inputSplits;
        vCtx = mapWork.getVectorizedRowBatchCtx();
        int threadNums = OmniDataConf.getOmniDataOptimizedThreadNums(jconf, MAX_THREAD_NUMS);
        // Need to limit the maximum number of threads to avoid out of memory
        threadNums = Math.min(fileSplits.size(), threadNums);
        // Initialize the thread pool
        executorService = new ThreadPoolExecutor(threadNums, threadNums, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat("omnidata-hive-optimized-thread-%d").build());
        rowBatchCompletionService = new ExecutorCompletionService<>(executorService);
    }

    @Override
    public boolean pushRecord() throws HiveException {
        mapOp.setExecContext(null);
        LOG.info("OmniDataMapRecordSource: use optimization to push record, inputSplit num:{}", fileSplits.size());
        // create DecodeType
        DecodeType[] columnTypes = new DecodeType[ndpPredicateInfo.getDecodeTypes().size()];
        for (int index = 0; index < columnTypes.length; index++) {
            String codeType = ndpPredicateInfo.getDecodeTypes().get(index);
            // If the returned data type is Agg, use transOmniDataAggDecodeType() method.
            if (ndpPredicateInfo.getDecodeTypesWithAgg().get(index)) {
                columnTypes[index] = OmniDataUtils.transOmniDataAggDecodeType(codeType);
            } else {
                columnTypes[index] = OmniDataUtils.transOmniDataDecodeType(codeType);
            }
        }
        for (FileSplit fs : fileSplits) {
            rowBatchCompletionService.submit(
                    new OmniDataReader(jconf, fs, ndpPredicateInfo, vCtx, columnTypes));
        }
        try {
            for (int loop = 0; loop < fileSplits.size(); loop++) {
                Queue<VectorizedRowBatch> rowBatches = rowBatchCompletionService.take().get();
                while (!rowBatches.isEmpty()) {
                    mapOp.process(rowBatches.poll());
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new HiveException(e);
        }
        executorService.shutdown();
        return false;
    }
}