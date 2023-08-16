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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.tez.MapRecordSource;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.omnidata.config.OmniDataConf;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This class inherits MapRecordSource and processes the Hive Task-level pushdown process
 * Native process optimization
 *
 * @since 2023-01-04
 */
public class OmniDataRawMapRecordSource extends MapRecordSource {
    private static final Logger LOG = LoggerFactory.getLogger(OmniDataRawMapRecordSource.class);

    private static final int RAW_MAX_THREAD_NUMS = 8;

    private AbstractMapOperator mapOp;

    private JobConf jconf;

    private ExecutorService executorService;

    private CompletionService<Queue<VectorizedRowBatch>> rowBatchCompletionService;

    private VectorizedRowBatchCtx vCtx;

    private List<FileSplit> fileSplits = new ArrayList<>();

    private String dataFormat = "";

    /**
     * Save InputSplit to fileSplits
     * Create a thread pool (executorService) based on parameters.
     *
     * @param jconf Omnidat configuration file
     * @param mapOp Abstract Map operator. Common code of MapOperator and VectorMapOperator
     * @param mapWork MapWork represents all the information used to run a map task on the cluster
     * @param inputSplits split file
     * @param dataFormat data Information
     * @since 2023-01-04
     */
    public void initOmniData(JobConf jconf, AbstractMapOperator mapOp,
                             MapWork mapWork, List<FileSplit> inputSplits, String dataFormat) {
        this.jconf = jconf;
        this.mapOp = mapOp;
        fileSplits = inputSplits;
        this.dataFormat = dataFormat;
        vCtx = mapWork.getVectorizedRowBatchCtx();
        // Need to limit the maximum number of threads to avoid out of memory
        int threadNums = Math.min(fileSplits.size(),
                OmniDataConf.getOmniDataRawThreadNums(this.jconf, RAW_MAX_THREAD_NUMS));
        // Initialize the thread pool
        executorService = new ThreadPoolExecutor(threadNums, threadNums, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingDeque<>(),
                new ThreadFactoryBuilder().setNameFormat("omnidata-hive-raw-optimized-thread-%d").build());
        rowBatchCompletionService = new ExecutorCompletionService<>(executorService);
    }

    @Override
    public boolean pushRecord() throws HiveException {
        mapOp.setExecContext(null);
        LOG.info("OmniDataRawMapRecordSource: use optimization to push raw record, "
                + "inputSplit num:{}, data format:{}", fileSplits.size(), dataFormat);
        try {
            if (dataFormat.toLowerCase(Locale.ENGLISH).contains("parquet")) {
                for (FileSplit fs : fileSplits) {
                    rowBatchCompletionService.submit(
                            new OmniDataParquetRawReader(jconf, fs));
                }
            } else {
                for (FileSplit fs : fileSplits) {
                    rowBatchCompletionService.submit(
                            new OmniDataOrcRawReader(jconf, fs, vCtx));
                }
            }
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