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

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataOrcRawReader for raw orc
 *
 * @since 2022-12-12
 */
public class OmniDataOrcRawReader implements Callable<Queue<VectorizedRowBatch>> {
    private static final Logger LOG = LoggerFactory.getLogger(OmniDataOrcRawReader.class);

    private RecordReader<NullWritable, VectorizedRowBatch> reader;

    private VectorizedRowBatchCtx vCtx;

    private Object[] partitionValues = null;

    /**
     *  Get VectorizedOrcRecordReader for current file split , and set partition values.
     *
     * @param conf Configuration of current MapReduce job
     * @param fileSplit the file split to be processed
     * @param vCtx current RowBatchCtx
     * @throws HiveException IO Exception
     */
    public OmniDataOrcRawReader(JobConf conf, FileSplit fileSplit, VectorizedRowBatchCtx vCtx) throws HiveException {
        this.vCtx = vCtx;
        try {
            this.reader = new VectorizedOrcInputFormat().getRecordReader(fileSplit, conf, Reporter.NULL);
            int partitionColumnCount = vCtx.getPartitionColumnCount();
            // If partition column exists, set the partition value
            if (partitionColumnCount > 0) {
                partitionValues = new Object[partitionColumnCount];
                VectorizedRowBatchCtx.getPartitionValues(vCtx, conf, fileSplit, partitionValues);
            }
        } catch (IOException e) {
            closeReader();
            throw new HiveException(e);
        }
    }

    @Override
    public Queue<VectorizedRowBatch> call() throws HiveException {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        // creates a vectorized row batch and the column vectors
        VectorizedRowBatch rowBatch = reader.createValue();
        try {
            while (reader.next(null, rowBatch)) {
                if (partitionValues != null) {
                    vCtx.addPartitionColsToBatch(rowBatch, partitionValues);
                }
                rowBatches.add(rowBatch);
                rowBatch = reader.createValue();
            }
        } catch (IOException e) {
            closeReader();
            throw new HiveException(e);
        }
        return rowBatches;
    }

    private void closeReader() {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("OmniDataOrcRawReader: Failed to close the reader; ignoring", e);
        }
    }
}