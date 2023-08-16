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
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataParquetRawReader for raw parquet
 *
 * @since 2023-01-04
 */
public class OmniDataParquetRawReader implements Callable<Queue<VectorizedRowBatch>> {
    private static final Logger LOG = LoggerFactory.getLogger(OmniDataParquetRawReader.class);

    private VectorizedParquetRecordReader reader;

    public OmniDataParquetRawReader(JobConf conf, FileSplit fileSplit) {
        this.reader = new VectorizedParquetRecordReader(fileSplit, conf, null, null, null);
    }

    @Override
    public Queue<VectorizedRowBatch> call() throws HiveException {
        Queue<VectorizedRowBatch> rowBatches = new LinkedList<>();
        // use reader to create a vectorized row batch and the column vectors
        VectorizedRowBatch rowBatch = reader.createValue();
        try {
            while (reader.next(null, rowBatch)) {
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
            LOG.error("OmniDataParquetRawReader: Failed to close the reader; ignoring", e);
        }
    }
}