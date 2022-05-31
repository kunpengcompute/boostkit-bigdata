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

import com.huawei.boostkit.omnidata.model.datasource.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * OmniDataOrcRecordReader
 */
public class OmniDataOrcRecordReader implements RecordReader {
    static final Logger LOG = LoggerFactory.getLogger(OmniDataOrcRecordReader.class);

    private OmniDataAdapter dataAdapter;

    public OmniDataOrcRecordReader(Configuration conf, FileSplit fileSplit, DataSource dataSource,
                                   NdpPredicateInfo ndpPredicateInfo) {
        this.dataAdapter = new OmniDataAdapter(dataSource, conf, fileSplit, ndpPredicateInfo);
    }

    private boolean ensureBatch() {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public long getRowNumber() {
        return 0;
    }

    @Override
    public float getProgress() throws IOException {
        return 0;
    }

    @Override
    public boolean hasNext() {
        return ensureBatch();
    }

    @Override
    public void seekToRow(long row) {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public Object next(Object previous) throws IOException {
        throw new UnsupportedOperationException("unsupported");
    }

    @Override
    public boolean nextBatch(VectorizedRowBatch theirBatch) {
        boolean ret = false;
        try {
            ret = dataAdapter.nextBatchFromOmniData(theirBatch);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    @Override
    public void close() throws IOException {
    }

}