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
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.omnidata.operator.predicate.NdpPredicateInfo;
import org.apache.hadoop.mapred.FileSplit;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

/**
 * OmniDataReader for agg optimization
 *
 * @since 2022-03-07
 */
public class OmniDataReader implements Callable<List<VectorizedRowBatch>> {

    private DataSource dataSource;

    private Configuration conf;

    private FileSplit fileSplit;

    private NdpPredicateInfo ndpPredicateInfo;

    public OmniDataReader(DataSource dataSource, Configuration conf, FileSplit fileSplit,
                          NdpPredicateInfo ndpPredicateInfo) {
        this.dataSource = dataSource;
        this.conf = conf;
        this.fileSplit = fileSplit;
        this.ndpPredicateInfo = ndpPredicateInfo;
    }

    @Override
    public List<VectorizedRowBatch> call() throws UnknownHostException {
        OmniDataAdapter omniDataAdapter = new OmniDataAdapter(dataSource, conf, fileSplit, ndpPredicateInfo);
        Queue<ColumnVector[]> pages = omniDataAdapter.getBatchFromOmniData();
        return getVectorizedRowBatch(pages);
    }

    private List<VectorizedRowBatch> getVectorizedRowBatch(Queue<ColumnVector[]> pages) {
        List<VectorizedRowBatch> rowBatches = new ArrayList<>();
        if (!pages.isEmpty()) {
            ColumnVector[] columnVectors = pages.poll();
            int channelCount = columnVectors.length;
            int positionCount = columnVectors[0].isNull.length;
            VectorizedRowBatch rowBatch = new VectorizedRowBatch(channelCount, positionCount);
            System.arraycopy(columnVectors, 0, rowBatch.cols, 0, channelCount);
            rowBatches.add(rowBatch);
        }
        return rowBatches;
    }
}