/*
 * Copyright (C) 2023-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.hive.reader;

import static com.huawei.boostkit.hive.cache.VectorCache.BATCH;
import static com.huawei.boostkit.hive.converter.VecConverter.CONVERTER_MAP;

import com.huawei.boostkit.hive.converter.VecConverter;

import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;

import java.io.IOException;
import java.util.Arrays;

public class OmniVectorizedOrcRecordReader extends OmniOrcRecordReader {
    private VecConverter[] partColumnConverters;
    private Vec[] partVecs;
    private final Vec[] withPartCol;
    private final Object[] partitionValues;

    private final PrimitiveTypeInfo[] partColTypeInfos;

    public OmniVectorizedOrcRecordReader(Configuration conf, FileSplit split) throws IOException {
        super(conf, split);
        VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(conf);
        int partitionColumnCount = rbCtx.getPartitionColumnCount();
        partColTypeInfos = new PrimitiveTypeInfo[partitionColumnCount];
        if (partitionColumnCount > 0) {
            partColumnConverters = new VecConverter[partitionColumnCount];
            for (int i = 0; i < rbCtx.getPartitionColumnCount(); i++) {
                TypeInfo partColTypeInfo = rbCtx.getRowColumnTypeInfos()[rbCtx.getDataColumnCount() + i];
                if (partColTypeInfo instanceof PrimitiveTypeInfo) {
                    partColTypeInfos[i] = (PrimitiveTypeInfo) partColTypeInfo;
                    partColumnConverters[i] = CONVERTER_MAP
                            .get(((PrimitiveTypeInfo) partColTypeInfo).getPrimitiveCategory());
                }
            }
            partitionValues = new Object[partitionColumnCount];
            withPartCol = new Vec[vecs.length + partitionColumnCount];
            partVecs = new Vec[partitionColumnCount];
            VectorizedRowBatchCtx.getPartitionValues(rbCtx, conf, split, partitionValues);
            for (int i = 0; i < partitionValues.length; i++) {
                Object[] partValue = new Object[BATCH];
                Arrays.fill(partValue, partColumnConverters[i].calculateValue(partitionValues[i], partColTypeInfos[i]));
                partVecs[i] = partColumnConverters[i].toOmniVec(partValue, BATCH, partColTypeInfos[i]);
            }
        } else {
            partitionValues = null;
            withPartCol = null;
        }
    }

    @Override
    public boolean next(NullWritable key, VecBatchWrapper value) throws IOException {
        int batchSize = BATCH;
        if (tableScanOp != null && tableScanOp.getDone()) {
            return false;
        }
        if (included.size() == 0) {
            batchSize = (int) recordReader.getNumberOfRowsJava();
        } else {
            batchSize = recordReader.next(vecs, typeIds);
        }
        if (batchSize == 0) {
            return false;
        }
        if (partitionValues != null) {
            int partVecLen = partVecs.length;
            Vec[] sliceVecs = new Vec[partVecLen];
            for (int i = 0; i < partVecLen; i++) {
                sliceVecs[i] = partVecs[i].slice(0, batchSize);
            }
            System.arraycopy(vecs, 0, withPartCol, 0, vecs.length);
            System.arraycopy(sliceVecs, 0, withPartCol, vecs.length, partVecLen);
            value.setVecBatch(new VecBatch(withPartCol, batchSize));
            return true;
        }
        value.setVecBatch(new VecBatch(vecs, batchSize));
        return true;
    }
}
