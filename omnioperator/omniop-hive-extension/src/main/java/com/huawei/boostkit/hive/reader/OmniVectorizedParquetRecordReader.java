/*
 * Copyright (C) 2024-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.IOPrepareCache;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.VectorTableScanDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Map;
import java.util.List;

public class OmniVectorizedParquetRecordReader extends OmniParquetRecordReader {

    private VecConverter[] partColumnConverters;
    private final Vec[] withPartCol;
    private final Object[] partitionValues;
    private final PrimitiveTypeInfo[] partColTypeInfos;

    OmniVectorizedParquetRecordReader(InputSplit oldSplit, JobConf oldJobConf) throws IOException {
        super(oldSplit, oldJobConf);
        VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(jobConf);
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

            Map<Path, PartitionDesc> pathToPartitionInfo = Utilities.getMapWork(conf).getPathToPartitionInfo();
            PartitionDesc partDesc = (PartitionDesc) HiveFileFormatUtils.getFromPathRecursively(pathToPartitionInfo,
                    split.getPath(), IOPrepareCache.get().getPartitionDescMap());
            VectorizedRowBatchCtx.getPartitionValues(rbCtx, partDesc, partitionValues);
        } else {
            partitionValues = null;
            withPartCol = null;
        }
    }

    public void processUnIntData() {
        List<Type> types = this.fileSchema.getFields();
        TableScanDesc desc = (TableScanDesc) this.tableScanOp.getConf();
        VectorTableScanDesc tableDesc = (VectorTableScanDesc) desc.getVectorDesc();
        TypeInfo[] info = tableDesc.getProjectedColumnTypeInfos();
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            if (!(type instanceof PrimitiveType) || ((PrimitiveType) type).getOriginalType() == null
                    || ((PrimitiveType) type).getOriginalType().name().contains("UINT")) {
                continue;
            }
            String decimalScale = "1";
            boolean isDecimal = false;
            if (info[i] instanceof DecimalTypeInfo) {
                int scale = ((DecimalTypeInfo) info[i]).getScale();
                for (int j = 0; j < scale; j++) {
                    decimalScale += "0";
                }
                isDecimal = true;
            }
            if (vecs[i] instanceof IntVec) {
                IntVec vec = (IntVec) vecs[i];
                vecs[i] = convertUnsignedInt(vec, isDecimal, decimalScale);
            } else if (vecs[i] instanceof LongVec) {
                LongVec vec = (LongVec) vecs[i];
                vecs[i] = convertUnsignedLong(vec, isDecimal, decimalScale);
            }
        }
    }

    public boolean next(NullWritable key, VecBatchWrapper value) throws IOException {
        int batchSize = BATCH;
        batchSize = recordReader.next(vecs, typeIds);
        if (batchSize == 0) {
            return false;
        }
        processUnIntData();
        if (partitionValues != null) {
            for (int i = 0; i < partitionValues.length; i++) {
                Object[] partValue = new Object[batchSize];
                Arrays.fill(partValue, partColumnConverters[i].calculateValue(partitionValues[i], partColTypeInfos[i]));
                Vec partVec = partColumnConverters[i].toOmniVec(partValue, batchSize, partColTypeInfos[i]);
                withPartCol[vecs.length + i] = partVec;
            }
            System.arraycopy(vecs, 0, withPartCol, 0, vecs.length);
            value.setVecBatch(new VecBatch(withPartCol, batchSize));
            return true;
        }
        value.setVecBatch(new VecBatch(vecs, batchSize));
        return true;
    }
}