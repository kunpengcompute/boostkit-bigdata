/*
 * Copyright (C) 2021-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.jni;

import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.*;

import org.apache.spark.sql.catalyst.util.RebaseDateTime;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ParquetColumnarBatchJniReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchJniReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader() {
        NativeLoader.getInstance();
    }

    public long initializeReaderJava(String path, int capacity,
             List<Integer> rowgroupIndices, List<Integer> columnIndices, String ugi) {
        JSONObject job = new JSONObject();
        job.put("filePath", path);
        job.put("capacity", capacity);
        job.put("rowGroupIndices", rowgroupIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("columnIndices", columnIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("ugi", ugi);
        parquetReader = initializeReader(job);
        return parquetReader;
    }

    public int next(Vec[] vecList) {
        int vectorCnt = vecList.length;
        int[] typeIds = new int[vectorCnt];
        long[] vecNativeIds = new long[vectorCnt];
        long rtn = recordReaderNext(parquetReader, typeIds, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        int nativeGetId = 0;
        for (int i = 0; i < vectorCnt; i++) {
            switch (DataType.DataTypeId.values()[typeIds[nativeGetId]]) {
                case OMNI_BOOLEAN: {
                    vecList[i] = new BooleanVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_SHORT: {
                    vecList[i] = new ShortVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DATE32: {
                    vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_INT: {
                    vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_LONG:
                case OMNI_DECIMAL64: {
                    vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DOUBLE: {
                    vecList[i] = new DoubleVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_VARCHAR: {
                    vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
                    break;
                }
                case OMNI_DECIMAL128: {
                    vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId]);
                    break;
                }
                default: {
                    throw new RuntimeException("UnSupport type for ColumnarFileScan:" +
                            DataType.DataTypeId.values()[typeIds[i]]);
                }
            }
            nativeGetId++;
        }
        return (int)rtn;
    }

    public void close() {
        recordReaderClose(parquetReader);
    }

    public native long initializeReader(JSONObject job);

    public native long recordReaderNext(long parquetReader, int[] typeId, long[] vecNativeId);

    public native void recordReaderClose(long parquetReader);

}