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

import com.huawei.boostkit.scan.jni.ParquetColumnarBatchJniReader;

import nova.hetu.omniruntime.type.BooleanDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.ShortDataType;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;
import java.net.URI;

public class ParquetColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchScanReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader jniReader;

    public ParquetColumnarBatchScanReader() {
        jniReader = new ParquetColumnarBatchJniReader();
    }

    public long initializeReaderJava(Path path, int capacity,
                                     List<Integer> rowgroupIndices, List<Integer> columnIndices, String ugi) {
        JSONObject job = new JSONObject();
        URI uri = path.toUri();
        job.put("uri", uri.toString());
        job.put("filePath", path);
        job.put("capacity", capacity);
        job.put("rowGroupIndices", rowgroupIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("columnIndices", columnIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("ugi", ugi);

        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("port", uri.getPort() == -1 ? "" : String.valueOf(uri.getPort()));
        job.put("path", uri.getPath() == null ? "" : uri.getPath());
        job.put("notNeedFSCache", true);
        parquetReader = jniReader.initializeReader(job);
        return parquetReader;
    }

    public int next(Vec[] vecList, List<DataType> types) {
        int vectorCnt = vecList.length;
        long[] vecNativeIds = new long[vectorCnt];
        long rtn = jniReader.recordReaderNext(parquetReader, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }
        for (int i = 0; i < vectorCnt; i++) {
            DataType type = types.get(i);
            if (type instanceof LongDataType) {
                vecList[i] = new LongVec(vecNativeIds[i]);
            } else if (type instanceof BooleanDataType) {
                vecList[i] = new BooleanVec(vecNativeIds[i]);
            } else if (type instanceof ShortDataType) {
                vecList[i] = new ShortVec(vecNativeIds[i]);
            } else if (type instanceof IntDataType) {
                vecList[i] = new IntVec(vecNativeIds[i]);
            } else if (type instanceof Decimal64DataType) {
                vecList[i] = new LongVec(vecNativeIds[i]);
            } else if (type instanceof Decimal128DataType) {
                vecList[i] = new Decimal128Vec(vecNativeIds[i]);
            } else if (type instanceof CharDataType) {
                vecList[i] = new VarcharVec(vecNativeIds[i]);
            } else if (type instanceof DoubleDataType) {
                vecList[i] = new DoubleVec(vecNativeIds[i]);
            } else if (type instanceof VarcharDataType) {
                vecList[i] = new VarcharVec(vecNativeIds[i]);
            } else if (type instanceof Date32DataType) {
                vecList[i] = new IntVec(vecNativeIds[i]);
            } else {
                throw new RuntimeException("Unsupport type for ColumnarFileScan: " +type.getId());
            }
        }
        return (int)rtn;
    }

    public void close() {
        jniReader.recordReaderClose(parquetReader);
    }
}