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
import com.huawei.boostkit.scan.jni.ParquetColumnarBatchJniReader;

import nova.hetu.omniruntime.vector.*;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;

public class ParquetColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchScanReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader jniReader;
    public ParquetColumnarBatchScanReader() {
        jniReader = new ParquetColumnarBatchJniReader();
    }

    public long initializeReaderJava(Path path, int capacity, List<Integer> rowgroupIndices, List<Integer> columnIndices, String ugi) {
        JSONObject job = new JSONObject();
        URI uri = path.toUri();

        job.put("uri", path.toString());
        job.put("capacity", capacity);
        job.put("rowGroupIndices", rowgroupIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("columnIndices", columnIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("ugi", ugi);

        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("port", uri.getPort());
        job.put("path", uri.getPath() == null ? "" : uri.getPath());
        System.out.println("======dzc=====" + job);
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
            if (type instanceof LongType) {
                vecList[i] = new LongVec(vecNativeIds[i]);
            } else if (type instanceof BooleanType) {
                vecList[i] = new BooleanVec(vecNativeIds[i]);
            } else if (type instanceof ShortType) {
                vecList[i] = new ShortVec(vecNativeIds[i]);
            } else if (type instanceof IntegerType) {
                vecList[i] = new IntVec(vecNativeIds[i]);
            } else if (type instanceof DecimalType) {
                if (DecimalType.is64BitDecimalType(type)) {
                    vecList[i] = new LongVec(vecNativeIds[i]);
                } else {
                    vecList[i] = new Decimal128Vec(vecNativeIds[i]);
                }
            } else if (type instanceof DoubleType) {
                vecList[i] = new DoubleVec(vecNativeIds[i]);
            } else if (type instanceof StringType) {
                vecList[i] = new VarcharVec(vecNativeIds[i]);
            } else if (type instanceof DateType) {
                vecList[i] = new IntVec(vecNativeIds[i]);
            } else if (type instanceof ByteType) {
                vecList[i] = new VarcharVec(vecNativeIds[i]);
            } else {
                throw new RuntimeException("Unsupport type for ColumnarFileScan: " + type.typeName());
            }
        }
        return (int)rtn;
    }

    public void close() {
        jniReader.recordReaderClose(parquetReader);
    }
}