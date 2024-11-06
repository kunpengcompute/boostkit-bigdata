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
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.ShortDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.fs.Path;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchScanReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader jniReader;

    private ArrayList<String> allFieldsNames;

    public ParquetColumnarBatchScanReader() {
        jniReader = new ParquetColumnarBatchJniReader();
    }

    public long initializeReaderJava(Path path, int capacity, String ugi) {
        JSONObject job = new JSONObject();
        URI uri = path.toUri();
        job.put("uri", path.toString());
        job.put("capacity", capacity);
        job.put("ugi", ugi);

        job.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        job.put("host", uri.getHost() == null ? "" : uri.getHost());
        job.put("port", String.valueOf(uri.getPort()));
        job.put("path", uri.getPath() == null ? "" : uri.getPath());
        job.put("notNeedFSCache", true);
        parquetReader = jniReader.initializeReaderV2(job);
        return parquetReader;
    }

    public long initializeRecordReaderJava(long start, long end, String[] fieldNames) {
        JSONObject job = new JSONObject();
        job.put("start", start);
        job.put("end", end);
        job.put("fieldNames", fieldNames);

        parquetReader = jniReader.initializeRecordReader(parquetReader, job);
        return parquetReader;
    }

    public ArrayList<String> getAllFieldsNames() {
        if (allFieldsNames == null) {
            allFieldsNames = new ArrayList<>();
            jniReader.getAllFieldNames(parquetReader, allFieldsNames);
        }
        return allFieldsNames;
    }

    public int next(List<DataType> types, Vec[] vecList, boolean[] missingColumns) {
        int vectorCnt = missingColumns.length;
        long[] vecNativeIds = new long[vectorCnt];
        long rtn = jniReader.recordReaderNext(parquetReader, vecNativeIds);
        if (rtn == 0) {
            return 0;
        }

        int nativeGetId = 0;
        int rowCount = (int) rtn;
        for (int i = 0; i < vectorCnt; i++) {
            if (missingColumns[i]) {
                vecList[i] = getNullVec(types.get(i).getId(), rowCount);
                continue;
            }
            DataType type = types.get(i);
            if (type instanceof LongDataType) {
                vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof BooleanDataType) {
                vecList[i] = new BooleanVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof ShortDataType) {
                vecList[i] = new ShortVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof IntDataType) {
                vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof Decimal64DataType) {
                vecList[i] = new LongVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof Decimal128DataType) {
                vecList[i] = new Decimal128Vec(vecNativeIds[nativeGetId]);
            } else if (type instanceof CharDataType) {
                vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof DoubleDataType) {
                vecList[i] = new DoubleVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof VarcharDataType) {
                vecList[i] = new VarcharVec(vecNativeIds[nativeGetId]);
            } else if (type instanceof Date32DataType) {
                vecList[i] = new IntVec(vecNativeIds[nativeGetId]);
            } else {
                throw new RuntimeException("Unsupport type for ColumnarFileScan: " + type.getId());
            }
            nativeGetId++;
        }
        return (int) rtn;
    }

    private Vec getNullVec(DataType.DataTypeId value, int rowCount) {
        Vec vec;
        switch (value) {
            case OMNI_BOOLEAN: {
                vec = new BooleanVec(rowCount);
                break;
            }
            case OMNI_SHORT: {
                vec = new ShortVec(rowCount);
                break;
            }
            case OMNI_DATE32:
            case OMNI_INT: {
                vec = new IntVec(rowCount);
                break;
            }
            case OMNI_LONG:
            case OMNI_DECIMAL64: {
                vec = new LongVec(rowCount);
                break;
            }
            case OMNI_DOUBLE: {
                vec = new DoubleVec(rowCount);
                break;
            }
            case OMNI_VARCHAR: {
                vec = new VarcharVec(rowCount);
                break;
            }
            case OMNI_DECIMAL128: {
                vec = new Decimal128Vec(rowCount);
                break;
            }
            default: {
                throw new RuntimeException("UnSupport type for ColumnarFileScan:" + value);
            }
        }
        byte[] nullsBuf = new byte[rowCount];
        Arrays.fill(nullsBuf, (byte) 1);
        vec.setNullsBuf(nullsBuf);
        return vec;
    }

    public void close() {
        jniReader.recordReaderClose(parquetReader);
    }
}