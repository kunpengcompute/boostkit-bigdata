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

import nova.hetu.omniruntime.vector.Vec;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ParquetColumnarBatchScanReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetColumnarBatchScanReader.class);

    public long parquetReader;

    public ParquetColumnarBatchJniReader jniReader;

    public ParquetColumnarBatchScanReader() {
        jniReader = new ParquetColumnarBatchJniReader();
    }

    public long initializeReaderJava(String path, int capacity,
                                     List<Integer> rowgroupIndices, List<Integer> columnIndices, String ugi) {
        JSONObject job = new JSONObject();
        job.put("filePath", path);
        job.put("capacity", capacity);
        job.put("rowGroupIndices", rowgroupIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("columnIndices", columnIndices.stream().mapToInt(Integer::intValue).toArray());
        job.put("ugi", ugi);
        parquetReader = jniReader.initializeReader(job);
        return parquetReader;
    }

    public int next(Vec[] vecList) {
        return 0;
    }

    public void close() {
        jniReader.recordReaderClose(parquetReader);
    }
}