/*
 * Copyright (C) 2023-2023. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.scan.jni;
import org.json.JSONObject;


public class OrcColumnarBatchJniReader {

    public OrcColumnarBatchJniReader() {
        NativeReaderLoader.getInstance();
    }

    public native long initializeReader(String path, JSONObject job);

    public native long initializeRecordReader(long reader, JSONObject job);

    public native long initializeBatch(long rowReader, long batchSize);

    public native long recordReaderNext(long rowReader, long batchReader, int[] typeId, long[] vecNativeId);

    public native long recordReaderGetRowNumber(long rowReader);

    public native float recordReaderGetProgress(long rowReader);

    public native void recordReaderClose(long rowReader, long reader, long batchReader);

    public native void recordReaderSeekToRow(long rowReader, long rowNumber);

    public native String[] getAllColumnNames(long reader);

    public native long getNumberOfRows(long rowReader, long batch);
}
