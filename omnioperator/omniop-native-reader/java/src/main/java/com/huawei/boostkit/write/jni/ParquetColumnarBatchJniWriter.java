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

package com.huawei.boostkit.write.jni;

import com.huawei.boostkit.scan.jni.NativeReaderLoader;

import org.json.JSONObject;

public class ParquetColumnarBatchJniWriter {
    public ParquetColumnarBatchJniWriter() {
        NativeReaderLoader.getInstance();
    }

    public native long initializeWriter(JSONObject var1);

    public native void initializeSchema(long writer, String[] fieldNames, int[] fieldTypes, boolean[] nullables, int[][]decimalParam);

    public native void write(long writer, long[] vecNativeId, int[] omniTypes, boolean[] dataColumnsIds, int rowNums);

    public native void splitWrite(long writer, long[] vecNativeId, int[] omniTypes, boolean[] dataColumnsIds, long starPos, long endPos);
}