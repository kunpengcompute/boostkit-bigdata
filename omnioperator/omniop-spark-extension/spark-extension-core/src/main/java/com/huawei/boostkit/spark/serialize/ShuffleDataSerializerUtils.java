/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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
 
package com.huawei.boostkit.spark.serialize;

import com.huawei.boostkit.spark.jni.NativeLoader;

public class ShuffleDataSerializerUtils {
    public long vecBatchAddress;
    public boolean isRowShuffle;

    public ShuffleDataSerializerUtils() {
        NativeLoader.getInstance();
    }

    public void init(long address, int length, boolean isRowShuffle) {
        this.isRowShuffle = isRowShuffle;
        if (isRowShuffle) {
            this.vecBatchAddress = rowShuffleParseInit(address, length);
        } else {
            this.vecBatchAddress = columnarShuffleParseInit(address, length);
        }
    }

    public void close() {
        if (isRowShuffle) {
            rowShuffleParseClose(vecBatchAddress);
        } else {
            columnarShuffleParseClose(vecBatchAddress);
        }
    }

    public int getVecCount() {
        if (isRowShuffle) {
            return rowShuffleParseVecCount(vecBatchAddress);
        } else {
            return columnarShuffleParseVecCount(vecBatchAddress);
        }
    }

    public int getRowCount() {
        if (isRowShuffle) {
            return rowShuffleParseRowCount(vecBatchAddress);
        } else {
            return columnarShuffleParseRowCount(vecBatchAddress);
        }
    }

    public void parse(int[] typeIdArray, int[] precisionArray, int[] scaleArray, long[] vecNativeIdArray) {
        if (isRowShuffle) {
            rowShuffleParseBatch(vecBatchAddress, typeIdArray, precisionArray, scaleArray, vecNativeIdArray);
        } else {
            columnarShuffleParseBatch(vecBatchAddress, typeIdArray, precisionArray, scaleArray, vecNativeIdArray);
        }
    }

    private native long rowShuffleParseInit(long address, int length);

    private native int rowShuffleParseVecCount(long vecBatchAddress);

    private native int rowShuffleParseRowCount(long vecBatchAddress);

    private native void rowShuffleParseClose(long vecBatchAddress);

    private native void rowShuffleParseBatch(long vecBatchAddress, int[] typeIdArray, int[] precisionArray, int[] scaleArray, long[] vecNativeIdArray);

    private native long columnarShuffleParseInit(long address, int length);

    private native int columnarShuffleParseVecCount(long vecBatchAddress);

    private native int columnarShuffleParseRowCount(long vecBatchAddress);

    private native void columnarShuffleParseClose(long vecBatchAddress);

    private native void columnarShuffleParseBatch(long vecBatchAddress, int[] typeIdArray, int[] precisionArray, int[] scaleArray, long[] vecNativeIdArray);
}