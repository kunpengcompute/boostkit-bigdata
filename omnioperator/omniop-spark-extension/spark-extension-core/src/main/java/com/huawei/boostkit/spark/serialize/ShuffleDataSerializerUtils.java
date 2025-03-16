/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
 
 package com.huawei.boostkit.spark.serialize;

 public class ShuffleDataSerializerUtils {
    private long vecBatchAddress;
    private int[] typeIdArray;

    public ShuffleDataSerializerUtils(long address, int length) {
        vecBatchAddress = parseInit(address, length);
    }

    public void parse(int[] typeIdArray, int[] precisionArray, int[] scaleArray, long[] vecNativeIdArray) {
        parseColumnarBatch(vecBatchAddress, typeIdArray, precisionArray, scaleArray, vecNativeIdArray);
    }

    public void close() {
        parseClose(vecBatchAddress);
    }

    public int getVecCount() {
        return parseVecCount(vecBatchAddress);
    }

    public int getRowCount() {
        return parseRowCount(vecBatchAddress);
    }

    private native void parseColumnarBatch(long vecBatchAddress, int[] typeIdArray, int[] precisionArray, int[] scaleArray, long[] vecNativeIdArray);

    private native long parseInit(long address, int length);

    private native int parseVecCount(long vecBatchAddress);

    private native int parseRowCount(long vecBatchAddress);

    private native void parseClose(long vecBatchAddress);
 }