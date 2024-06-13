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

package com.huawei.boostkit.hive.shuffle;

import nova.hetu.omniruntime.type.DataType;

public class SerDeUtils {
    public static void getSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, DataType.DataTypeId typeId, boolean invert) {
        switch (typeId) {
            case OMNI_BOOLEAN:
                getBooleanSerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_INT:
                getIntSerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_SHORT:
                getShortSerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                getLongSerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_DOUBLE:
                getDoubleSerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_DECIMAL128:
                getDecimal128SerializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            default:
                throw new RuntimeException("unSupport data type: " + typeId);
        }
    }

    public static void getDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, DataType.DataTypeId typeId, boolean invert) {
        switch (typeId) {
            case OMNI_BOOLEAN:
                getBooleanDeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_INT:
                getIntDeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_SHORT:
                getShortDeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_LONG:
            case OMNI_DECIMAL64:
                getLongDeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_DOUBLE:
                getDoubleDeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            case OMNI_DECIMAL128:
                getDecimal128DeserializeByte(originalByte, startOriginal, resultByte, startResult, invert);
                break;
            default:
                throw new RuntimeException("unSupport data type: " + typeId);
        }
    }

    public static void getBooleanSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getBooleanDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getIntSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte((byte) (originalByte[startOriginal + 3] ^ 0x80), invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getIntDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 3] = dealByte((byte) (originalByte[startOriginal] ^ 0x80), invert);
    }

    public static void getShortSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte((byte) (originalByte[startOriginal + 1] ^ 0x80), invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getShortDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 1] = dealByte((byte) (originalByte[startOriginal] ^ 0x80), invert);
    }

    public static void getLongSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte((byte) (originalByte[startOriginal + 7] ^ 0x80), invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 7] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getLongDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal + 7], invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 7] = dealByte((byte) (originalByte[startOriginal] ^ 0x80), invert);
    }

    public static void getDoubleSerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        if ((originalByte[startOriginal + 7] & ((byte) 1 << 7)) != 0) {
            originalByte[startOriginal + 7] = (byte) (0xff ^ originalByte[startOriginal + 7]);
            originalByte[startOriginal + 6] = (byte) (0xff ^ originalByte[startOriginal + 6]);
            originalByte[startOriginal + 5] = (byte) (0xff ^ originalByte[startOriginal + 5]);
            originalByte[startOriginal + 4] = (byte) (0xff ^ originalByte[startOriginal + 4]);
            originalByte[startOriginal + 3] = (byte) (0xff ^ originalByte[startOriginal + 3]);
            originalByte[startOriginal + 2] = (byte) (0xff ^ originalByte[startOriginal + 2]);
            originalByte[startOriginal + 1] = (byte) (0xff ^ originalByte[startOriginal + 1]);
            originalByte[startOriginal] = (byte) (0xff ^ originalByte[startOriginal]);
        } else {
            originalByte[startOriginal + 7] = (byte) (0x80 ^ originalByte[startOriginal + 7]);
        }
        resultByte[startResult] = dealByte(originalByte[startOriginal + 7], invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 7] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getDoubleDeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        if ((dealByte(originalByte[startOriginal], invert) & ((byte) 1 << 7)) == 0) {
            originalByte[startOriginal] = (byte) (0xff ^ originalByte[startOriginal]);
            originalByte[startOriginal + 1] = (byte) (0xff ^ originalByte[startOriginal + 1]);
            originalByte[startOriginal + 2] = (byte) (0xff ^ originalByte[startOriginal + 2]);
            originalByte[startOriginal + 3] = (byte) (0xff ^ originalByte[startOriginal + 3]);
            originalByte[startOriginal + 4] = (byte) (0xff ^ originalByte[startOriginal + 4]);
            originalByte[startOriginal + 5] = (byte) (0xff ^ originalByte[startOriginal + 5]);
            originalByte[startOriginal + 6] = (byte) (0xff ^ originalByte[startOriginal + 6]);
            originalByte[startOriginal + 7] = (byte) (0xff ^ originalByte[startOriginal + 7]);
        } else {
            originalByte[startOriginal] = (byte) (0x80 ^ originalByte[startOriginal]);
        }
        resultByte[startResult] = dealByte(originalByte[startOriginal + 7], invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 7] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getDecimal128SerializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte((byte) (originalByte[startOriginal + 15] ^ 0x80), invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 14], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 13], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 12], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 11], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 10], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 9], invert);
        resultByte[startResult + 7] = dealByte(originalByte[startOriginal + 8], invert);
        resultByte[startResult + 8] = dealByte(originalByte[startOriginal + 7], invert);
        resultByte[startResult + 9] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 10] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 11] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 12] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 13] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 14] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 15] = dealByte(originalByte[startOriginal], invert);
    }

    public static void getDecimal128DeserializeByte(byte[] originalByte, int startOriginal, byte[] resultByte,
            int startResult, boolean invert) {
        resultByte[startResult] = dealByte(originalByte[startOriginal + 15], invert);
        resultByte[startResult + 1] = dealByte(originalByte[startOriginal + 14], invert);
        resultByte[startResult + 2] = dealByte(originalByte[startOriginal + 13], invert);
        resultByte[startResult + 3] = dealByte(originalByte[startOriginal + 12], invert);
        resultByte[startResult + 4] = dealByte(originalByte[startOriginal + 11], invert);
        resultByte[startResult + 5] = dealByte(originalByte[startOriginal + 10], invert);
        resultByte[startResult + 6] = dealByte(originalByte[startOriginal + 9], invert);
        resultByte[startResult + 7] = dealByte(originalByte[startOriginal + 8], invert);
        resultByte[startResult + 8] = dealByte(originalByte[startOriginal + 7], invert);
        resultByte[startResult + 9] = dealByte(originalByte[startOriginal + 6], invert);
        resultByte[startResult + 10] = dealByte(originalByte[startOriginal + 5], invert);
        resultByte[startResult + 11] = dealByte(originalByte[startOriginal + 4], invert);
        resultByte[startResult + 12] = dealByte(originalByte[startOriginal + 3], invert);
        resultByte[startResult + 13] = dealByte(originalByte[startOriginal + 2], invert);
        resultByte[startResult + 14] = dealByte(originalByte[startOriginal + 1], invert);
        resultByte[startResult + 15] = dealByte((byte) (originalByte[startOriginal] ^ 0x80), invert);
    }

    public static byte dealByte(byte b, boolean invert) {
        if (invert) {
            return (byte) (0xff ^ b);
        }
        return b;
    }
}
