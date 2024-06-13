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

public class FixedWidthColumnSerDe implements ColumnSerDe {
    protected static byte[] EMPTY = new byte[16];

    protected int columnTypeLen;
    protected transient byte columnNullMarker;

    public FixedWidthColumnSerDe(int columnTypeLen) {
        this.columnTypeLen = columnTypeLen;
    }

    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int index = vecWrapper.index;
        int totalLen = offset;
        if (vecWrapper.isNull[index] == 1) {
            writeBytes[totalLen] = -1;
            ++totalLen;
            return totalLen;
        }
        int valueLen = trimBytes(vecWrapper.value, index * columnTypeLen, columnTypeLen);
        writeBytes[totalLen] = (byte) valueLen;
        ++totalLen;
        // write value array
        System.arraycopy(vecWrapper.value, index * columnTypeLen, writeBytes, totalLen, valueLen);
        totalLen = totalLen + valueLen;
        return totalLen;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        int totalLen = offset;
        if (bytes[totalLen] == -1) {
            vecSerdeBody.isNull = 1;
            ++totalLen;
            System.arraycopy(EMPTY, 0, vecSerdeBody.value, 0, columnTypeLen);
            return totalLen;
        }
        vecSerdeBody.isNull = 0;
        int length = bytes[totalLen];
        ++totalLen;
        System.arraycopy(bytes, totalLen, vecSerdeBody.value, 0, length);
        System.arraycopy(EMPTY, 0, vecSerdeBody.value, length, columnTypeLen - length);
        totalLen = totalLen + length;
        return totalLen;
    }

    private int trimBytes(byte[] bytes, int start, int length) {
        int index = 0;
        for (int i = length - 1; i >= 0; i--) {
            if (bytes[start + i] != 0) {
                index = i;
                break;
            }
        }
        return index + 1;
    }
}
