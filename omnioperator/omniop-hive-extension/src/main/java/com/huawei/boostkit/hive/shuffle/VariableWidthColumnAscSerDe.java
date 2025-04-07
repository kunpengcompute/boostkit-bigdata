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

import nova.hetu.omniruntime.utils.NullsBufHelper;

public class VariableWidthColumnAscSerDe implements ColumnSerDe {
    private transient byte columnNullMarker;

    public VariableWidthColumnAscSerDe(byte columnNullMarker) {
        this.columnNullMarker = columnNullMarker;
    }
    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int totalLen = offset;
        int index = vecWrapper.index;
        if (NullsBufHelper.isSet(vecWrapper.isNull, index) == 1) {
            if (columnNullMarker == 1) {
                writeBytes[totalLen++] = (byte) 0x7;
            } else {
                writeBytes[totalLen++] = (byte) 0x9;
            }
            writeBytes[totalLen++] = (byte) 0;
            return totalLen;
        }
        int valueLen = vecWrapper.offset[index + 1] - vecWrapper.offset[index];
        writeBytes[totalLen++] = (byte) 0x8;
        for (int i = 0; i < valueLen; i++) {
            totalLen = writeByte(totalLen, writeBytes, vecWrapper.value[vecWrapper.offset[index] + i]);
        }
        writeBytes[totalLen++] = 0;
        return totalLen;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        int totalLen = offset;
        totalLen++;
        if (bytes[totalLen] == 0) {
            if (bytes[totalLen - 1] == 0x8) {
                vecSerdeBody.isNull = 0;
            } else {
                vecSerdeBody.isNull = 1;
            }
            vecSerdeBody.length = 0;
            ++totalLen;
            return totalLen;
        }
        vecSerdeBody.isNull = 0;
        int vecSerdeBodyValueIndex = 0;
        int escapeCharNum = 0;
        for (int i = totalLen; i < bytes.length && bytes[i] != 0; i++) {
            if (bytes[i] == 1) {
                vecSerdeBody.value[vecSerdeBodyValueIndex++] = (byte) (bytes[++i] - 1);
                escapeCharNum++;
            } else {
                vecSerdeBody.value[vecSerdeBodyValueIndex++] = bytes[i];
            }
        }
        vecSerdeBody.length = vecSerdeBodyValueIndex;
        return (totalLen + vecSerdeBodyValueIndex + escapeCharNum + 1);
    }

    public int writeByte(int offset, byte[] writeBytes, byte byteValue) {
        int totalLen = offset;
        if (byteValue == 0 || byteValue == 1) {
            writeBytes[totalLen++] = (byte) (1);
            writeBytes[totalLen++] = (byte) (byteValue + 1);
        } else {
            writeBytes[totalLen++] = byteValue;
        }
        return totalLen;
    }
}
