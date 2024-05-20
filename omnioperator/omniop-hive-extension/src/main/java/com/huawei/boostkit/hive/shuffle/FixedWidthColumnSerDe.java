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
    protected int columnTypeLen;
    protected static byte[] EMPTY = new byte[16];
    protected transient byte columnNullMarker;

    public FixedWidthColumnSerDe(int columnTypeLen) {
        this.columnTypeLen = columnTypeLen;
    }

    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int index = vecWrapper.index;
        if (vecWrapper.isNull[index] == 1) {
            writeBytes[offset] = -1;
            ++offset;
            return offset;
        }
        int valueLen = trimBytes(vecWrapper.value, index * columnTypeLen, columnTypeLen);
        writeBytes[offset] = (byte) valueLen;
        ++offset;
        // write value array
        System.arraycopy(vecWrapper.value, index * columnTypeLen, writeBytes, offset, valueLen);
        offset = offset + valueLen;
        return offset;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        if (bytes[offset] == -1) {
            vecSerdeBody.isNull = 1;
            ++offset;
            System.arraycopy(EMPTY, 0, vecSerdeBody.value, 0, columnTypeLen);
            return offset;
        }
        vecSerdeBody.isNull = 0;
        int length = bytes[offset];
        ++offset;
        System.arraycopy(bytes, offset, vecSerdeBody.value, 0, length);
        System.arraycopy(EMPTY, 0, vecSerdeBody.value, length, columnTypeLen - length);
        offset = offset + length;
        return offset;
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
