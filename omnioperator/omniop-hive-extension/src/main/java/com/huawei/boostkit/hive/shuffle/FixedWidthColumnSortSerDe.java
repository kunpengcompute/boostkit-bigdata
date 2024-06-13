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

public class FixedWidthColumnSortSerDe extends FixedWidthColumnSerDe {
    private final DataType.DataTypeId dataTypeId;
    private final boolean invert;

    public FixedWidthColumnSortSerDe(int columnTypeLen, byte columnNullMarker,
            DataType.DataTypeId dataTypeId, boolean invert) {
        super(columnTypeLen);
        this.columnNullMarker = columnNullMarker;
        this.dataTypeId = dataTypeId;
        this.invert = invert;
    }

    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int totalLen = offset;
        int index = vecWrapper.index;
        if (vecWrapper.isNull[index] == 1) {
            if (columnNullMarker == 1) {
                writeBytes[totalLen++] = (byte) (columnTypeLen - 1);
            } else {
                writeBytes[totalLen++] = (byte) (columnTypeLen + 1);
            }
            return totalLen;
        }
        writeBytes[totalLen++] = (byte) columnTypeLen;
        SerDeUtils.getSerializeByte(vecWrapper.value, index * columnTypeLen, writeBytes, totalLen, dataTypeId, invert);
        totalLen = totalLen + columnTypeLen;
        return totalLen;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        int totalLen = offset;
        if (bytes[totalLen] == columnTypeLen + 1 || bytes[totalLen] == columnTypeLen - 1) {
            vecSerdeBody.isNull = 1;
            ++totalLen;
            System.arraycopy(EMPTY, 0, vecSerdeBody.value, 0, columnTypeLen);
            return totalLen;
        }
        vecSerdeBody.isNull = 0;
        totalLen++;
        SerDeUtils.getDeserializeByte(bytes, totalLen, vecSerdeBody.value, 0, dataTypeId, invert);
        totalLen = totalLen + columnTypeLen;
        return totalLen;
    }
}
