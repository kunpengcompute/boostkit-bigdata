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

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import nova.hetu.omniruntime.utils.NullsBufHelper;

public class VariableWidthColumnSerDe implements ColumnSerDe {
    private byte[] lengthBytes = new byte[4];
    private LazyBinaryUtils.VInt vInt = new LazyBinaryUtils.VInt();

    @Override
    public int serialize(byte[] writeBytes, VecWrapper vecWrapper, int offset) {
        int totalLen = offset;
        int index = vecWrapper.index;
        if (NullsBufHelper.isSet(vecWrapper.isNull, index) == 1) {
            writeBytes[totalLen] = -1;
            ++totalLen;
            return totalLen;
        }
        // write length
        int valueLen = vecWrapper.offset[index + 1] - vecWrapper.offset[index];
        int len = LazyBinaryUtils.writeVLongToByteArray(lengthBytes, valueLen);
        System.arraycopy(lengthBytes, 0, writeBytes, totalLen, len);
        totalLen = totalLen + len;
        // write value array
        System.arraycopy(vecWrapper.value, vecWrapper.offset[index], writeBytes, totalLen, valueLen);
        totalLen = totalLen + valueLen;
        return totalLen;
    }

    @Override
    public int deserialize(VecSerdeBody vecSerdeBody, byte[] bytes, int offset) {
        int totalLen = offset;
        if (bytes[totalLen] == -1) {
            vecSerdeBody.isNull = 1;
            vecSerdeBody.length = 0;
            ++totalLen;
            return totalLen;
        }
        vecSerdeBody.isNull = 0;
        LazyBinaryUtils.readVInt(bytes, totalLen, vInt);
        vecSerdeBody.length = vInt.value;
        System.arraycopy(bytes, totalLen + vInt.length, vecSerdeBody.value, 0, vInt.value);
        totalLen = totalLen + vInt.length + vInt.value;
        return totalLen;
    }
}
