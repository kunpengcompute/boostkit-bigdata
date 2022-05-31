/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
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

package com.huawei.boostkit.omnidata.decode;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

public interface Decoding<T> {

    T decode(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("ARRAY")
    T decodeArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("BYTE_ARRAY")
    T decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("BOOLEAN_ARRAY")
    T decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("INT_ARRAY")
    T decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("INT128_ARRAY")
    T decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("SHORT_ARRAY")
    T decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_ARRAY")
    T decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("FLOAT_ARRAY")
    T decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DOUBLE_ARRAY")
    T decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("MAP")
    T decodeMap(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("MAP_ELEMENT")
    T decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("VARIABLE_WIDTH")
    T decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DICTIONARY")
    T decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("RLE")
    T decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
        throws InvocationTargetException, IllegalAccessException;

    @Decode("ROW")
    T decodeRow(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DATE")
    T decodeDate(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_INT")
    T decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_SHORT")
    T decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_BYTE")
    T decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("LONG_TO_FLOAT")
    T decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput);

    @Decode("DECIMAL")
    T decodeDecimal(Optional<DecodeType> type, SliceInput sliceInput, String decodeType);

    @Decode("TIMESTAMP")
    T decodeTimestamp(Optional<DecodeType> type, SliceInput sliceInput);
}
