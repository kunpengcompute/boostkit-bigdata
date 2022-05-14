/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.omnidata.decode;

import com.huawei.boostkit.omnidata.decode.type.DecodeType;

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
}
