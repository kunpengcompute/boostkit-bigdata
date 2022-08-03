/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.omnidata.decode;


import io.airlift.slice.SliceInput;

import org.apache.hadoop.hive.ql.omnidata.decode.type.DecodeType;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;

/**
 * Decode Slice to type
 *
 * @param <T>
 * @since 2022-07-28
 */
public interface Decoding<T>
{
    /**
     * decode
     *
     * @param type decode type
     * @param sliceInput content
     * @return T
     */
    T decode(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("ARRAY")
    T decodeArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode byte array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("BYTE_ARRAY")
    T decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode boolean array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("BOOLEAN_ARRAY")
    T decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode int array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("INT_ARRAY")
    T decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode int128 array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("INT128_ARRAY")
    T decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode short array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("SHORT_ARRAY")
    T decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode long array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("LONG_ARRAY")
    T decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode float array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("FLOAT_ARRAY")
    T decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode double array type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("DOUBLE_ARRAY")
    T decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode map type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("MAP")
    T decodeMap(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode map element type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("MAP_ELEMENT")
    T decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode variable width type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("VARIABLE_WIDTH")
    T decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode dictionary type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("DICTIONARY")
    T decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode rle type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     * @throws InvocationTargetException throw invocation target exception
     * @throws IllegalAccessException throw illegal access exception
     */
    @Decode("RLE")
    T decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
            throws InvocationTargetException, IllegalAccessException;

    /**
     * decode row type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("ROW")
    T decodeRow(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode date type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("DATE")
    T decodeDate(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode long to int type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("LONG_TO_INT")
    T decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode long to short type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("LONG_TO_SHORT")
    T decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode long to byte type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("LONG_TO_BYTE")
    T decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput);

    /**
     * decode long to float type
     *
     * @param type type of data to decode
     * @param sliceInput data to decode
     * @return T
     */
    @Decode("LONG_TO_FLOAT")
    T decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput);
}
