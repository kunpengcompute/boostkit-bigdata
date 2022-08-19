/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;

import com.huawei.boostkit.omnidata.decode.AbstractDecoding;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.DateType;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import com.huawei.boostkit.omnidata.decode.type.DecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToFloatDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToIntDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToByteDecodeType;
import com.huawei.boostkit.omnidata.decode.type.LongToShortDecodeType;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * PageDecoding
 */
public class PageDecoding extends AbstractDecoding<ColumnVector[]> {
    private final int BATCH_SIZE = VectorizedRowBatch.DEFAULT_SIZE;

    @Override
    public ColumnVector[] decodeArray(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector[] decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] longColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = sliceInput.readByte();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            longColumnVectors[i] = longColumnVector;
        }
        return longColumnVectors;
    }

    @Override
    public ColumnVector[] decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeByteArray(type, sliceInput);
    }

    @Override
    public ColumnVector[] decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    // int
    @Override
    public ColumnVector[] decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] intColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = sliceInput.readInt();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            intColumnVectors[i] = longColumnVector;
        }
        return intColumnVectors;
    }

    @Override
    public ColumnVector[] decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector[] decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] shortColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = sliceInput.readShort();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            shortColumnVectors[i] = longColumnVector;
        }
        return shortColumnVectors;
    }

    @Override
    public ColumnVector[] decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] longColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = sliceInput.readLong();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            longColumnVectors[i] = longColumnVector;
        }
        return longColumnVectors;
    }

    // float
    @Override
    public ColumnVector[] decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] floatColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    doubleColumnVector.vector[position] = intBitsToFloat(sliceInput.readInt());
                    doubleColumnVector.isNull[position] = false;
                } else {
                    doubleColumnVector.isNull[position] = true;
                    doubleColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            floatColumnVectors[i] = doubleColumnVector;
        }
        return floatColumnVectors;
    }

    // double
    @Override
    public ColumnVector[] decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] doubleColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    doubleColumnVector.vector[position] = longBitsToDouble(sliceInput.readLong());
                    doubleColumnVector.isNull[position] = false;
                } else {
                    doubleColumnVector.isNull[position] = true;
                    doubleColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            doubleColumnVectors[i] = doubleColumnVector;
        }
        return doubleColumnVectors;
    }

    @Override
    public ColumnVector[] decodeMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector[] decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ColumnVector[] decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT,  Math.multiplyExact(positionCount, SIZE_OF_INT));
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        int blockSize = sliceInput.readInt();

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] bytesColumnVectors = new ColumnVector[batchCount];
        int curOffset = offsets[0];
        int nextOffset;
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            BytesColumnVector bytesColumnVector = new BytesColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    nextOffset = offsets[batchPosition + position + 1];
                    int length = nextOffset - curOffset;
                    curOffset = nextOffset;
                    byte[] bytes = new byte[length];
                    sliceInput.readBytes(bytes, 0, length);
                    bytesColumnVector.vector[position] = bytes;
                    bytesColumnVector.length[position] = length;
                    bytesColumnVector.isNull[position] = false;
                } else {
                    bytesColumnVector.isNull[position] = true;
                    bytesColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            bytesColumnVectors[i] = bytesColumnVector;
        }
        return bytesColumnVectors;
    }

    @Override
    public ColumnVector[] decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
            throws InvocationTargetException, IllegalAccessException {
        // read the run length
        int positionCount = sliceInput.readInt();
        // decode first
        ColumnVector[] columnVectors = decode(type, sliceInput);
        ColumnVector[] resColumnVectors = null;
        String decodeName = typeToDecodeRunLengthName(type);
        if (columnVectors == null || decodeName == null) {
            throw new IllegalAccessException();
        }
        checkArgument(columnVectors.length == 1, "unanticipated results");
        //        throw new UnsupportedOperationException("Decoding run length is not supported.");
        //TODO No test scenario is found. To be verified.
        Map<String, Method> decompressMethods = new HashMap<>();
        Method[] methods = PageDeRunLength.class.getDeclaredMethods();
        for (Method method : methods) {
            decompressMethods.put(method.getName(), method);
        }
        Method method = decompressMethods.get(decodeName);

        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        Object objResult = method.invoke(pageDeRunLength, positionCount, columnVectors[0]);
        if (objResult instanceof ColumnVector[]) {
            resColumnVectors = (ColumnVector[]) objResult;
        }
        return resColumnVectors;
    }

    @Override
    public ColumnVector[] decodeRow(Optional<DecodeType> type, SliceInput sliceInput) {
        return null;
    }

    @Override
    public ColumnVector[] decodeDate(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] longColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = sliceInput.readInt();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            longColumnVectors[i] = longColumnVector;
        }
        return longColumnVectors;
    }

    @Override
    public ColumnVector[] decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] intColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = (int) sliceInput.readLong();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            intColumnVectors[i] = longColumnVector;
        }
        return intColumnVectors;
    }

    @Override
    public ColumnVector[] decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] shortColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = (short) sliceInput.readLong();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            shortColumnVectors[i] = longColumnVector;
        }
        return shortColumnVectors;
    }

    @Override
    public ColumnVector[] decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] longColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            LongColumnVector longColumnVector = new LongColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    longColumnVector.vector[position] = (byte) sliceInput.readLong();
                    longColumnVector.isNull[position] = false;
                } else {
                    longColumnVector.isNull[position] = true;
                    longColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            longColumnVectors[i] = longColumnVector;
        }
        return longColumnVectors;
    }

    @Override
    public ColumnVector[] decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        int remainderCount = positionCount % BATCH_SIZE;
        if (remainderCount == 0) {
            remainderCount = BATCH_SIZE;
        }
        int loopPositionCount = BATCH_SIZE;
        int batchPosition = 0;

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int batchCount = (positionCount + BATCH_SIZE - 1) / BATCH_SIZE;
        ColumnVector[] floatColumnVectors = new ColumnVector[batchCount];
        for (int i = 0; i < batchCount; i++) {
            if (batchCount - 1 == i) {
                loopPositionCount = remainderCount;
            }
            DoubleColumnVector doubleColumnVector = new DoubleColumnVector(loopPositionCount);
            for (int position = 0; position < loopPositionCount; position++) {
                if (valueIsNull == null || !valueIsNull[batchPosition + position]) {
                    doubleColumnVector.vector[position] = intBitsToFloat((int) sliceInput.readLong());
                    doubleColumnVector.isNull[position] = false;
                } else {
                    doubleColumnVector.isNull[position] = true;
                    doubleColumnVector.noNulls = false;
                }
            }
            batchPosition += BATCH_SIZE;
            floatColumnVectors[i] = doubleColumnVector;
        }
        return floatColumnVectors;
    }

    private String typeToDecodeRunLengthName(Optional<DecodeType> optType) {
        Class<?> javaType = null;
        if (!optType.isPresent()) {
            return null;
        }
        DecodeType type = optType.get();
        if (type.getJavaType().isPresent()) {
            javaType = type.getJavaType().get();
        }
        if (javaType == double.class) {
            return "decompressDoubleArray";
        }
        if (javaType == float.class || javaType == LongToFloatDecodeType.class) {
            return "decompressFloatArray";
        }
        if (javaType == int.class || javaType == LongToIntDecodeType.class || javaType == DateType.class) {
            return "decompressIntArray";
        }
        if (javaType == long.class) {
            return "decompressLongArray";
        }
        if (javaType == byte.class || javaType == LongToByteDecodeType.class) {
            return "decompressByteArray";
        }
        if (javaType == boolean.class) {
            return "decompressBooleanArray";
        }
        if (javaType == short.class || javaType == LongToShortDecodeType.class) {
            return "decompressShortArray";
        }
        if (javaType == String.class) {
            return "decompressVariableWidth";
        }
        return null;
    }
}