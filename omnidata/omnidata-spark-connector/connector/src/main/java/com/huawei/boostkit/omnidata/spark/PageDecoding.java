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

package com.huawei.boostkit.omnidata.spark;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import com.huawei.boostkit.omnidata.decode.AbstractDecoding;
import com.huawei.boostkit.omnidata.decode.type.*;
import com.huawei.boostkit.omnidata.exception.OmniDataException;

import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import io.prestosql.spi.type.DateType;

import io.prestosql.spi.type.Decimals;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Decode data to spark writableColumnVector
 *
 * @since 2021-03-30
 */
public class PageDecoding extends AbstractDecoding<Optional<WritableColumnVector>> {
    private static Field filedElementsAppended;

    static {
        try {
            filedElementsAppended = WritableColumnVector.class.getDeclaredField("elementsAppended");
            filedElementsAppended.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<WritableColumnVector> decodeArray(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ByteType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putByte(position, sliceInput.readByte());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.BooleanType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                boolean value = sliceInput.readByte() != 0;
                columnVector.putBoolean(position, value);
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.IntegerType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putInt(position, sliceInput.readInt());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ShortType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putShort(position, sliceInput.readShort());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.LongType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putLong(position, sliceInput.readLong());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.FloatType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putFloat(position, intBitsToFloat(sliceInput.readInt()));
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.DoubleType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putDouble(position, longBitsToDouble(sliceInput.readLong()));
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, positionCount * SIZE_OF_INT);
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        int blockSize = sliceInput.readInt();
        int curOffset = offsets[0];
        int nextOffset;
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.StringType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                nextOffset = offsets[position + 1];
                int length = nextOffset - curOffset;
                curOffset = nextOffset;
                byte[] bytes = new byte[length];
                sliceInput.readBytes(bytes, 0, length);
                columnVector.putByteArray(position, bytes, 0, length);
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
            throws InvocationTargetException, IllegalAccessException {
        int positionCount = sliceInput.readInt();
        Optional<WritableColumnVector> resColumnVector = Optional.empty();

        Optional<WritableColumnVector> optColumnVector = decode(type, sliceInput);
        if (!optColumnVector.isPresent()) {
            return resColumnVector;
        }
        WritableColumnVector columnVector = optColumnVector.get();

        Optional<String> decodeNameOpt = typeToDecodeName(type);
        if (!decodeNameOpt.isPresent()) {
            return resColumnVector;
        }
        String decodeName = decodeNameOpt.get();

        Map<String, Method> decompressMethods = new HashMap<>();
        Method[] methods = PageDeRunLength.class.getDeclaredMethods();
        for (Method method : methods) {
            decompressMethods.put(method.getName(), method);
        }
        Method method = decompressMethods.get(decodeName);
        PageDeRunLength pageDeRunLength = new PageDeRunLength();
        Object objResult = method.invoke(pageDeRunLength, positionCount, columnVector);
        if (objResult instanceof Optional) {
            Optional optResult = (Optional) objResult;
            if (optResult.isPresent() && (optResult.get() instanceof WritableColumnVector)) {
                WritableColumnVector writableColumnVector = (WritableColumnVector) optResult.get();
                resColumnVector = Optional.of(writableColumnVector);
            }
        }
        return resColumnVector;
    }

    @Override
    public Optional<WritableColumnVector> decodeRow(Optional<DecodeType> type, SliceInput sliceInput) {
        return Optional.empty();
    }

    @Override
    public Optional<WritableColumnVector> decodeDate(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.DateType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putInt(position, sliceInput.readInt());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.IntegerType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putInt(position, (int) sliceInput.readLong());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ShortType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putShort(position, (short) sliceInput.readLong());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ByteType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putByte(position, (byte) sliceInput.readLong());
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.FloatType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                columnVector.putFloat(position, intBitsToFloat((int) sliceInput.readLong()));
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeDecimal(Optional<DecodeType> type, SliceInput sliceInput, String decodeName) {
        int positionCount = sliceInput.readInt();
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        if (!(type.get() instanceof DecimalDecodeType)) {
            Optional.empty();
        }
        DecimalDecodeType decimalDecodeType = (DecimalDecodeType) type.get();
        int scale = decimalDecodeType.getScale();
        int precision = decimalDecodeType.getPrecision();
        OnHeapColumnVector columnVector = new OnHeapColumnVector(positionCount, new DecimalType(precision, scale));
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                BigInteger value = null;
                switch (decodeName) {
                    case "LONG_ARRAY":
                        value = BigInteger.valueOf(sliceInput.readLong());
                        break;
                    case "INT128_ARRAY":
                        value = Decimals.decodeUnscaledValue(sliceInput.readSlice(16));
                        break;
                    default:
                        throw new UnsupportedOperationException(decodeName + "is not supported.");
                }
                Decimal decimalValue = new Decimal().set(value);
                columnVector.putDecimal(position, decimalValue, precision);
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    @Override
    public Optional<WritableColumnVector> decodeTimestamp(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, TimestampType);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                // milliseconds to microsecond
                int rawOffset = TimeZone.getDefault().getRawOffset();
                columnVector.putLong(position, (sliceInput.readLong() - rawOffset) * 1000);
            } else {
                columnVector.putNull(position);
            }
        }
        try {
            PageDecoding.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new OmniDataException(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    private Optional<String> typeToDecodeName(Optional<DecodeType> optType) {
        Class<?> javaType = null;
        if (!optType.isPresent()) {
            return Optional.empty();
        }
        DecodeType type = optType.get();
        if (type.getJavaType().isPresent()) {
            javaType = type.getJavaType().get();
        }
        if (javaType == double.class) {
            return Optional.of("decompressDoubleArray");
        }
        if (javaType == float.class || javaType == LongToFloatDecodeType.class) {
            return Optional.of("decompressFloatArray");
        }
        if (javaType == int.class || javaType == LongToIntDecodeType.class || javaType == DateType.class) {
            return Optional.of("decompressIntArray");
        }
        if (javaType == long.class) {
            return Optional.of("decompressLongArray");
        }
        if (javaType == byte.class || javaType == LongToByteDecodeType.class) {
            return Optional.of("decompressByteArray");
        }
        if (javaType == boolean.class) {
            return Optional.of("decompressBooleanArray");
        }
        if (javaType == short.class || javaType == LongToShortDecodeType.class) {
            return Optional.of("decompressShortArray");
        }
        if (javaType == String.class) {
            return Optional.of("decompressVariableWidth");
        }
        if (javaType == DecimalDecodeType.class) {
            return Optional.of("decompressDecimal");
        }
        return Optional.empty();
    }
}
