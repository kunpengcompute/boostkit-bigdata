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

import org.apache.spark.sql.catalyst.util.RebaseDateTime;
import org.apache.spark.sql.execution.util.SparkMemoryUtils;
import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(PageDecoding.class);

    /**
     * Log appended files.
     */
    static Field filedElementsAppended;

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
        throw new UnsupportedOperationException("not support array decode");
    }

    @Override
    public Optional<WritableColumnVector> decodeByteArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.ByteType, "byte");
    }

    @Override
    public Optional<WritableColumnVector> decodeBooleanArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.BooleanType, "boolean");
    }

    @Override
    public Optional<WritableColumnVector> decodeIntArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.IntegerType, "int");
    }

    @Override
    public Optional<WritableColumnVector> decodeInt128Array(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeShortArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.ShortType, "short");
    }

    @Override
    public Optional<WritableColumnVector> decodeLongArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.LongType, "long");
    }

    @Override
    public Optional<WritableColumnVector> decodeFloatArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.FloatType, "float");
    }

    @Override
    public Optional<WritableColumnVector> decodeDoubleArray(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.DoubleType, "double");
    }

    @Override
    public Optional<WritableColumnVector> decodeMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<WritableColumnVector> decodeSingleMap(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    protected Optional<WritableColumnVector> decodeVariableWidthBase(
            Optional<DecodeType> type,
            SliceInput sliceInput,
            WritableColumnVector columnVector,
            int positionCount) {
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, Math.multiplyExact(positionCount, SIZE_OF_INT));
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        int blockSize = sliceInput.readInt();
        int curOffset = offsets[0];
        int nextOffset;
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                nextOffset = offsets[position + 1];
                int length = nextOffset - curOffset;
                curOffset = nextOffset;
                byte[] bytes = new byte[length];
                sliceInput.readBytes(bytes, 0, length);
                if (columnVector instanceof OnHeapColumnVector) {
                    columnVector.putByteArray(position, bytes, 0, length);
                } else {
                    columnVector.putBytes(position, length, bytes, 0);
                }
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
    public Optional<WritableColumnVector> decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        return decodeVariableWidthBase(type, sliceInput,
                new OnHeapColumnVector(positionCount, DataTypes.StringType), positionCount);
    }

    @Override
    public Optional<WritableColumnVector> decodeDictionary(Optional<DecodeType> type, SliceInput sliceInput) {
        throw new UnsupportedOperationException();
    }

    protected Optional<WritableColumnVector> decodeRunLengthBase(
            Optional<DecodeType> type,
            SliceInput sliceInput,
            PageDeRunLength pageDeRunLength)
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
    public Optional<WritableColumnVector> decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
            throws InvocationTargetException, IllegalAccessException {
        return decodeRunLengthBase(type, sliceInput, new PageDeRunLength());
    }

    @Override
    public Optional<WritableColumnVector> decodeRow(Optional<DecodeType> type, SliceInput sliceInput) {
        return Optional.empty();
    }

    @Override
    public Optional<WritableColumnVector> decodeDate(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.DateType, "date");
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToInt(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.IntegerType, "longToInt");
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToShort(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.ShortType, "longToShort");
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToByte(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.ByteType, "longToByte");
    }

    @Override
    public Optional<WritableColumnVector> decodeLongToFloat(Optional<DecodeType> type, SliceInput sliceInput) {
        return decodeSimple(sliceInput, DataTypes.FloatType, "longToFloat");
    }


    private WritableColumnVector createColumnVectorForDecimal(int positionCount, DecimalType decimalType) {
        return new OnHeapColumnVector(positionCount, decimalType);
    }

    @Override
    public Optional<WritableColumnVector> decodeDecimal(Optional<DecodeType> type, SliceInput sliceInput, String decodeName) {
        int positionCount = sliceInput.readInt();
        DecimalDecodeType decimalDecodeType;
        if ((type.get() instanceof DecimalDecodeType)) {
            decimalDecodeType = (DecimalDecodeType) type.get();
        } else {
            return Optional.empty();
        }
        int scale = decimalDecodeType.getScale();
        int precision = decimalDecodeType.getPrecision();
        WritableColumnVector columnVector = createColumnVectorForDecimal(positionCount, new DecimalType(precision, scale));
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                BigInteger value;
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
        return decodeSimple(sliceInput, DataTypes.TimestampType, "timestamp");
    }

    Optional<String> typeToDecodeName(Optional<DecodeType> optType) {
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

    Optional<WritableColumnVector> getWritableColumnVector(SliceInput sliceInput, int positionCount,
                                                           WritableColumnVector columnVector, String type) {
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                putData(columnVector, sliceInput, position, type);
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

    private void putData(WritableColumnVector columnVector, SliceInput sliceInput, int position, String type) {
        switch (type) {
            case "byte":
                columnVector.putByte(position, sliceInput.readByte());
                break;
            case "boolean":
                columnVector.putBoolean(position, sliceInput.readByte() != 0);
                break;
            case "int":
                columnVector.putInt(position, sliceInput.readInt());
                break;
            case "date":
                int src = sliceInput.readInt();
                int trans = RebaseDateTime.rebaseJulianToGregorianDays(src);
                columnVector.putInt(position, trans);
                break;
            case "short":
                columnVector.putShort(position, sliceInput.readShort());
                break;
            case "long":
                columnVector.putLong(position, sliceInput.readLong());
                break;
            case "float":
                columnVector.putFloat(position, intBitsToFloat(sliceInput.readInt()));
                break;
            case "double":
                columnVector.putDouble(position, longBitsToDouble(sliceInput.readLong()));
                break;
            case "longToInt":
                columnVector.putInt(position, (int) sliceInput.readLong());
                break;
            case "longToShort":
                columnVector.putShort(position, (short) sliceInput.readLong());
                break;
            case "longToByte":
                columnVector.putByte(position, (byte) sliceInput.readLong());
                break;
            case "longToFloat":
                columnVector.putFloat(position, intBitsToFloat((int) sliceInput.readLong()));
                break;
            case "timestamp":
                // milliseconds to microsecond
                int rawOffset = TimeZone.getDefault().getRawOffset();
                columnVector.putLong(position, (sliceInput.readLong() - rawOffset) * 1000);
                break;
            default:
        }
    }

    private Optional<WritableColumnVector> decodeSimple(
            SliceInput sliceInput,
            DataType dataType,
            String dataTypeName) {
        int positionCount = sliceInput.readInt();
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, dataType);
        return getWritableColumnVector(sliceInput, positionCount, columnVector, dataTypeName);
    }
}