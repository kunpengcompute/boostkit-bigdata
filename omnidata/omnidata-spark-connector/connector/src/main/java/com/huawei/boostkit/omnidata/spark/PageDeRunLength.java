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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;

import java.lang.reflect.Field;
import java.util.Optional;

/**
 * DeCompress RunLength
 *
 * @since 2021-09-27
 */
public class PageDeRunLength {
    private static Field filedElementsAppended;

    static {
        try {
            filedElementsAppended = WritableColumnVector.class.getDeclaredField("elementsAppended");
            filedElementsAppended.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
    }

    /**
     * decompress byteColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of byteArray to decompress
     * @return decompressed byteColumnVector
     */
    public Optional<WritableColumnVector> decompressByteArray(int positionCount,
                                                              WritableColumnVector writableColumnVector) throws Exception {
        byte value = writableColumnVector.getByte(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ByteType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putBytes(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress booleanColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of booleanArray to decompress
     * @return decompressed booleanColumnVector
     */
    public Optional<WritableColumnVector> decompressBooleanArray(int positionCount,
                                                                 WritableColumnVector writableColumnVector) throws Exception {
        boolean value = writableColumnVector.getBoolean(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.BooleanType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putBooleans(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress intColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of intArray to decompress
     * @return decompressed intColumnVector
     */
    public Optional<WritableColumnVector> decompressIntArray(int positionCount,
                                                             WritableColumnVector writableColumnVector) throws Exception {
        int value = writableColumnVector.getInt(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.IntegerType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putInts(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress shortColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of shortArray to decompress
     * @return decompressed shortColumnVector
     */
    public Optional<WritableColumnVector> decompressShortArray(int positionCount,
                                                               WritableColumnVector writableColumnVector) throws Exception {
        short value = writableColumnVector.getShort(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.ShortType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putShorts(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress longColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of longArray to decompress
     * @return decompressed longColumnVector
     */
    public Optional<WritableColumnVector> decompressLongArray(int positionCount,
                                                              WritableColumnVector writableColumnVector) throws Exception {
        long value = writableColumnVector.getLong(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.LongType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putLongs(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress floatColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of floatArray to decompress
     * @return decompressed floatColumnVector
     */
    public Optional<WritableColumnVector> decompressFloatArray(int positionCount,
                                                               WritableColumnVector writableColumnVector) throws Exception {
        float value = writableColumnVector.getFloat(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.FloatType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putFloats(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress doubleColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of doubleArray to decompress
     * @return decompressed doubleColumnVector
     */
    public Optional<WritableColumnVector> decompressDoubleArray(int positionCount,
                                                                WritableColumnVector writableColumnVector) throws Exception {
        double value = writableColumnVector.getDouble(0);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.DoubleType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            columnVector.putDoubles(0, positionCount, value);
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress stringColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of string to decompress
     * @return decompressed stringColumnVector
     */
    public Optional<WritableColumnVector> decompressVariableWidth(int positionCount,
                                                                  WritableColumnVector writableColumnVector) throws Exception {
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, DataTypes.StringType);
        if (writableColumnVector.isNullAt(0)) {
            columnVector.putNulls(0, positionCount);
        } else {
            throw new UnsupportedOperationException();
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }

    /**
     * decompress decimalColumnVector
     *
     * @param positionCount        the positionCount to decompress
     * @param writableColumnVector the columnVector of decimal to decompress
     * @return decompressed stringColumnVector
     */
    public Optional<WritableColumnVector> decompressDecimal(int positionCount,
                                                            WritableColumnVector writableColumnVector) throws Exception {
        int precision = ((DecimalType) writableColumnVector.dataType()).precision();
        int scale = ((DecimalType) writableColumnVector.dataType()).scale();
        Decimal value = writableColumnVector.getDecimal(0, precision, scale);
        WritableColumnVector columnVector = new OnHeapColumnVector(positionCount, writableColumnVector.dataType());
        for (int rowId = 0; rowId < positionCount; rowId++) {
            if (writableColumnVector.isNullAt(rowId)) {
                columnVector.putNull(rowId);
            } else {
                columnVector.putDecimal(rowId, value, precision);
            }
        }
        try {
            PageDeRunLength.filedElementsAppended.set(columnVector, positionCount);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
        return Optional.of(columnVector);
    }
}
