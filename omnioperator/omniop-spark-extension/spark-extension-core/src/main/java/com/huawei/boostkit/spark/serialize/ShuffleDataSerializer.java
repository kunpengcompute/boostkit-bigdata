/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.serialize;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.huawei.boostkit.spark.jni.NativeLoader;
import nova.hetu.omniruntime.type.*;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.*;
import nova.hetu.omniruntime.type.DataType.DataTypeId;

import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Arrays;
 
import sun.misc.Unsafe;

public class ShuffleDataSerializer {
    private static final Logger LOG = LoggerFactory.getLogger(NativeLoader.class);
    private static final Unsafe unsafe;
    private static final long BYTE_ARRAY_BASE_OFFSET;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("create unsafe object failed. errmsg:" + e.getMessage());
        }
    }

    public static ColumnarBatch deserialize(boolean isRowShuffle, byte[] bytes, int readSize) {
        ColumnVector[] vecs = null;
        long address = -1;
        ShuffleDataSerializerUtils deserializer = null;
        try {
            address = unsafe.allocateMemory(readSize);
            unsafe.copyMemory(bytes, BYTE_ARRAY_BASE_OFFSET, null, address, readSize);

            deserializer = new ShuffleDataSerializerUtils();
            deserializer.init(address, readSize, isRowShuffle);
            int vecCount = deserializer.getVecCount();
            int rowCount = deserializer.getRowCount();

            int[] typeIdArray = new int[vecCount];
            int[] precisionArray = new int[vecCount];
            int[] scaleArray = new int[vecCount];
            long[] vecNativeIdArray = new long[vecCount];
            deserializer.parse(typeIdArray, precisionArray, scaleArray, vecNativeIdArray);
            vecs = new ColumnVector[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(typeIdArray[i], vecNativeIdArray[i], rowCount, precisionArray[i], scaleArray[i]);
            }
            deserializer.close();
            unsafe.freeMemory(address);
            return new ColumnarBatch(vecs, rowCount);
        } catch (OmniRuntimeException e) {
            if (vecs != null) {
                for (int i = 0; i < vecs.length; i++) {
                    ColumnVector vec = vecs[i];
                    if (vec != null) {
                        vec.close();
                    }
                }
            }
            if (deserializer != null) {
                deserializer.close();
            }
            if (address != -1) {
                unsafe.freeMemory(address);
            }
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        }
    }

    private static ColumnVector buildVec(int typeId, long vecNativeId, int vecSize, int precision, int scale) {
        Vec vec;
        DataType type;
        switch (DataTypeId.values()[typeId]) {
            case OMNI_INT:
                type = DataTypes.IntegerType;
                vec = new IntVec(vecNativeId);
                break;
            case OMNI_DATE32:
                type = DataTypes.DateType;
                vec = new IntVec(vecNativeId);
                break;
            case OMNI_LONG:
                type = DataTypes.LongType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_TIMESTAMP:
                type = DataTypes.TimestampType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_DATE64:
                type = DataTypes.DateType;
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_DECIMAL64:
                type = DataTypes.createDecimalType(precision, scale);
                vec = new LongVec(vecNativeId);
                break;
            case OMNI_SHORT:
                type = DataTypes.ShortType;
                vec = new ShortVec(vecNativeId);
                break;
            case OMNI_BOOLEAN:
                type = DataTypes.BooleanType;
                vec = new BooleanVec(vecNativeId);
                break;
            case OMNI_DOUBLE:
                type = DataTypes.DoubleType;
                vec = new DoubleVec(vecNativeId);
                break;
            case OMNI_VARCHAR:
            case OMNI_CHAR:
                type = DataTypes.StringType;
                vec = new VarcharVec(vecNativeId);
                break;
            case OMNI_DECIMAL128:
                type = DataTypes.createDecimalType(precision, scale);
                vec = new Decimal128Vec(vecNativeId);
                break;
            case OMNI_TIME32:
            case OMNI_TIME64:
            case OMNI_INTERVAL_DAY_TIME:
            case OMNI_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + typeId);
        }
        OmniColumnVector vecTmp = new OmniColumnVector(vecSize, type, false);
        vecTmp.setVec(vec);
        return vecTmp;
    }
}
