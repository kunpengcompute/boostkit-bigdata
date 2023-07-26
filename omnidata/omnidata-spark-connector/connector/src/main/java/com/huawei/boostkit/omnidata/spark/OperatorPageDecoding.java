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

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

/**
 * Decode data to spark writableColumnVector for combine with operator
 *
 * @since 2023-07-20
 */
public class OperatorPageDecoding extends PageDecoding {

    static {
        SparkMemoryUtils.init();
    }

    @Override
    public Optional<WritableColumnVector> decodeVariableWidth(Optional<DecodeType> type, SliceInput sliceInput) {
        int positionCount = sliceInput.readInt();
        return decodeVariableWidthBase(type, sliceInput,
                new OmniColumnVector(positionCount, DataTypes.StringType, true), positionCount);
    }

    @Override
    public Optional<WritableColumnVector> decodeRunLength(Optional<DecodeType> type, SliceInput sliceInput)
            throws InvocationTargetException, IllegalAccessException {
        return decodeRunLengthBase(type, sliceInput, new OperatorPageDeRunLength());
    }

    private WritableColumnVector createColumnVectorForDecimal(int positionCount, DecimalType decimalType) {
        return new OmniColumnVector(positionCount, decimalType, true);
    }

    private Optional<WritableColumnVector> decodeSimple(
            SliceInput sliceInput,
            DataType dataType,
            String dataTypeName) {
        int positionCount = sliceInput.readInt();
        WritableColumnVector columnVector = new OmniColumnVector(positionCount, dataType, true);
        return getWritableColumnVector(sliceInput, positionCount, columnVector, dataTypeName);
    }
}