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

package com.huawei.boostkit.hive.converter;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Decimal64VecConverter extends LongVecConverter {
    static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLastValue.class.getName());

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end,
                                                   PrimitiveObjectInspector primitiveObjectInspector) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        Decimal64ColumnVector decimal64ColumnVector = new Decimal64ColumnVector(decimalTypeInfo.getPrecision(),
                decimalTypeInfo.getScale());
        for (int i = start; i < end; i++) {
            if (vec.isNull(i)) {
                decimal64ColumnVector.vector[i - start] = 1L;
                decimal64ColumnVector.isNull[i - start] = true;
                decimal64ColumnVector.noNulls = false;
                continue;
            }
            long value;
            if (vec instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vec;
                value = dictionaryVec.getLong(i);
            } else {
                LongVec longVec = (LongVec) vec;
                value = longVec.get(i);
            }
            decimal64ColumnVector.vector[i - start] = value;
        }
        return decimal64ColumnVector;
    }

    public static boolean isConvertedDecimal64(String fieldName, VectorizationContext vectorizationContext) {
        boolean convertedDecimal64 = false;
        try {
            if (vectorizationContext != null) {
                convertedDecimal64 = vectorizationContext.getDataTypePhysicalVariation(
                        vectorizationContext.getInputColumnIndex(fieldName.replace("key.","KEY.").replace("value.","VALUE."))) == DataTypePhysicalVariation.DECIMAL_64;
            }
        } catch (HiveException e) {
            throw new RuntimeException("error occurs when finding field from vectorizationContext", e);
        }
        return convertedDecimal64;
    }
}