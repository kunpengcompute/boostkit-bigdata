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

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.DoubleColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.lazy.LazyDouble;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleVecConverter implements VecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getDouble(index);
        }
        DoubleVec DoubleVec = (DoubleVec) vec;
        return DoubleVec.get(index);
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        double doubleValue;
        if (col instanceof LazyDouble) {
            LazyDouble lazyDouble = (LazyDouble) col;
            doubleValue = lazyDouble.getWritableObject().get();
        } else if (col instanceof DoubleWritable) {
            doubleValue = ((DoubleWritable) col).get();
        } else {
            doubleValue = (double) col;
        }
        return doubleValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        DoubleVec doubleVec = new DoubleVec(columnSize);
        double[] doubleValues = new double[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                doubleVec.setNull(i);
                continue;
            }
            doubleValues[i] = (double) col[i];
        }
        doubleVec.put(doubleValues, 0, 0, columnSize);
        return doubleVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        DoubleVec doubleVec = new DoubleVec(columnSize);
        DoubleColumnCache doubleColumnCache = (DoubleColumnCache) columnCache;
        if (doubleColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                doubleVec.set(i, doubleColumnCache.dataCache[i]);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (doubleColumnCache.isNull[i]) {
                    doubleVec.setNull(i);
                } else {
                    doubleVec.set(i, doubleColumnCache.dataCache[i]);
                }
            }
        }
        return doubleVec;
    }

    @Override
    public void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                         ColumnCache columnCache, int colIndex, int rowCount,
                                         PrimitiveTypeInfo primitiveTypeInfo) {
        DoubleColumnVector columnVector = (DoubleColumnVector) vectorizedRowBatch.cols[vectorColIndex];
        DoubleColumnCache doubleColumnCache = (DoubleColumnCache) columnCache;
        double[] vector = columnVector.vector;
        if (!columnVector.noNulls) {
            doubleColumnCache.noNulls = false;
        }
        if (columnVector.isRepeating) {
            if (columnVector.isNull[0]) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    doubleColumnCache.isNull[rowCount + i] = true;
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    doubleColumnCache.dataCache[rowCount + i] = vector[0];
                }
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    doubleColumnCache.dataCache[rowCount + i] = vector[vectorizedRowBatch.selected[i]];
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[vectorizedRowBatch.selected[i]]) {
                        doubleColumnCache.isNull[rowCount + i] = true;
                    } else {
                        doubleColumnCache.dataCache[rowCount + i] = vector[vectorizedRowBatch.selected[i]];
                    }
                }
            }
        } else {
            if (columnVector.noNulls) {
                System.arraycopy(vector, 0, doubleColumnCache.dataCache, rowCount, vectorizedRowBatch.size);
            } else {
                System.arraycopy(vector, 0, doubleColumnCache.dataCache, rowCount, vectorizedRowBatch.size);
                System.arraycopy(columnVector.isNull, 0, doubleColumnCache.isNull, rowCount, vectorizedRowBatch.size);
            }
        }
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        DoubleColumnVector doubleColumnVector = new DoubleColumnVector();
        for (int i = start; i < end; i++) {
            if (vec.isNull(i)) {
                doubleColumnVector.vector[i - start] = 0.00;
                doubleColumnVector.isNull[i - start] = true;
                doubleColumnVector.noNulls = false;
                continue;
            }
            double value;
            if (vec instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vec;
                value = dictionaryVec.getDouble(i);
            } else {
                DoubleVec doubleVec = (DoubleVec) vec;
                value = doubleVec.get(i);
            }
            doubleColumnVector.vector[i - start] = value;
        }
        return doubleColumnVector;
    }
}
