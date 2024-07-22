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
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.io.IntWritable;

public class IntVecConverter extends LongVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getInt(index);
        }
        Object value;
        if (vec instanceof LongVec) {
            long longValue = ((LongVec) vec).get(index);
            if (longValue > Integer.MAX_VALUE || longValue < Integer.MIN_VALUE) {
                value = null;
            } else {
                value = (int) longValue;
            }
        } else {
            value = ((IntVec) vec).get(index);
        }
        return value;
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        int intValue;
        if (col instanceof LazyInteger) {
            LazyInteger lazyInteger = (LazyInteger) col;
            intValue = lazyInteger.getWritableObject().get();
        } else if (col instanceof IntWritable) {
            intValue = ((IntWritable) col).get();
        } else {
            intValue = (int) col;
        }
        return intValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        IntVec intVec = new IntVec(columnSize);
        int[] intValues = new int[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                intVec.setNull(i);
                continue;
            }
            intValues[i] = (int) col[i];
        }
        intVec.put(intValues, 0, 0, columnSize);
        return intVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        IntVec intVec = new IntVec(columnSize);
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        if (longColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                intVec.set(i, (int) longColumnCache.dataCache[i]);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (longColumnCache.isNull[i]) {
                    intVec.setNull(i);
                } else {
                    intVec.set(i, (int) longColumnCache.dataCache[i]);
                }
            }
        }
        return intVec;
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        LongColumnVector longColumnVector = new LongColumnVector();
        for (int i = start; i < end; i++) {
            if (vec.isNull(i)) {
                longColumnVector.vector[i - start] = 1L;
                longColumnVector.isNull[i - start] = true;
                longColumnVector.noNulls = false;
                continue;
            }
            long value;
            if (vec instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vec;
                value = dictionaryVec.getInt(i);
            } else if (vec instanceof LongVec) {
                value = (int) ((LongVec) vec).get(i);
                if (value < 0) {
                    longColumnVector.vector[i - start] = 1L;
                    longColumnVector.isNull[i - start] = true;
                    longColumnVector.noNulls = false;
                    continue;
                }
            } else {
                value = ((IntVec) vec).get(i);
            }
            longColumnVector.vector[i - start] = value;
        }
        return longColumnVector;
    }
}
