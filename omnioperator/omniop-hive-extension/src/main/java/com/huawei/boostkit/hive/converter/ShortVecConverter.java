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
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;


public class ShortVecConverter extends LongVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getShort(index);
        }
        Object value;
        if (vec instanceof LongVec) {
            long longValue = ((LongVec) vec).get(index);
            if (longValue > Short.MAX_VALUE || longValue < Short.MIN_VALUE) {
                value = null;
            } else {
                value = (short) longValue;
            }
        } else if (vec instanceof IntVec) {
            int intValue = ((IntVec) vec).get(index);
            if (intValue > Short.MAX_VALUE || intValue < Short.MIN_VALUE) {
                value = null;
            } else {
                value = (short) intValue;
            }
        } else {
            value = ((ShortVec) vec).get(index);
        }
        return value;
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        short shortValue;
        if (col instanceof LazyShort) {
            LazyShort lazyShort = (LazyShort) col;
            shortValue = lazyShort.getWritableObject().get();
        } else if (col instanceof ShortWritable) {
            shortValue = ((ShortWritable) col).get();
        } else {
            shortValue = (short) col;
        }
        return shortValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        ShortVec shortVec = new ShortVec(columnSize);
        short[] shortValues = new short[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                shortVec.setNull(i);
                continue;
            }
            shortValues[i] = (short) col[i];
        }
        shortVec.put(shortValues, 0, 0, columnSize);
        return shortVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        ShortVec shortVec = new ShortVec(columnSize);
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        if (longColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                shortVec.set(i, (short) longColumnCache.dataCache[i]);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (longColumnCache.isNull[i]) {
                    shortVec.setNull(i);
                } else {
                    shortVec.set(i, (short) longColumnCache.dataCache[i]);
                }
            }
        }
        return shortVec;
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        LongColumnVector longColumnVector = new LongColumnVector();
        for (int i = start; i < end; i++) {
            Object value = fromOmniVec(vec, i);
            if (value == null) {
                longColumnVector.vector[i - start] = 1L;
                longColumnVector.isNull[i - start] = true;
                longColumnVector.noNulls = false;
            } else {
                longColumnVector.vector[i - start] = (short) value;
            }
        }
        return longColumnVector;
    }
}