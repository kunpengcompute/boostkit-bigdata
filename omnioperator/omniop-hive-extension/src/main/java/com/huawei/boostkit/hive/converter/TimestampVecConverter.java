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

import static org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch.DEFAULT_SIZE;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyTimestamp;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class TimestampVecConverter extends LongVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return Timestamp.ofEpochMilli(dictionaryVec.getLong(index));
        }
        LongVec timeVec = (LongVec) vec;
        return Timestamp.ofEpochMilli(timeVec.get(index));
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        long timeValue;
        if (col instanceof LazyTimestamp) {
            LazyTimestamp lazyTimestamp = (LazyTimestamp) col;
            timeValue = lazyTimestamp.getWritableObject().getTimestamp().toEpochMilli();
        } else if (col instanceof TimestampWritable) {
            timeValue = ((TimestampWritable) col).getTimestamp().getTime();
        } else if (col instanceof TimestampWritableV2) {
            timeValue = ((TimestampWritableV2) col).getTimestamp().toEpochMilli();
        } else {
            timeValue = ((Timestamp) col).toEpochMilli();
        }
        return timeValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        LongVec timestampVec = new LongVec(columnSize);
        long[] timestampValues = new long[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                timestampVec.setNull(i);
                continue;
            }
            timestampValues[i] = (long) col[i];
        }
        timestampVec.put(timestampValues, 0, 0, columnSize);
        return timestampVec;
    }

    @Override
    public void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                         ColumnCache columnCache, int colIndex, int rowCount) {
        ColumnVector columnVector = vectorizedRowBatch.cols[vectorColIndex];
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        long[] vector = ((TimestampColumnVector) columnVector).time;
        if (columnVector.isRepeating) {
            if (columnVector.isNull[0]) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    longColumnCache.isNull[rowCount + i] = true;
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    longColumnCache.dataCache[rowCount + i] = vector[0];
                }
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    longColumnCache.dataCache[rowCount + i] = vector[vectorizedRowBatch.selected[i]];
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[vectorizedRowBatch.selected[i]]) {
                        longColumnCache.isNull[rowCount + i] = true;
                    } else {
                        longColumnCache.dataCache[rowCount + i] = vector[vectorizedRowBatch.selected[i]];
                    }
                }
            }
        } else {
            if (columnVector.noNulls) {
                System.arraycopy(vector, 0, longColumnCache.dataCache, rowCount, vectorizedRowBatch.size);
            } else {
                System.arraycopy(vector, 0, longColumnCache.dataCache, rowCount, vectorizedRowBatch.size);
                System.arraycopy(columnVector.isNull, 0, longColumnCache.isNull, rowCount, vectorizedRowBatch.size);
            }
        }
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        TimestampColumnVector timestampColumnVector = new TimestampColumnVector();
        for (int i = start; i < end; i++) {
            Object value = fromOmniVec(vec, i);
            if (value == null) {
                timestampColumnVector.time[i - start] = 0L;
                timestampColumnVector.nanos[i - start] = 1;
                timestampColumnVector.isNull[i - start] = true;
                timestampColumnVector.noNulls = false;
            } else {
                timestampColumnVector.timestampUpdate(new java.sql.Timestamp((long) value), DEFAULT_SIZE);
            }
        }
        return timestampColumnVector;
    }
}
