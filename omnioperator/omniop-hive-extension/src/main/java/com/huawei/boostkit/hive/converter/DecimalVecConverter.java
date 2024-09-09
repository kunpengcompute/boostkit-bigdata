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

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;
import static nova.hetu.omniruntime.vector.Decimal128Vec.longToBytes;
import static org.apache.hadoop.hive.common.type.HiveDecimal.SCRATCH_LONGS_LEN;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.DecimalColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class DecimalVecConverter implements VecConverter {
    private long[] scratchLongs = new long[SCRATCH_LONGS_LEN];
    private static final int DECIMAL_MAX_SCALE = 18;

    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        HiveDecimalWritable hiveDecimalWritable;
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            if (dictionaryVec.getType().getId() == LongDataType.LONG.getId()) {
                hiveDecimalWritable = new HiveDecimalWritable();
                hiveDecimalWritable.setFromLongAndScale(dictionaryVec.getLong(index), decimalTypeInfo.getScale());
            } else {
                hiveDecimalWritable = getDecimalWritableFromLong(dictionaryVec.getDecimal128(index),
                        decimalTypeInfo.getScale());
            }
        } else if (vec instanceof LongVec) {
            hiveDecimalWritable = new HiveDecimalWritable();
            hiveDecimalWritable.setFromLongAndScale(((LongVec) vec).get(index), decimalTypeInfo.getScale());
        } else if (vec instanceof IntVec) {
            hiveDecimalWritable = new HiveDecimalWritable();
            hiveDecimalWritable.setFromLongAndScale(((IntVec) vec).get(index), decimalTypeInfo.getScale());
        } else {
            Decimal128Vec decimal128Vec = (Decimal128Vec) vec;
            byte[] result = decimal128Vec.getBytes(index);
            hiveDecimalWritable = new HiveDecimalWritable(result, decimalTypeInfo.getScale());
        }
        return hiveDecimalWritable;
    }

    private HiveDecimalWritable getDecimalWritableFromLong(long[] longs, int scale) {
        byte[] bytes = new byte[16];
        byte[] highBytes = longToBytes(longs[1]);
        byte[] lowBytes = longToBytes(longs[0]);
        System.arraycopy(highBytes, 0, bytes, 0, 8);
        System.arraycopy(lowBytes, 0, bytes, 8, 8);
        return new HiveDecimalWritable(bytes, scale);
    }

    @Override
    public Object calculateValue(Object col, PrimitiveTypeInfo primitiveTypeInfo) {
        if (col == null) {
            return null;
        }
        HiveDecimal hiveDecimal;
        if (col instanceof LazyHiveDecimal) {
            LazyHiveDecimal lazyHiveDecimal = (LazyHiveDecimal) col;
            hiveDecimal = lazyHiveDecimal.getWritableObject().getHiveDecimal();
        } else if (col instanceof HiveDecimalWritable) {
            hiveDecimal = ((HiveDecimalWritable) col).getHiveDecimal();
        } else {
            hiveDecimal = (HiveDecimal) col;
        }
        return hiveDecimal;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        if (decimalTypeInfo.getPrecision() <= 18) {
            return toOmniLongVec(col, columnSize, decimalTypeInfo);
        }
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                decimal128Vec.setNull(i);
            } else {
                HiveDecimal hiveDecimal = (HiveDecimal) col[i];
                decimal128Vec.setBigInteger(i, hiveDecimal.bigIntegerBytesScaled(decimalTypeInfo.getScale()),
                        hiveDecimal.signum() == -1);
            }
        }
        return decimal128Vec;
    }

    private Vec toOmniLongVec(Object[] col, int columnSize, DecimalTypeInfo decimalTypeInfo) {
        LongVec longVec = new LongVec(columnSize);
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                longVec.setNull(i);
            } else {
                HiveDecimal hiveDecimal = (HiveDecimal) col[i];
                longVec.set(i, getLongFromHiveDecimal(hiveDecimal, decimalTypeInfo));
            }
        }
        return longVec;
    }

    private long getLongFromHiveDecimal(HiveDecimal hiveDecimal, DecimalTypeInfo decimalTypeInfo) {
        if (hiveDecimal.scale() < decimalTypeInfo.getScale() || hiveDecimal.scale() > DECIMAL_MAX_SCALE) {
            return hiveDecimal.scaleByPowerOfTen(decimalTypeInfo.getScale()).longValue();
        } else {
            return hiveDecimal.unscaledValue().longValue();
        }
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        if (columnCache instanceof LongColumnCache) {
            return toOmniVecLong(columnCache, columnSize);
        }
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        DecimalColumnCache decimal128ColumnCache = (DecimalColumnCache) columnCache;
        byte[] value = new byte[columnSize * 16];
        byte[] isNull = new byte[columnSize];
        if (decimal128ColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                System.arraycopy(decimal128ColumnCache.dataCache[i], 0, value, i * 16, 16);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (decimal128ColumnCache.isNull[i]) {
                    isNull[i] = 1;
                } else {
                    System.arraycopy(decimal128ColumnCache.dataCache[i], 0, value, i * 16, 16);
                }
            }
        }
        decimal128Vec.setValuesBuf(value);
        decimal128Vec.setNullsBuf(isNull);
        return decimal128Vec;
    }

    private Vec toOmniVecLong(ColumnCache columnCache, int columnSize) {
        LongVec longVec = new LongVec(columnSize);
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        if (longColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                longVec.set(i, longColumnCache.dataCache[i]);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (longColumnCache.isNull[i]) {
                    longVec.setNull(i);
                } else {
                    longVec.set(i, longColumnCache.dataCache[i]);
                }
            }
        }
        return longVec;
    }

    @Override
    public void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                         ColumnCache columnCache, int colIndex, int rowCount,
                                         PrimitiveTypeInfo primitiveTypeInfo) {
        if (columnCache instanceof LongColumnCache) {
            setValueFromColumnVectorLong(vectorizedRowBatch, vectorColIndex, columnCache, colIndex, rowCount,
                    primitiveTypeInfo);
            return;
        }
        ColumnVector columnVector = vectorizedRowBatch.cols[vectorColIndex];
        DecimalColumnCache decimalColumnCache = (DecimalColumnCache) columnCache;
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        HiveDecimalWritable[] vector = ((DecimalColumnVector) columnVector).vector;
        if (!columnVector.noNulls) {
            decimalColumnCache.noNulls = false;
        }
        if (columnVector.isRepeating) {
            if (columnVector.isNull[0]) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.isNull[rowCount + i] = true;
                }
            } else {
                byte[] decimalBytes = getDecimalBytes(vector[0], decimalTypeInfo);
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = decimalBytes;
                }
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[vectorizedRowBatch.selected[i]],
                            decimalTypeInfo);
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[vectorizedRowBatch.selected[i]]) {
                        decimalColumnCache.isNull[rowCount + i] = true;
                    } else {
                        decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(
                                vector[vectorizedRowBatch.selected[i]], decimalTypeInfo);
                    }
                }
            }
        } else {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[i], decimalTypeInfo);
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[i]) {
                        decimalColumnCache.isNull[rowCount + i] = true;
                    } else {
                        decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[i], decimalTypeInfo);
                    }
                }
            }
        }
    }

    private void setValueFromColumnVectorLong(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                              ColumnCache columnCache, int colIndex, int rowCount,
                                              PrimitiveTypeInfo primitiveTypeInfo) {
        ColumnVector columnVector = vectorizedRowBatch.cols[vectorColIndex];
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        HiveDecimalWritable[] vector = ((DecimalColumnVector) columnVector).vector;
        if (!columnVector.noNulls) {
            longColumnCache.noNulls = false;
        }
        if (columnVector.isRepeating) {
            if (columnVector.isNull[0]) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    longColumnCache.isNull[rowCount + i] = true;
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    longColumnCache.dataCache[rowCount + i] = getLongFromHiveDecimal(vector[0].getHiveDecimal(),
                            decimalTypeInfo);
                }
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            setSelecedInUseDataCache(vectorizedRowBatch, rowCount, columnVector, longColumnCache, decimalTypeInfo,
                    vector);
        } else {
            setDataCache(vectorizedRowBatch, rowCount, columnVector, longColumnCache, decimalTypeInfo, vector);
        }
    }

    private void setSelecedInUseDataCache(VectorizedRowBatch vectorizedRowBatch, int rowCount,
                                          ColumnVector columnVector, LongColumnCache longColumnCache,
                                          DecimalTypeInfo decimalTypeInfo, HiveDecimalWritable[] vector) {
        if (columnVector.noNulls) {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                longColumnCache.dataCache[rowCount + i] = getLongFromHiveDecimal(vector[vectorizedRowBatch.selected[i]]
                                .getHiveDecimal(), decimalTypeInfo);
            }
        } else {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                if (columnVector.isNull[vectorizedRowBatch.selected[i]]) {
                    longColumnCache.isNull[rowCount + i] = true;
                } else {
                    longColumnCache.dataCache[rowCount + i] = getLongFromHiveDecimal(
                            vector[vectorizedRowBatch.selected[i]].getHiveDecimal(), decimalTypeInfo);
                }
            }
        }
    }

    private void setDataCache(VectorizedRowBatch vectorizedRowBatch, int rowCount, ColumnVector columnVector,
                              LongColumnCache longColumnCache, DecimalTypeInfo decimalTypeInfo,
                              HiveDecimalWritable[] vector) {
        if (columnVector.noNulls) {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                longColumnCache.dataCache[rowCount + i] = getLongFromHiveDecimal(vector[i].getHiveDecimal(),
                        decimalTypeInfo);
            }
        } else {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                if (columnVector.isNull[i]) {
                    longColumnCache.isNull[rowCount + i] = true;
                } else {
                    longColumnCache.dataCache[rowCount + i] = getLongFromHiveDecimal(vector[i].getHiveDecimal(),
                            decimalTypeInfo);
                }
            }
        }
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end,
                                                   PrimitiveObjectInspector primitiveObjectInspector) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        DecimalColumnVector decimalColumnVector = new DecimalColumnVector(decimalTypeInfo.getPrecision(),
                decimalTypeInfo.getScale());
        for (int i = start; i < end; i++) {
            Object value = fromOmniVec(vec, i, primitiveObjectInspector);
            if (value == null) {
                decimalColumnVector.isNull[i - start] = true;
                decimalColumnVector.noNulls = false;
            } else {
                decimalColumnVector.vector[i - start] = (HiveDecimalWritable) value;
            }
        }
        return decimalColumnVector;
    }

    private byte[] getDecimalBytes(HiveDecimalWritable hiveDecimal, DecimalTypeInfo decimalTypeInfo) {
        boolean isNegative = hiveDecimal.signum() == -1;
        byte[] buffer = hiveDecimal.getHiveDecimal().bigIntegerBytesScaled(decimalTypeInfo.getScale());
        int byteArrayLength = buffer.length;
        if (byteArrayLength > 2 * Long.BYTES) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Decimal overflow.");
        }
        int i;
        byte[] result = new byte[16];
        for (i = 0; i < byteArrayLength; i++) {
            result[i] = buffer[byteArrayLength - i - 1];
        }
        if (isNegative) {
            for (i = byteArrayLength; i < 16; i++) {
                result[i] = (byte) -1;
            }
        }
        return result;
    }
}
