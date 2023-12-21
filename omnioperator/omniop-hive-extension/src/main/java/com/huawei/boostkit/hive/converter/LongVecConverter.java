package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.LongWritable;


public class LongVecConverter implements VecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getLong(index);
        }
        LongVec longVec = (LongVec) vec;
        return longVec.get(index);
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        long longValue;
        if (col instanceof LazyLong) {
            LazyLong lazyLong = (LazyLong) col;
            longValue = lazyLong.getWritableObject().get();
        } else if (col instanceof LongWritable) {
            longValue = ((LongWritable) col).get();
        } else {
            longValue = (long) col;
        }
        return longValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        LongVec longVec = new LongVec(columnSize);
        long[] longValues = new long[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                longVec.setNull(i);
                continue;
            }
            longValues[i] = (long) col[i];
        }
        longVec.put(longValues, 0, 0, columnSize);
        return longVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
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
        LongColumnVector columnVector = (LongColumnVector) vectorizedRowBatch.cols[vectorColIndex];
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        long[] vector = columnVector.vector;
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
                value = dictionaryVec.getLong(i);
            } else {
                LongVec longVec = (LongVec) vec;
                value = longVec.get(i);
            }
            longColumnVector.vector[i - start] = value;
        }
        return longColumnVector;
    }
}
