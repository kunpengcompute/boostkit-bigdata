package com.huawei.boostkit.hive.converter;

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

import static org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch.DEFAULT_SIZE;

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
            timeValue = ((Timestamp) col).toEpochMilli()
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
        return timestampVec
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
