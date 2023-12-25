package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
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
        ShortVec shortVec = (ShortVec) vec;
        return shortVec.get(index);
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