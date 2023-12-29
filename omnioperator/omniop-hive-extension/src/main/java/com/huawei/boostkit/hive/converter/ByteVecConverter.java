package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.io.ByteWritable;


public class ByteVecConverter extends ShortVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return (byte) dictionaryVec.getShort(index);
        }
        ShortVec ShortVec = (ShortVec) vec;
        return ShortVec.get(index);
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        byte byteValue;
        if (col instanceof LazyByte) {
            LazyByte lazyByte = (LazyByte) col;
            byteValue = lazyByte.getWritableObject().get();
        } else if (col instanceof ByteWritable) {
            byteValue = ((ByteWritable) col).get();
        } else {
            byteValue = (byte) col;
        }
        return byteValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        ShortVec tinyVec = new ShortVec(columnSize);
        short[] values = new short[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                tinyVec.setNull(i);
                continue;
            }
            values[i] = (short) col[i];
        }
        tinyVec.put(values, 0, 0, columnSize);
        return tinyVec;
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
                longColumnVector.vector[i - start] = (byte) value;
            }
        }
        return longColumnVector;
    }
}
