package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.IntVec;
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
        IntVec intVec = (IntVec) vec;
        return intVec.get(index);
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
            } else {
                IntVec intVec = (IntVec) vec;
                value = intVec.get(i);
            }
            longColumnVector.vector[i - start] = value;
        }
        return longColumnVector;
    }
}
