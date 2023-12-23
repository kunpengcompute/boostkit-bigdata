package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.lazy.LazyBoolean;
import org.apache.hadoop.io.BooleanWritable;


public class BooleanVecConverter extends LongVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getLong(index);
        }
        BooleanVec booleanVec = (BooleanVec) vec;
        return booleanVec.get(index);
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        boolean booleanValue;
        if (col instanceof LazyBoolean) {
            LazyBoolean lazyBoolean = (LazyBoolean) col;
            booleanValue = lazyBoolean.getWritableObject().get();
        } else if (col instanceof BooleanWritable) {
            booleanValue = ((BooleanWritable) col).get();
        } else {
            booleanValue = (boolean) col;
        }
        return booleanValue;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        BooleanVec booleanVec = new BooleanVec(columnSize);
        boolean[] booleanValues = new boolean[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                booleanVec.setNull(i);
                continue;
            }
            booleanValues[i] = (boolean) col[i];
        }
        booleanVec.put(booleanValues, 0, 0, columnSize);
        return booleanVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        BooleanVec booleanVec = new BooleanVec(columnSize);
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        if (longColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                booleanVec.set(i, longColumnCache.dataCache[i] ==1L);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (longColumnCache.isNull[i]) {
                    booleanVec.setNull(i);
                } else {
                    booleanVec.set(i, longColumnCache.dataCache[i]==1L);
                }
            }
        }
        return booleanVec;
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        LongColumnVector longColumnVector = new LongColumnVector();
        for (int i = start; i < end; i++) {
            Object value=fromOmniVec(vec,i);
            if (value==null) {
                longColumnVector.vector[i - start] = 1L;
                longColumnVector.isNull[i - start] = true;
                longColumnVector.noNulls = false;
            }else{
                longColumnVector.vector[i-start]=((boolean) value) ? 1L:0L;
            }
        }
        return longColumnVector;
    }
}
