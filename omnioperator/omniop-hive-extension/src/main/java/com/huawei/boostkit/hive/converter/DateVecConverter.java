package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.lazy.LazyDate;

public class DateVecConverter extends IntVecConverter {
    public Object fromOmniVec(Vec vec, int index) {
        if (vec.isNull(index)) {
            return null;
        }
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return Date.ofEpochDay(dictionaryVec.getInt(index));
        }
        IntVec timeVec = (IntVec) vec;
        return Date.ofEpochDay(timeVec.get(index));
    }

    @Override
    public Object calculateValue(Object col) {
        if (col == null) {
            return null;
        }
        int day;
        if (col instanceof LazyDate) {
            LazyDate lazyDate = (LazyDate) col;
            day = lazyDate.getWritableObject().get().toEpochDay();
        } else if (col instanceof DateWritable) {
            day = ((DateWritable) col).get().getDay();
        } else if(col instanceof DateWritableV2){
            day = ((DateWritableV2) col).get().toEpochDay();
        }else {
            day = ((Date) col).toEpochDay();
        }
        return day;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        IntVec dateVec = new IntVec(columnSize);
        int[] intValues = new int[columnSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                dateVec.setNull(i);
                continue;
            }
            intValues[i] = (int) col[i];
        }
        dateVec.put(intValues, 0, 0, columnSize);
        return dateVec;
    }
}