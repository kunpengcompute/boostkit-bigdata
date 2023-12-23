package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class StringVecConverter extends VarcharVecConverter {
    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        byte[] bytes = getBytes(vec, index);
        LazyString lazyString = new LazyString((LazyStringObjectInspector) primitiveObjectInspector);
        ByteArrayRef byteArrayRef = new ByteArrayRef();
        byteArrayRef.setData(bytes);
        lazyString.init(byteArrayRef, 0, bytes.length);
        return lazyString;
    }
}
