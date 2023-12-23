
package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveBaseCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.nio.charset.StandardCharsets;


public class CharVecConverter extends VarcharVecConverter {
    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        byte[] bytes = getBytes(vec, index);
        LazyHiveChar lazyHiveChar = new LazyHiveChar((LazyHiveCharObjectInspector) primitiveObjectInspector);
        ByteArrayRef byteArrayRef = new ByteArrayRef();
        byteArrayRef.setData(bytes);
        lazyHiveChar.init(byteArrayRef, 0, bytes.length);
        return lazyHiveChar;
    }

    protected byte[] getByteFromLazyPrimitive(Object col, int length) {
        byte[] resultBytes = null;
        if (col instanceof LazyPrimitive) {
            LazyPrimitive lazyPrimitive = (LazyPrimitive) col;
            Writable writableObject = lazyPrimitive.getWritableObject();
            if (writableObject instanceof HiveBaseCharWritable) {
                resultBytes = trimBytes(((HiveBaseCharWritable) writableObject).getTextValue().getBytes());
            }
            if (writableObject instanceof Text) {
                resultBytes = trimBytes(((Text) writableObject).getBytes());
            }
            throw new RuntimeException(String.format("doesn't support wriablrObject: %s",
                    writableObject.getClass().getSimpleName()));
        }
        if (col instanceof HiveVarchar) {
            resultBytes = ((HiveVarchar) col).getValue().getBytes(StandardCharsets.UTF_8);
        }
        if (col instanceof HiveVarcharWritable) {
            HiveBaseCharWritable hiveBaseCharWritable = (HiveBaseCharWritable) col;
            resultBytes = trimBytes(hiveBaseCharWritable.getTextValue().getBytes());
        }
        if (col instanceof Text) {
            resultBytes = trimBytes(((Text) col).getBytes());
        }
        return resultBytes;
    }

    private byte[] trimBytes(byte[] bytes) {
        int index = -1;
        for (int i = bytes.length - 1; i >= 0; i--) {
            // 0x20 means space
            if (bytes[i] != 0x20 && bytes[i] != 0) {
                index = i + 1;
                break;
            }
        }
        byte[] trimBytes;
        if (index != -1) {
            trimBytes = new byte[index];
            System.arraycopy(bytes, 0, trimBytes, 0, index);
        } else {
            trimBytes = new byte[bytes.length];
            System.arraycopy(bytes, 0, trimBytes, 0, bytes.length);
        }
        return trimBytes;
    }
}