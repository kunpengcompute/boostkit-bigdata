package com.huawei.boostkit.hive.converter;

import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class DictionaryVecConverter implements VecConverter {
    @Override
    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        DictionaryVec dictionaryVec = (DictionaryVec) vec;
        PrimitiveTypeInfo primitiveTypeInfo = primitiveObjectInspector.getTypeInfo();
        switch (primitiveTypeInfo.getPrimitiveCategory()) {
            case BYTE:
                return (byte) dictionaryVec.getShort(index);
            case SHORT:
                return dictionaryVec.getShort(index);
            case INT:
                return dictionaryVec.getInt(index);
            case LONG:
                return dictionaryVec.getLong(index);
            case BOOLEAN:
                return dictionaryVec.getBoolean(index);
            case FLOAT:
            case DOUBLE:
                return dictionaryVec.getDouble(index);
            case STRING: {
                byte[] bytes = dictionaryVec.getBytes(index);
                LazyString lazyString = new LazyString((LazyStringObjectInspector) primitiveObjectInspector);
                ByteArrayRef byteArrayRef = new ByteArrayRef();
                byteArrayRef.setData(bytes);
                lazyString.init(byteArrayRef, 0, bytes.length);
                return lazyString;
            }
            case VARCHAR: {
                byte[] bytes = dictionaryVec.getBytes(index);
                LazyHiveVarchar lazyHiveVarchar = new LazyHiveVarchar((LazyHiveVarcharObjectInspector) primitiveObjectInspector);
                ByteArrayRef byteArrayRefVarchar = new ByteArrayRef();
                byteArrayRefVarchar.setData(bytes);
                lazyHiveVarchar.init(byteArrayRefVarchar, 0, bytes.length);
                return lazyHiveVarchar;
            }
            case CHAR: {
                byte[] bytes = dictionaryVec.getBytes(index);
                LazyHiveChar lazyHiveChar = new LazyHiveChar((LazyHiveCharObjectInspector) primitiveObjectInspector);
                ByteArrayRef byteArrayRefChar = new ByteArrayRef();
                byteArrayRefChar.setData(bytes);
                lazyHiveChar.init(byteArrayRefChar, 0, bytes.length);
                return lazyHiveChar;
            }
            case TIMESTAMP:
                return Timestamp.ofEpochMilli(dictionaryVec.getLong(index));
            case DATE:
                return Date.ofEpochMilli(dictionaryVec.getLong(index));
            case DECIMAL: {
                DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
                return HiveDecimal.create(Decimal128Vec.getDecimal(dictionaryVec.getDecimal128(index)), decimalTypeInfo.getScale());
            }
            default:
                return null;
        }
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        throw new RuntimeException(String.format("%s doesn't support toOmniVecV2", this.getClass().getSimpleName()));
    }
}
