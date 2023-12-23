package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.LongColumnCache;

import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.DataTypePhysicalVariation;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.Decimal64ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLastValue;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


public class Decimal64VecConverter extends LongVecConverter {
    static final Logger LOG = LoggerFactory.getLogger(GenericUDAFLastValue.class.getName());

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end,
                                                   PrimitiveObjectInspector primitiveObjectInspector) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        Decimal64ColumnVector decimal64ColumnVector = new Decimal64ColumnVector(decimalTypeInfo.getPrecision(),
                decimalTypeInfo.getScale());
        for (int i = start; i < end; i++) {
            if (vec.isNull(i)) {
                decimal64ColumnVector.vector[i - start] = 1L;
                decimal64ColumnVector.isNull[i - start] = true;
                decimal64ColumnVector.noNulls = false;
                continue;
            }
            long value;
            if (vec instanceof DictionaryVec) {
                DictionaryVec dictionaryVec = (DictionaryVec) vec;
                value = dictionaryVec.getDecimal128(i)[0];
            } else {
                Decimal128Vec decimal128Vec = (Decimal128Vec) vec;
                Map.Entry<Boolean, byte[]> result = decimal128Vec.getBytes(i);
                if (result.getValue().length > 0) {
                    byte[] newBytes = new byte[8];
                    System.arraycopy(result.getValue(), result.getValue().length - 8, newBytes, 0, 8);
                    value = Decimal128Vec.bytesToLong(newBytes) * (result.getKey() ? -1 : 1);
                } else {
                    value = Decimal128Vec.bytesToLong(result.getValue()) * (result.getKey() ? -1 : 1);
                }
            }
            decimal64ColumnVector.vector[i - start] = value;
        }
        return decimal64ColumnVector;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                decimal128Vec.setNull(i);
            } else {
                long value = (long) col[i];
                decimal128Vec.setBigInteger(i, Decimal128Vec.longToBytes(Math.abs(value)), value < 0L);
            }
        }
        return decimal128Vec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        LongColumnCache longColumnCache = (LongColumnCache) columnCache;
        if (longColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                long value = longColumnCache.dataCache[i];
                decimal128Vec.setBigInteger(i, Decimal128Vec.longToBytes(Math.abs(value)), value < 0L);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (longColumnCache.isNull[i]) {
                    decimal128Vec.setNull(i);
                } else {
                    long value = longColumnCache.dataCache[i];
                    decimal128Vec.setBigInteger(i, Decimal128Vec.longToBytes(Math.abs(value)), value < 0L);
                }
            }
        }
        return decimal128Vec;
    }

    public static boolean isConvertedDecimal64(String fieldName, VectorizationContext vectorizationContext) {
        boolean convertedDecimal64 = false;
        try {
            if (vectorizationContext != null) {
                convertedDecimal64 = vectorizationContext.getDataTypePhysicalVariation(
                        vectorizationContext.getInputColumnIndex(fieldName)) == DataTypePhysicalVariation.DECIMAL_64;
            }
        } catch (HiveException e) {
            LOG.error("error occurs when finding field from vectorizationContext");
        }
        return convertedDecimal64;
    }
}