package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.DecimalColumnCache;

import nova.hetu.omniruntime.utils.OmniRuntimeException;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.Map;

import static nova.hetu.omniruntime.utils.OmniErrorType.OMNI_INNER_ERROR;
import static nova.hetu.omniruntime.vector.Decimal128Vec.bytesToLong;
import static nova.hetu.omniruntime.vector.Decimal128Vec.longToBytes;
import static org.apache.hadoop.hive.common.type.HiveDecimal.SCRATCH_LONGS_LEN;


public class DecimalVecConverter implements VecConverter {
    private long[] scratchLongs = new long[SCRATCH_LONGS_LEN];

    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return getDecimalWritableFromLong(dictionaryVec.getDecimal128(index), decimalTypeInfo.getScale());
        }
        Decimal128Vec decimal128Vec = (Decimal128Vec) vec;
        Map.Entry<Boolean, byte[]> result = decimal128Vec.getBytes(index);
        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(result.getValue(), decimalTypeInfo.getScale());
        if (result.getKey()) {
            hiveDecimalWritable.mutateNegate();
        }
        return hiveDecimalWritable;
    }

    private Object getDecimalWritableFromLong(long[] longs, int scale) {
        boolean isNegative = longs[1] < 0L;
        if (isNegative) {
            longs[1] &= 9223372036854775807L;
        }

        byte[] bytes = new byte[16];
        byte[] highBytes = longToBytes(longs[1]);
        byte[] lowBytes = longToBytes(longs[0]);
        System.arraycopy(highBytes, 0, bytes, 0, 8);
        System.arraycopy(lowBytes, 0, bytes, 8, 8);
        HiveDecimalWritable hiveDecimalWritable = new HiveDecimalWritable(bytes, scale);
        if (isNegative) {
            hiveDecimalWritable.mutateNegate();
        }
        return hiveDecimalWritable;
    }

    @Override
    public Object calculateValue(Object col, PrimitiveTypeInfo primitiveTypeInfo) {
        if (col == null) {
            return null;
        }
        HiveDecimal hiveDecimal;
        if (col instanceof LazyHiveDecimal) {
            LazyHiveDecimal lazyHiveDecimal = (LazyHiveDecimal) col;
            hiveDecimal = lazyHiveDecimal.getWritableObject().getHiveDecimal();
        } else if (col instanceof HiveDecimalWritable) {
            hiveDecimal = ((HiveDecimalWritable) col).getHiveDecimal();
        } else {
            hiveDecimal = (HiveDecimal) col;
        }
        return hiveDecimal;
    }

    @Override
    public Vec toOmniVec(Object[] col, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                decimal128Vec.setNull(i);
            } else {
                HiveDecimal hiveDecimal = (HiveDecimal) col[i];
                decimal128Vec.setBigInteger(i, hiveDecimal.abs().bigIntegerBytesScaled(decimalTypeInfo.getScale()),
                        hiveDecimal.signum() == -1);
            }
        }
        return decimal128Vec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        Decimal128Vec decimal128Vec = new Decimal128Vec(columnSize);
        DecimalColumnCache decimal128ColumnCache = (DecimalColumnCache) columnCache;
        byte[] value = new byte[columnSize * 16];
        byte[] isNull = new byte[columnSize];
        if (decimal128ColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                System.arraycopy(decimal128ColumnCache.dataCache[i], 0, value, i * 16, 16);
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (decimal128ColumnCache.isNull[i]) {
                    isNull[i] = 1;
                } else {
                    System.arraycopy(decimal128ColumnCache.dataCache[i], 0, value, i * 16, 16);
                }
            }
        }
        decimal128Vec.setValuesBuf(value);
        decimal128Vec.setNullsBuf(isNull);
        return decimal128Vec;
    }

    @Override
    public void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                         ColumnCache columnCache, int colIndex, int rowCount,
                                         PrimitiveTypeInfo primitiveTypeInfo) {
        ColumnVector columnVector = vectorizedRowBatch.cols[vectorColIndex];
        DecimalColumnCache decimalColumnCache = (DecimalColumnCache) columnCache;
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveTypeInfo;
        HiveDecimalWritable[] vector = ((DecimalColumnVector) columnVector).vector;
        if (!columnVector.noNulls) {
            decimalColumnCache.noNulls = false;
        }
        if (columnVector.isRepeating) {
            if (columnVector.isNull[0]) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.isNull[rowCount + i] = true;
                }
            } else {
                byte[] decimalBytes = getDecimalBytes(vector[0], decimalTypeInfo);
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = decimalBytes;
                }
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[vectorizedRowBatch.selected[i]],
                            decimalTypeInfo);
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[vectorizedRowBatch.selected[i]]) {
                        decimalColumnCache.isNull[rowCount + i] = true;
                    } else {
                        decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(
                                vector[vectorizedRowBatch.selected[i]], decimalTypeInfo);
                    }
                }
            }
        } else {
            if (columnVector.noNulls) {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[i], decimalTypeInfo);
                }
            } else {
                for (int i = 0; i < vectorizedRowBatch.size; i++) {
                    if (columnVector.isNull[i]) {
                        decimalColumnCache.isNull[rowCount + i] = true;
                    } else {
                        decimalColumnCache.dataCache[rowCount + i] = getDecimalBytes(vector[i], decimalTypeInfo);
                    }
                }
            }
        }
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end,
                                                   PrimitiveObjectInspector primitiveObjectInspector) {
        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
        DecimalColumnVector decimalColumnVector = new DecimalColumnVector(decimalTypeInfo.getPrecision(),
                decimalTypeInfo.getScale());
        for (int i = start; i < end; i++) {
            Object value = fromOmniVec(vec, i, primitiveObjectInspector);
            if (value == null) {
                decimalColumnVector.isNull[i - start] = true;
                decimalColumnVector.noNulls = false;
            } else {
                decimalColumnVector.vector[i - start] = (HiveDecimalWritable) value;
            }
        }
        return decimalColumnVector;
    }

    private byte[] getDecimalBytes(HiveDecimalWritable hiveDecimal, DecimalTypeInfo decimalTypeInfo) {
        boolean isNegative = hiveDecimal.signum() == -1;
        byte[] buffer = new byte[16];
        hiveDecimal.mutateAbs();
        buffer = hiveDecimal.getHiveDecimal().bigIntegerBytesScaled(decimalTypeInfo.getScale());
        int byteArrayLength = buffer.length;
        if (byteArrayLength > 2 * Long.BYTES) {
            throw new OmniRuntimeException(OMNI_INNER_ERROR, "Decimal overflow.");
        }

        // the array is big endian
        byte[] highBytes = new byte[Long.BYTES];
        byte[] lowBytes = new byte[Long.BYTES];
        if (byteArrayLength <= Long.BYTES) {
            System.arraycopy(buffer, 0, lowBytes, Long.BYTES - byteArrayLength, Math.min(byteArrayLength, Long.BYTES));
        } else {
            int offset = byteArrayLength - Long.BYTES;
            System.arraycopy(buffer, 0, highBytes, 2 * Long.BYTES - byteArrayLength, Math.min(offset, Long.BYTES));
            System.arraycopy(buffer, offset, lowBytes, 0, Long.BYTES);
        }
        if (isNegative) {
            long signBit = bytesToLong(highBytes) | (1L << 63);
            highBytes = longToBytes(signBit);
        }
        byte[] result = new byte[16];
        for (int i = 0; i < 8; i++) {
            result[i] = lowBytes[7 - i];
        }
        for (int i = 8; i < 16; i++) {
            result[i] = highBytes[15 - i];
        }
        return result;
    }
}
