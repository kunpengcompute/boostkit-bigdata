
package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.BytesColumnCache;
import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.VarcharCache;

import nova.hetu.omniruntime.vector.DictionaryVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.commons.lang3.reflect.TypeUtils;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveBaseCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveVarchar;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.nio.charset.StandardCharsets;

public class VarcharVecConverter implements VecConverter {
    @Override
    public Vec toOmniVec(Object[] col, int columnSize) {
        int totalSize = 0;
        int[] offsets = new int[columnSize + 1];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                VarcharCache vc = (VarcharCache) col[i];
                totalSize += vc.offset;
            }
            offsets[i + 1] = totalSize;
        }
        VarcharVec varcharVec = new VarcharVec(totalSize, columnSize);
        byte[] varcharValues = new byte[totalSize];
        for (int i = 0; i < columnSize; i++) {
            if (col[i] == null) {
                VarcharCache vc = (VarcharCache) col[i];
                System.arraycopy(vc.values, 0, varcharValues, offsets[i], offsets[i + 1] - offsets[i]);
            } else {
                varcharVec.setNull(i);
            }
        }
        varcharVec.put(0, varcharValues, 0, offsets, 0, columnSize);
        return varcharVec;
    }

    @Override
    public Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        VarcharVec VarcharVec = new VarcharVec(columnSize);
        BytesColumnCache bytesColumnCache = (BytesColumnCache) columnCache;
        int totalSize = 0;
        int[] offsets = new int[columnSize + 1];
        for (int i = 0; i < columnSize; i++) {
            if (!bytesColumnCache.isNull[i]) {
                VarcharCache vc = bytesColumnCache.dataCache[i];
                totalSize += vc.offset;
            }
            offsets[i + 1] = totalSize;
        }
        VarcharVec varcharVec = new VarcharVec(totalSize, columnSize);
        byte[] varcharValues = new byte[totalSize];
        if (bytesColumnCache.noNulls) {
            for (int i = 0; i < columnSize; i++) {
                if (!bytesColumnCache.isNull[i]) {
                    VarcharCache vc = bytesColumnCache.dataCache[i];
                    System.arraycopy(vc.values, 0, varcharValues, offsets[i], offsets[i + 1] - offsets[i]);
                }
            }
        } else {
            for (int i = 0; i < columnSize; i++) {
                if (bytesColumnCache.isNull[i]) {
                    VarcharVec.setNull(i);
                } else {
                    VarcharCache vc = bytesColumnCache.dataCache[i];
                    System.arraycopy(vc.values, 0, varcharValues, offsets[i], offsets[i + 1] - offsets[i]);
                }
            }
        }
        varcharVec.put(0, varcharValues, 0, offsets, 0, columnSize);
        return VarcharVec;
    }

    @Override
    public Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        if (vec.isNull(index)) {
            return null;
        }
        byte[] bytes = getBytes(vec, index);
        LazyHiveVarchar lazyHiveVarchar = new LazyHiveVarchar((LazyHiveVarcharObjectInspector) primitiveObjectInspector);
        ByteArrayRef byteArrayRef = new ByteArrayRef();
        byteArrayRef.setData(bytes);
        lazyHiveVarchar.init(byteArrayRef, 0, bytes.length);
        return lazyHiveVarchar;
    }

    @Override
    public Object calculateValue(Object col, PrimitiveTypeInfo primitiveTypeInfo) {
        if (col == null) {
            return null;
        }
        int length = TypeUtils.DEFAULT_VARCHAR_LENGTH;
        if (primitiveTypeInfo instanceof BaseCharTypeInfo) {
            length = ((BaseCharTypeInfo) primitiveTypeInfo).getLength();
        }
        VarcharCache vc = new VarcharCache();
        byte[] value = getByteFromLazyPrimitive(col, length);
        if (value == null) {
            return null;
        }
        int offset = value.length;
        vc.offset = offset;
        vc.values = value;
        return vc;
    }

    protected byte[] getBytes(Vec vec, int index) {
        if (vec instanceof DictionaryVec) {
            DictionaryVec dictionaryVec = (DictionaryVec) vec;
            return dictionaryVec.getBytes(index);
        }
        VarcharVec stringVec = (VarcharVec) vec;
        return stringVec.get(index);
    }

    protected byte[] getByteFromLazyPrimitive(Object col, int length) {
        byte[] resultBytes = null;
        if (col instanceof LazyPrimitive) {
            LazyPrimitive lazyPrimitive = (LazyPrimitive) col;
            Writable writableObject = lazyPrimitive.getWritableObject();
            if (writableObject instanceof HiveBaseCharWritable) {
                resultBytes = ((HiveBaseCharWritable) writableObject).getTextValue().copyBytes();
            }
            if (writableObject instanceof Text) {
                resultBytes = ((Text) writableObject).copyBytes();
            }
            throw new RuntimeException(String.format("doesn't support wriablrObject: %s",
                    writableObject.getClass().getSimpleName()));
        }
        if (col instanceof HiveVarchar) {
            resultBytes = ((HiveVarchar) col).getValue().getBytes(StandardCharsets.UTF_8);
        }
        if (col instanceof HiveVarcharWritable) {
            HiveBaseCharWritable hiveBaseCharWritable = (HiveBaseCharWritable) col;
            resultBytes = hiveBaseCharWritable.getTextValue().copyBytes();
        }
        if (col instanceof Text) {
            resultBytes = ((Text) col).copyBytes();
        }
        return resultBytes;
    }

    @Override
    public void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                         ColumnCache columnCache, int colIndex, int rowCount) {
        BytesColumnVector columnVector = (BytesColumnVector) vectorizedRowBatch.cols[vectorColIndex];
        BytesColumnCache bytesColumnCache = (BytesColumnCache) columnCache;
        if (!columnVector.noNulls) {
            bytesColumnCache.noNulls = false;
        }
        if (columnVector.isRepeating) {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                setVectorCache(0, bytesColumnCache, rowCount + i, columnVector);
            }
        } else if (vectorizedRowBatch.selectedInUse) {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                setVectorCache(vectorizedRowBatch.selected[i], bytesColumnCache, rowCount + i, columnVector);
            }
        } else {
            for (int i = 0; i < vectorizedRowBatch.size; i++) {
                setVectorCache(i, bytesColumnCache, rowCount + i, columnVector);
            }
        }
    }

    private void setVectorCache(int rowIndex, BytesColumnCache bytesColumnCache, int rowCount, BytesColumnVector columnVector) {
        if (columnVector.isNull[rowIndex]) {
            bytesColumnCache.isNull[rowCount] = true;
            return;
        }
        VarcharCache vc = new VarcharCache();
        vc.offset = columnVector.length[rowIndex];
        byte[] result = new byte[columnVector.length[rowIndex]];
        System.arraycopy(columnVector.vector[rowIndex], columnVector.start[rowIndex], result, 0, columnVector.length[rowIndex]);
        vc.values = result;
        bytesColumnCache.dataCache[rowCount] = vc;
    }

    @Override
    public ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        BytesColumnVector bytesColumnVector = new BytesColumnVector();
        for (int i = start; i < end; i++) {
            if (vec.isNull(i)) {
                bytesColumnVector.vector[i - start] = null;
                bytesColumnVector.isNull[i - start] = true;
                bytesColumnVector.noNulls = false;
            } else {
                byte[] value = getBytes(vec, i);
                bytesColumnVector.setRef(i - start, value, 0, value.length);
            }
        }
        return bytesColumnVector;
    }
}
