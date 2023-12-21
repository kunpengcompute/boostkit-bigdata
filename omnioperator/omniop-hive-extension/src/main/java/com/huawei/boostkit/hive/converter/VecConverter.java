package com.huawei.boostkit.hive.converter;

import com.huawei.boostkit.hive.cache.ColumnCache;
import com.huawei.boostkit.hive.cache.VectorCache;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.util.HashMap;
import java.util.Map;

public interface VecConverter {
    Map<PrimitiveObjectInspector.PrimitiveCategory, VecConverter> CONVERTER_MAP =
            new HashMap<PrimitiveObjectInspector.PrimitiveCategory, VecConverter>() {
                {
                    put(PrimitiveObjectInspector.PrimitiveCategory.BYTE, new ByteVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.SHORT, new ShortVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.INT, new IntVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.LONG, new LongVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN, new BooleanVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, new DoubleVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, new DoubleVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.STRING, new StringVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.CHAR, new CharVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.VARCHAR, new VarcharVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP, new TimestampVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.DATE, new DateVecConverter());
                    put(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, new DecimalVecConverter());
                }
            };

    /**
     * convert vec to java type data
     *
     * @param vec   omni vec
     * @param index the row index
     * @return row data
     */
    default Object fromOmniVec(Vec vec, int index) {
        throw new RuntimeException(String.format("%s doesn't support fromOmniVec(Vec vec)",
                this.getClass().getSimpleName()));
    }

    /**
     * convert vec to java type data
     *
     * @param vec                      omni vec
     * @param index                    the row index
     * @param primitiveObjectInspector primitiveObjectInspector
     * @return row data
     */
    default Object fromOmniVec(Vec vec, int index, PrimitiveObjectInspector primitiveObjectInspector) {
        return fromOmniVec(vec, index);
    }

    /**
     * convert data from hive type to java type
     *
     * @param col hive data
     * @return converted data
     */
    default Object calculateValue(Object col) {
        throw new RuntimeException(String.format("%s doesn't support calculate(Object col)",
                this.getClass().getSimpleName()));
    }

    /**
     * convert data from hive type to java type
     *
     * @param col               hive data
     * @param primitiveTypeInfo primitiveTypeInfo
     * @return converted data
     */
    default Object calculateValue(Object col, PrimitiveTypeInfo primitiveTypeInfo) {
        return calculateValue(col);
    }

    /**
     * convert data to vec
     *
     * @param col        data
     * @param columnSize columnSize
     * @return omni vec
     */
    default Vec toOmniVec(Object[] col, int columnSize) {
        throw new RuntimeException(String.format("%s doesn't support toOmniVec(Object[] col, int columnSize)",
                this.getClass().getSimpleName()));
    }

    default Vec toOmniVec(Object[] col, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        return toOmniVec(col, columnSize);
    }

    default Vec toOmniVec(ColumnCache columnCache, int columnSize, PrimitiveTypeInfo primitiveTypeInfo) {
        return toOmniVec(columnCache, columnSize);
    }

    default Vec toOmniVec(ColumnCache columnCache, int columnSize) {
        throw new RuntimeException(String.format("%s doesn't support toOmniVec(ColumnCache columnCache,int columnSize)",
                this.getClass().getSimpleName()));
    }

    default void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                          VectorCache vectorCache, int colIndex, int rowCount) {
        throw new RuntimeException(String.format("%s doesn't support setValueFromColumnVector(VectorizedRowBatch " +
                        "vectorizedRowBatch, int vectorColIndex, VectorCache vectorCache,int colIndex,int rowCount)",
                this.getClass().getSimpleName()));
    }

    default void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                          VectorCache vectorCache, int colIndex, int rowCount,
                                          PrimitiveTypeInfo primitiveTypeInfo) {
        setValueFromColumnVector(vectorizedRowBatch, vectorColIndex, vectorCache, colIndex, rowCount);
    }

    default void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                          ColumnCache columnCache, int colIndex, int rowCount) {
    }

    default void setValueFromColumnVector(VectorizedRowBatch vectorizedRowBatch, int vectorColIndex,
                                          ColumnCache columnCache, int colIndex, int rowCount,
                                          PrimitiveTypeInfo primitiveTypeInfo) {
        setValueFromColumnVector(vectorizedRowBatch, vectorColIndex, columnCache, colIndex, rowCount);
    }

    default ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end) {
        throw new RuntimeException(String.format("%s doesn't support getColumnVectorFromOmniVec(Vec vec, int start, " +
                "int end)", this.getClass().getSimpleName()));
    }

    default ColumnVector getColumnVectorFromOmniVec(Vec vec, int start, int end,
                                                    PrimitiveObjectInspector primitiveObjectInspector) {
        return getColumnVectorFromOmniVec(vec, start, end);
    }
}
