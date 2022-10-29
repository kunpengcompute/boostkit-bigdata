/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.ock.spark.serialize;

import com.huawei.ock.spark.jni.OckShuffleJniReader;

import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.vector.BooleanVec;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.ShortVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;

import org.apache.spark.sql.execution.vectorized.OmniColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.rmi.UnexpectedException;

/**
 * Ock Shuffle DataSerializer
 *
 * @since 2022-6-10
 */
public class OckShuffleDataSerializer {
    private boolean isFinish = false;
    private final OckShuffleJniReader jniReader;
    private final nova.hetu.omniruntime.type.DataType[] vectorTypes;
    private final int maxLength;
    private final int maxRowNum;

    OckShuffleDataSerializer(OckShuffleJniReader reader,
                            nova.hetu.omniruntime.type.DataType[] vectorTypes,
                            int maxLength,
                            int maxRowNum) {
        this.jniReader = reader;
        this.vectorTypes = vectorTypes;
        this.maxLength = maxLength;
        this.maxRowNum = maxRowNum;
    }

    // must call this function before deserialize
    public boolean isFinish() {
        return isFinish;
    }

    /**
     * deserialize
     *
     * @return ColumnarBatch
     * @throws UnexpectedException UnexpectedException
     */
    public ColumnarBatch deserialize() throws UnexpectedException {
        jniReader.getNewVectorBatch(maxLength, maxRowNum);
        int rowCount = jniReader.rowCntInVB();
        int vecCount = jniReader.colCntInVB();
        ColumnVector[] vectors = new ColumnVector[vecCount];
        for (int index = 0; index < vecCount; index++) { // mutli value
            vectors[index] = buildVec(vectorTypes[index], rowCount, index);
        }

        isFinish = jniReader.readFinish();
        return new ColumnarBatch(vectors, rowCount);
    }

    private ColumnVector buildVec(nova.hetu.omniruntime.type.DataType srcType, int rowNum, int colIndex) {
        Vec dstVec;
        switch (srcType.getId()) {
            case OMNI_INT:
            case OMNI_DATE32:
                dstVec = new IntVec(rowNum);
                break;
            case OMNI_LONG:
            case OMNI_DATE64:
            case OMNI_DECIMAL64:
                dstVec = new LongVec(rowNum);
                break;
            case OMNI_SHORT:
                dstVec = new ShortVec(rowNum);
                break;
            case OMNI_BOOLEAN:
                dstVec = new BooleanVec(rowNum);
                break;
            case OMNI_DOUBLE:
                dstVec = new DoubleVec(rowNum);
                break;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                // values buffer length
                dstVec = new VarcharVec(jniReader.getVectorValueLength(colIndex), rowNum);
                break;
            case OMNI_DECIMAL128:
                dstVec = new Decimal128Vec(rowNum);
                break;
            case OMNI_TIME32:
            case OMNI_TIME64:
            case OMNI_INTERVAL_DAY_TIME:
            case OMNI_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + srcType.getId());
        }

        jniReader.copyVectorDataInVB(dstVec, colIndex);
        OmniColumnVector vecTmp = new OmniColumnVector(rowNum, getRealType(srcType), false);
        vecTmp.setVec(dstVec);
        return vecTmp;
    }

    private DataType getRealType(nova.hetu.omniruntime.type.DataType srcType) {
        switch (srcType.getId()) {
            case OMNI_INT:
                return DataTypes.IntegerType;
            case OMNI_DATE32:
                return DataTypes.DateType;
            case OMNI_LONG:
                return DataTypes.LongType;
            case OMNI_DATE64:
                return DataTypes.DateType;
            case OMNI_DECIMAL64:
                // for example 123.45=> precision(data length) = 5 ，scale(decimal length) = 2
                if (srcType instanceof Decimal64DataType) {
                    return DataTypes.createDecimalType(((Decimal64DataType) srcType).getPrecision(),
                            ((Decimal64DataType) srcType).getScale());
                } else {
                    throw new IllegalStateException("Unexpected value: " + srcType.getId());
                }
            case OMNI_SHORT:
                return DataTypes.ShortType;
            case OMNI_BOOLEAN:
                return DataTypes.BooleanType;
            case OMNI_DOUBLE:
                return DataTypes.DoubleType;
            case OMNI_CHAR:
            case OMNI_VARCHAR:
                return DataTypes.StringType;
            case OMNI_DECIMAL128:
                if (srcType instanceof Decimal128DataType) {
                    return DataTypes.createDecimalType(((Decimal128DataType) srcType).getPrecision(),
                            ((Decimal128DataType) srcType).getScale());
                } else {
                    throw new IllegalStateException("Unexpected value: " + srcType.getId());
                }
            case OMNI_TIME32:
            case OMNI_TIME64:
            case OMNI_INTERVAL_DAY_TIME:
            case OMNI_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + srcType.getId());
        }
    }
}