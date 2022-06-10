/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

package com.huawei.boostkit.spark.serialize;


import com.google.protobuf.InvalidProtocolBufferException;
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


public class ShuffleDataSerializer {

    public static ColumnarBatch deserialize(byte[] bytes) {
        try {
            VecData.VecBatch vecBatch = VecData.VecBatch.parseFrom(bytes);
            int vecCount = vecBatch.getVecCnt();
            int rowCount = vecBatch.getRowCnt();
            ColumnVector[] vecs = new ColumnVector[vecCount];
            for (int i = 0; i < vecCount; i++) {
                vecs[i] = buildVec(vecBatch.getVecs(i), rowCount);
            }
            return new ColumnarBatch(vecs, rowCount);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("deserialize failed. errmsg:" + e.getMessage());
        }
    }

    private static ColumnVector buildVec(VecData.Vec protoVec, int vecSize) {
        VecData.VecType protoTypeId = protoVec.getVecType();
        Vec vec;
        DataType type;
        switch (protoTypeId.getTypeId()) {
            case VEC_TYPE_INT:
                type = DataTypes.IntegerType;
                vec = new IntVec(vecSize);
                break;
            case VEC_TYPE_DATE32:
                type = DataTypes.DateType;
                vec = new IntVec(vecSize);
                break;
            case VEC_TYPE_LONG:
                type = DataTypes.LongType;
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_DATE64:
                type = DataTypes.DateType;
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_DECIMAL64:
                type = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                vec = new LongVec(vecSize);
                break;
            case VEC_TYPE_SHORT:
                type = DataTypes.ShortType;
                vec = new ShortVec(vecSize);
                break;
            case VEC_TYPE_BOOLEAN:
                type = DataTypes.BooleanType;
                vec = new BooleanVec(vecSize);
                break;
            case VEC_TYPE_DOUBLE:
                type = DataTypes.DoubleType;
                vec = new DoubleVec(vecSize);
                break;
            case VEC_TYPE_VARCHAR:
            case VEC_TYPE_CHAR:
                type = DataTypes.StringType;
                vec = new VarcharVec(protoVec.getValues().size(), vecSize);
                if (vec instanceof VarcharVec) {
                    ((VarcharVec) vec).setOffsetsBuf(protoVec.getOffset().toByteArray());
                }
                break;
            case VEC_TYPE_DECIMAL128:
                type = DataTypes.createDecimalType(protoTypeId.getPrecision(), protoTypeId.getScale());
                vec = new Decimal128Vec(vecSize);
                break;
            case VEC_TYPE_TIME32:
            case VEC_TYPE_TIME64:
            case VEC_TYPE_INTERVAL_DAY_TIME:
            case VEC_TYPE_INTERVAL_MONTHS:
            default:
                throw new IllegalStateException("Unexpected value: " + protoTypeId.getTypeId());
        }
        vec.setValuesBuf(protoVec.getValues().toByteArray());
        vec.setNullsBuf(protoVec.getNulls().toByteArray());
        OmniColumnVector vecTmp = new OmniColumnVector(vecSize, type, false);
        vecTmp.setVec(vec);
        return vecTmp;
    }
}
