/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.boostkit.spark;

import java.io.File;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Date64DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

abstract class ColumnShuffleTest {
    public static String shuffleTestDir = "/tmp/shuffleTests";

    public DataType[] dataTypeId2DataType(DataType.DataTypeId[] idTypes) {
        DataType[] types = new DataType[idTypes.length];
        for(int i = 0; i < idTypes.length; i++) {
            switch (idTypes[i]) {
                case OMNI_INT: {
                    types[i] = IntDataType.INTEGER;
                    break;
                }
                case OMNI_LONG: {
                    types[i] = LongDataType.LONG;
                    break;
                }
                case OMNI_DOUBLE: {
                    types[i] = DoubleDataType.DOUBLE;
                    break;
                }
                case OMNI_VARCHAR: {
                    types[i] = VarcharDataType.VARCHAR;
                    break;
                }
                case OMNI_CHAR: {
                    types[i] = CharDataType.CHAR;
                    break;
                }
                case OMNI_DATE32: {
                    types[i] = Date32DataType.DATE32;
                    break;
                }
                case OMNI_DATE64: {
                    types[i] = Date64DataType.DATE64;
                    break;
                }
                case OMNI_DECIMAL64: {
                    types[i] = Decimal64DataType.DECIMAL64;
                    break;
                }
                case OMNI_DECIMAL128: {
                    types[i] = Decimal128DataType.DECIMAL128; // Or types[i] = new Decimal128DataType(2, 0);
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported type : " + idTypes[i]);
                }
            }
        }
        return types;
    }

    public VecBatch buildVecBatch(DataType.DataTypeId[] idTypes, int rowNum, int partitionNum, boolean mixHalfNull, boolean withPidVec) {
        List<Vec> columns = new ArrayList<>();
        Vec tmpVec = null;
        // prepare pidVec
        if (withPidVec) {
            IntVec pidVec = new IntVec(rowNum);
            for (int i = 0; i < rowNum; i++) {
                pidVec.set(i, i % partitionNum);
            }
            columns.add(pidVec);
        }

        for(int i = 0; i < idTypes.length; i++) {
            switch (idTypes[i]) {
                case OMNI_INT:
                case OMNI_DATE32:{
                    tmpVec = new IntVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((IntVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_LONG:
                case OMNI_DECIMAL64:
                case OMNI_DATE64: {
                    tmpVec = new LongVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((LongVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_DOUBLE: {
                    tmpVec = new DoubleVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((DoubleVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    tmpVec = new VarcharVec(rowNum * 16, rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((VarcharVec)tmpVec).set(j, ("VAR_" + (j + 1) + "_END").getBytes(StandardCharsets.UTF_8));
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_DECIMAL128: {
                    long[][] arr = new long[rowNum][2];
                    for (int j = 0; j < rowNum; j++) {
                        arr[j][0] = 2 * j;
                        arr[j][1] = 2 * j + 1;
                        if (mixHalfNull && (j % 2) == 0) {
                            arr[j] = null;
                        }
                    }
                    tmpVec = createDecimal128Vec(arr);
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported type : " + idTypes[i]);
                }
            }
            columns.add(tmpVec);
        }
        return new VecBatch(columns);
    }

    public Decimal128Vec createDecimal128Vec(long[][] data) {
        Decimal128Vec result = new Decimal128Vec(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                result.setNull(i);
            } else {
                result.set(i, new long[]{data[i][0], data[i][1]});
            }
        }
        return result;
    }

    public List<Vec> buildValInt(int pid, int val) {
        IntVec c0 = new IntVec(1);
        IntVec c1 = new IntVec(1);
        c0.set(0, pid);
        c1.set(0, val);
        List<Vec> columns = new ArrayList<>();
        columns.add(c0);
        columns.add(c1);
        return columns;
    }

    public List<Vec> buildValChar(int pid, String varChar) {
        IntVec c0 = new IntVec(1);
        VarcharVec c1 = new VarcharVec(8, 1);
        c0.set(0, pid);
        c1.set(0, varChar.getBytes(StandardCharsets.UTF_8));
        List<Vec> columns = new ArrayList<>();
        columns.add(c0);
        columns.add(c1);
        return columns;
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
}
