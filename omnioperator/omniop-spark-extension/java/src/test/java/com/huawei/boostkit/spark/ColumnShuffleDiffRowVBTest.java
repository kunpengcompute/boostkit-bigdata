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

import com.huawei.boostkit.spark.jni.SparkJniWrapper;

import java.io.File;
import nova.hetu.omniruntime.type.DataType;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_CHAR;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DATE32;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DATE64;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DECIMAL128;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DECIMAL64;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_DOUBLE;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_INT;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_LONG;
import static nova.hetu.omniruntime.type.DataType.DataTypeId.OMNI_VARCHAR;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import nova.hetu.omniruntime.vector.VecBatch;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

public class ColumnShuffleDiffRowVBTest extends ColumnShuffleTest {
    private static String shuffleDataFile = "";

    @BeforeClass
    public static void runOnceBeforeClass() {
        File folder = new File(shuffleTestDir);
        if (!folder.exists() && !folder.isDirectory()) {
            folder.mkdirs();
        }
    }

    @AfterClass
    public static void runOnceAfterClass() {
        File folder = new File(shuffleTestDir);
        if (folder.exists()) {
            deleteDir(folder);
        }
    }

    @Before
    public void runBeforeTestMethod() {

    }

    @After
    public void runAfterTestMethod() {
        File file = new File(shuffleDataFile);
        if (file.exists()) {
            file.delete();
        }
    }

    @Test
    public void columnShuffleMixColTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_MixCol_test";
        DataType.DataTypeId[] idTypes = {OMNI_LONG, OMNI_DOUBLE, OMNI_INT, OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 999; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 999, partitionNum, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleVarCharFirstTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_varCharFirst_test";
        DataType.DataTypeId[] idTypes = {OMNI_VARCHAR, OMNI_LONG, OMNI_DOUBLE, OMNI_INT, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                0,
                4096,
                1024*1024*1024);
        for (int i = 0; i < 999; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 999, partitionNum, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffle1Row1024VBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_1row1024vb_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 1024; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 1, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffle1024Row1VBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_1024row1vb_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 1; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 1024, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleChangeRowVBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_changeRow_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR, OMNI_CHAR};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int numPartition = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                numPartition,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 1; i < 1000; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, i, numPartition, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleVarChar1RowVBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_varChar1Row_test";
        DataType.DataTypeId[] idTypes = {OMNI_VARCHAR};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        VecBatch vecBatchTmp1 = new VecBatch(buildValChar(3, "N"));
        jniWrapper.split(splitterId, vecBatchTmp1.getNativeVectorBatch());
        VecBatch vecBatchTmp2 = new VecBatch(buildValChar(2, "F"));
        jniWrapper.split(splitterId, vecBatchTmp2.getNativeVectorBatch());
        VecBatch vecBatchTmp3 = new VecBatch(buildValChar(3, "N"));
        jniWrapper.split(splitterId, vecBatchTmp3.getNativeVectorBatch());
        VecBatch vecBatchTmp4 = new VecBatch(buildValChar(2, "F"));
        jniWrapper.split(splitterId, vecBatchTmp4.getNativeVectorBatch());
        VecBatch vecBatchTmp5 = new VecBatch(buildValChar(2, "F"));
        jniWrapper.split(splitterId, vecBatchTmp5.getNativeVectorBatch());
        VecBatch vecBatchTmp6 = new VecBatch(buildValChar(2, "F"));
        jniWrapper.split(splitterId, vecBatchTmp6.getNativeVectorBatch());
        VecBatch vecBatchTmp7 = new VecBatch(buildValChar(1, "R"));
        jniWrapper.split(splitterId, vecBatchTmp7.getNativeVectorBatch());
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleFix1RowVBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_fix1Row_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                3,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        VecBatch vecBatchTmp1 = new VecBatch(buildValInt(3, 1));
        jniWrapper.split(splitterId, vecBatchTmp1.getNativeVectorBatch());
        VecBatch vecBatchTmp2 = new VecBatch(buildValInt(2, 2));
        jniWrapper.split(splitterId, vecBatchTmp2.getNativeVectorBatch());
        VecBatch vecBatchTmp3 = new VecBatch(buildValInt(3, 3));
        jniWrapper.split(splitterId, vecBatchTmp3.getNativeVectorBatch());
        VecBatch vecBatchTmp4 = new VecBatch(buildValInt(2, 4));
        jniWrapper.split(splitterId, vecBatchTmp4.getNativeVectorBatch());
        VecBatch vecBatchTmp5 = new VecBatch(buildValInt(2, 5));
        jniWrapper.split(splitterId, vecBatchTmp5.getNativeVectorBatch());
        VecBatch vecBatchTmp6 = new VecBatch(buildValInt(1, 6));
        jniWrapper.split(splitterId, vecBatchTmp6.getNativeVectorBatch());
        VecBatch vecBatchTmp7 = new VecBatch(buildValInt(3, 7));
        jniWrapper.split(splitterId, vecBatchTmp7.getNativeVectorBatch());
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }
}
