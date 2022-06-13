/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class ColumnShuffleGBSizeTest extends ColumnShuffleTest {
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
    public void columnShuffleFixed1GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_fixed1GB_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 3;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                4096,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 6 * 1024; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Ignore
    public void columnShuffleFixed10GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_fixed10GB_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 3;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                4096,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 10 * 8 * 1024; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleVarChar1GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_varChar1GB_test";
        DataType.DataTypeId[] idTypes = {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                1024,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        // 不能重复split同一个vb,接口有释放vb内存，重复split会导致重复释放内存而Core
        for (int i = 0; i < 99; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Ignore
    public void columnShuffleVarChar10GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_varChar10GB_test";
        DataType.DataTypeId[] idTypes = {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 4;
        long splitterId = jniWrapper.nativeMake(
                "hash",
                partitionNum,
                tmpStr,
                types.length,
                1024,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 10 * 3 * 999; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleMix1GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_mix1GB_test";
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
                4096,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        // 不能重复split同一个vb,接口有释放vb内存，重复split会导致重复释放内存而Core
        for (int i = 0; i < 6 * 999; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Ignore
    public void columnShuffleMix10GBTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_mix10GB_test";
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
                4096,    //shuffle value_buffer init size
                "lz4",
                shuffleDataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 3 * 9 * 999; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, partitionNum, false, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }
}
