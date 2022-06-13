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
import org.junit.Test;

import java.io.IOException;

public class ColumnShuffleNullTest extends ColumnShuffleTest {
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
    public void columnShuffleFixNullTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_fixNull_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
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
        // 不能重复split同一个vb,接口有释放vb内存，重复split会导致重复释放内存而Core
        for (int i = 0; i < 1; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9, numPartition, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleVarCharNullTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_fixNull_test";
        DataType.DataTypeId[] idTypes = {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR,OMNI_VARCHAR};
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
        // 不能重复split同一个vb,接口有释放vb内存，重复split会导致重复释放内存而Core
        for (int i = 0; i < 1; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9, numPartition, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleMixNullTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_MixNull_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE,OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
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
        // 不能重复split同一个vb,接口有释放vb内存，重复split会导致重复释放内存而Core
        for (int i = 0; i < 1; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9, numPartition, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }

    @Test
    public void columnShuffleMixNullFullTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_MixNullFull_test";
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE,OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
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
        for (int i = 0; i < 1; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 9999, numPartition, true, true);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }
}
