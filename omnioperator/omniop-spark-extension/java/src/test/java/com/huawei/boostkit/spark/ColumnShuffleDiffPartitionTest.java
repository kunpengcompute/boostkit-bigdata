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

public class ColumnShuffleDiffPartitionTest extends ColumnShuffleTest {
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
    public void columnShuffleSinglePartitionTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_singlePartition_test";
        columnShufflePartitionTest("single", shuffleDataFile);
    }

    @Test
    public void columnShuffleHashPartitionTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_hashPartition_test";
        columnShufflePartitionTest("hash", shuffleDataFile);
    }

    @Test
    public void columnShuffleRangePartitionTest() throws IOException {
        shuffleDataFile = shuffleTestDir + "/shuffle_dataFile_rangePartition_test";
        columnShufflePartitionTest("range", shuffleDataFile);
    }

    public void columnShufflePartitionTest(String partitionType, String dataFile) throws IOException {
        DataType.DataTypeId[] idTypes = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR, OMNI_CHAR,
                OMNI_DATE32, OMNI_DATE64, OMNI_DECIMAL64, OMNI_DECIMAL128};
        DataType[] types = dataTypeId2DataType(idTypes);
        String tmpStr = DataTypeSerializer.serialize(types);
        SparkJniWrapper jniWrapper = new SparkJniWrapper();
        int partitionNum = 1;
        boolean pidVec = true;
        if (partitionType.equals("single")){
            pidVec = false;
        }
        long splitterId = jniWrapper.nativeMake(
                partitionType,
                1,
                tmpStr,
                types.length,
                3,
                "lz4",
                dataFile,
                0,
                shuffleTestDir,
                64 * 1024,
                4096,
                1024 * 1024 * 1024);
        for (int i = 0; i < 99; i++) {
            VecBatch vecBatchTmp = buildVecBatch(idTypes, 999, partitionNum, true, pidVec);
            jniWrapper.split(splitterId, vecBatchTmp.getNativeVectorBatch());
        }
        jniWrapper.stop(splitterId);
        jniWrapper.close(splitterId);
    }
}
