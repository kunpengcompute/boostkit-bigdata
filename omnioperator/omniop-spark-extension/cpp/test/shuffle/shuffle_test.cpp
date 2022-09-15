/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "gtest/gtest.h"
#include "../utils/test_utils.h"

static std::string tmpTestingDir;
static std::string tmpShuffleFilePath;

class ShuffleTest : public testing::Test {
protected:

    // run before first case...
    static void SetUpTestSuite() {
        tmpTestingDir = s_shuffle_tests_dir;
        if (!IsFileExist(tmpTestingDir)) {
            mkdir(tmpTestingDir.c_str(), S_IRWXU|S_IRWXG|S_IROTH|S_IXOTH);
        }
    }

    // run after last case...
    static void TearDownTestSuite() {
        if (IsFileExist(tmpTestingDir)) {
            DeletePathAll(tmpTestingDir.c_str());
        }
    }

    // run before each case...
    virtual void SetUp() override {
    }

    // run after each case...
    virtual void TearDown() override {
        if (IsFileExist(tmpShuffleFilePath)) {
            remove(tmpShuffleFilePath.c_str());
        }
    }

};

TEST_F (ShuffleTest, Split_SingleVarChar) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_SingleVarChar";
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int splitterId = Test_splitter_nativeMake("hash",
                                              4,
                                              inputDataTypes,
                                              colNumber,
                                              1024,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    VectorBatch* vb1 = CreateVectorBatch_1row_varchar_withPid(3, "N");
    Test_splitter_split(splitterId, vb1);
    VectorBatch* vb2 = CreateVectorBatch_1row_varchar_withPid(2, "F");
    Test_splitter_split(splitterId, vb2);
    VectorBatch* vb3 = CreateVectorBatch_1row_varchar_withPid(3, "N");
    Test_splitter_split(splitterId, vb3);
    VectorBatch* vb4 = CreateVectorBatch_1row_varchar_withPid(2, "F");
    Test_splitter_split(splitterId, vb4);
    VectorBatch* vb5 = CreateVectorBatch_1row_varchar_withPid(2, "F");
    Test_splitter_split(splitterId, vb5);
    VectorBatch* vb6 = CreateVectorBatch_1row_varchar_withPid(1, "R");
    Test_splitter_split(splitterId, vb6);
    VectorBatch* vb7 = CreateVectorBatch_1row_varchar_withPid(3, "N");
    Test_splitter_split(splitterId, vb7);
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Fixed_Cols) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_fixed_cols";
    int32_t inputVecTypeIds[] = {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 1; j++) {
        VectorBatch* vb = CreateVectorBatch_5fixedCols_withPid(partitionNum, 999);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Fixed_SinglePartition_SomeNullRow) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_fixed_singlePartition_someNullRow";
    int32_t inputVecTypeIds[] = {OMNI_BOOLEAN, OMNI_SHORT, OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 1;
    int splitterId = Test_splitter_nativeMake("single",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch* vb = CreateVectorBatch_someNullRow_vectorBatch();
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Fixed_SinglePartition_SomeNullCol) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_fixed_singlePartition_someNullCol";
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 1;
    int splitterId = Test_splitter_nativeMake("single",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch* vb = CreateVectorBatch_someNullCol_vectorBatch();
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Compression_None) {
    Test_Shuffle_Compression("uncompressed", 4, 999, 999);
}

TEST_F (ShuffleTest, Split_Compression_zstd) {
    Test_Shuffle_Compression("zstd", 4, 999, 999);
}

TEST_F (ShuffleTest, Split_Compression_Lz4) {
    Test_Shuffle_Compression("lz4", 4, 999, 999);
}

TEST_F (ShuffleTest, Split_Compression_Snappy) {
    Test_Shuffle_Compression("snappy", 4, 999, 999);
}

TEST_F (ShuffleTest, Split_Compression_Zlib) {
    Test_Shuffle_Compression("zlib", 4, 999, 999);
}

TEST_F (ShuffleTest, Split_Mix_LargeSize) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_mix_largeSize";
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG, OMNI_DOUBLE, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 999; j++) {
        VectorBatch* vb = CreateVectorBatch_4col_withPid(partitionNum, 999);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Short_10WRows) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_short_10WRows";
    int32_t inputVecTypeIds[] = {OMNI_SHORT};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 10;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch* vb = CreateVectorBatch_1FixCol_withPid(partitionNum, 1000, OMNI_SHORT);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Boolean_10WRows) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_boolean_10WRows";
    int32_t inputVecTypeIds[] = {OMNI_BOOLEAN};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 10;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch* vb = CreateVectorBatch_1FixCol_withPid(partitionNum, 1000, OMNI_BOOLEAN);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Long_100WRows) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_long_100WRows";
    int32_t inputVecTypeIds[] = {OMNI_LONG};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 10;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 100; j++) {
        VectorBatch* vb = CreateVectorBatch_1FixCol_withPid(partitionNum, 10000, OMNI_LONG);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_VarChar_LargeSize) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_varchar_largeSize";
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR, OMNI_VARCHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              64,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 99; j++) {
        VectorBatch* vb = CreateVectorBatch_4varcharCols_withPid(partitionNum, 99);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_VarChar_First) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_varchar_first";
    int32_t inputVecTypeIds[] = {OMNI_VARCHAR, OMNI_INT};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    VectorBatch* vb0 = CreateVectorBatch_2column_1row_withPid(0, "corpbrand #4", 1);
    Test_splitter_split(splitterId, vb0);
    VectorBatch* vb1 = CreateVectorBatch_2column_1row_withPid(3, "brandmaxi #4", 1);
    Test_splitter_split(splitterId, vb1);
    VectorBatch* vb2 = CreateVectorBatch_2column_1row_withPid(1, "edu packnameless #9", 1);
    Test_splitter_split(splitterId, vb2);
    VectorBatch* vb3 = CreateVectorBatch_2column_1row_withPid(1, "amalgunivamalg #11", 1);
    Test_splitter_split(splitterId, vb3);
    VectorBatch* vb4 = CreateVectorBatch_2column_1row_withPid(0, "brandcorp #2", 1);
    Test_splitter_split(splitterId, vb4);
    VectorBatch* vb5 = CreateVectorBatch_2column_1row_withPid(0, "scholarbrand #2", 1);
    Test_splitter_split(splitterId, vb5);
    VectorBatch* vb6 = CreateVectorBatch_2column_1row_withPid(2, "edu packcorp #6", 1);
    Test_splitter_split(splitterId, vb6);
    VectorBatch* vb7 = CreateVectorBatch_2column_1row_withPid(2, "edu packamalg #1", 1);
    Test_splitter_split(splitterId, vb7);
    VectorBatch* vb8 = CreateVectorBatch_2column_1row_withPid(0, "brandnameless #8", 1);
    Test_splitter_split(splitterId, vb8);
    VectorBatch* vb9 = CreateVectorBatch_2column_1row_withPid(2, "univmaxi #2", 1);
    Test_splitter_split(splitterId, vb9);
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Dictionary) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_split_dictionary";
    int32_t inputVecTypeIds[] = {OMNI_INT, OMNI_LONG};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 2; j++) {
        VectorBatch* vb = CreateVectorBatch_2dictionaryCols_withPid(partitionNum);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Char) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_char_largeSize";
    int32_t inputVecTypeIds[] = {OMNI_CHAR, OMNI_CHAR, OMNI_CHAR, OMNI_CHAR};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              64,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 99; j++) {
        VectorBatch* vb = CreateVectorBatch_4charCols_withPid(partitionNum, 99);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Decimal128) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_decimal128_test";
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL128};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 999; j++) {
        VectorBatch* vb = CreateVectorBatch_1decimal128Col_withPid(partitionNum, 999);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Decimal64) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_decimal64_test";
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL64};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 999; j++) {
        VectorBatch* vb = CreateVectorBatch_1decimal64Col_withPid(partitionNum, 999);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}

TEST_F (ShuffleTest, Split_Decimal64_128) {
    tmpShuffleFilePath = tmpTestingDir + "/shuffle_decimal64_128";
    int32_t inputVecTypeIds[] = {OMNI_DECIMAL64, OMNI_DECIMAL128};
    int colNumber = sizeof(inputVecTypeIds) / sizeof(inputVecTypeIds[0]);
    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputVecTypeIds;
    inputDataTypes.inputDataPrecisions = new uint32_t[colNumber];
    inputDataTypes.inputDataScales = new uint32_t[colNumber];
    int partitionNum = 4;
    int splitterId = Test_splitter_nativeMake("hash",
                                              partitionNum,
                                              inputDataTypes,
                                              colNumber,
                                              4096,
                                              "lz4",
                                              tmpShuffleFilePath,
                                              0,
                                              tmpTestingDir);
    for (uint64_t j = 0; j < 999; j++) {
        VectorBatch* vb = CreateVectorBatch_2decimalCol_withPid(partitionNum, 999);
        Test_splitter_split(splitterId, vb);
    }
    Test_splitter_stop(splitterId);
    Test_splitter_close(splitterId);
    delete[] inputDataTypes.inputDataPrecisions;
    delete[] inputDataTypes.inputDataScales;
}