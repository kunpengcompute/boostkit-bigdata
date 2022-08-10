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

#ifndef SPARK_THESTRAL_PLUGIN_TEST_UTILS_H
#define SPARK_THESTRAL_PLUGIN_TEST_UTILS_H

#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <type/data_types.h>
#include "../../src/shuffle/splitter.h"
#include "../../src/jni/concurrent_map.h"

static ConcurrentMap<std::shared_ptr<Splitter>> shuffle_splitter_holder_;

static std::string s_shuffle_tests_dir = "/tmp/shuffleTests";

VectorBatch* CreateInputData(const int32_t numRows, const int32_t numCols, int32_t* inputTypeIds, int64_t* allData);

Vector *buildVector(const DataType &aggType, int32_t rowNumber);

VectorBatch* CreateVectorBatch_1row_varchar_withPid(int pid, std::string inputChar);

VectorBatch* CreateVectorBatch_4col_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_1longCol_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar);

VectorBatch* CreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_4charCols_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_3fixedCols_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_2dictionaryCols_withPid(int partitionNum);

VectorBatch* CreateVectorBatch_1decimal128Col_withPid(int partitionNum, int rowNum);

VectorBatch* CreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum);

VectorBatch* CreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum);
VectorBatch* CreateVectorBatch_someNullRow_vectorBatch();

VectorBatch* CreateVectorBatch_someNullCol_vectorBatch();

void Test_Shuffle_Compression(std::string compStr, int32_t numPartition, int32_t numVb, int32_t numRow);

long Test_splitter_nativeMake(std::string partitioning_name,
                              int num_partitions,
                              InputDataTypes inputDataTypes,
                              int numCols,
                              int buffer_size,
                              const char* compression_type_jstr,
                              std::string data_file_jstr,
                              int num_sub_dirs,
                              std::string local_dirs_jstr);

int Test_splitter_split(long splitter_id, VectorBatch* vb);

void Test_splitter_stop(long splitter_id);

void Test_splitter_close(long splitter_id);

template <typename T, typename V> T *CreateVector(V *values, int32_t length)
{
    VectorAllocator *vecAllocator = omniruntime::vec::GetProcessGlobalVecAllocator();
    auto vector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

void GetFilePath(const char *path, const char *filename, char *filepath);

void DeletePathAll(const char* path);

#endif //SPARK_THESTRAL_PLUGIN_TEST_UTILS_H