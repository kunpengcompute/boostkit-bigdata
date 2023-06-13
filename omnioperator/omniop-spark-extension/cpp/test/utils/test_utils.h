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
#include "shuffle/splitter.h"
#include "jni/concurrent_map.h"

static ConcurrentMap<std::shared_ptr<Splitter>> testShuffleSplitterHolder;

static std::string s_shuffle_tests_dir = "/tmp/shuffleTests";

VectorBatch *CreateVectorBatch(const DataTypes &types, int32_t rowCount, ...);

std::unique_ptr<BaseVector> CreateVector(DataType &dataType, int32_t rowCount, va_list &args);

template <typename T> std::unique_ptr<BaseVector> CreateVector(int32_t length, T *values)
{
    std::unique_ptr<Vector<T>> vector = std::make_unique<Vector<T>>(length);
    for (int32_t i = 0; i < length; i++) {
        vector->SetValue(i, values[i]);
    }
    return vector;
}

template <omniruntime::type::DataTypeId typeId>
std::unique_ptr<BaseVector> CreateFlatVector(int32_t length, va_list &args)
{
    using namespace omniruntime::type;
    using T = typename NativeType<typeId>::type;
    using VarcharVector = Vector<LargeStringContainer<std::string_view>>;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        std::unique_ptr<VarcharVector> vector = std::make_unique<VarcharVector>(length);
        std::string *str = va_arg(args, std::string *);
        for (int32_t i = 0; i < length; i++) {
            std::string_view value(str[i].data(), str[i].length());
            vector->SetValue(i, value);
        }
        return vector;
    } else {
        std::unique_ptr<Vector<T>> vector = std::make_unique<Vector<T>>(length);
        T *value = va_arg(args, T *);
        for (int32_t i = 0; i < length; i++) {
            vector->SetValue(i, value[i]);
        }
        return vector;
    }
}

std::unique_ptr<BaseVector> CreateDictionaryVector(DataType &dataType, int32_t rowCount, int32_t *ids, int32_t idsCount,
    ...);

template <DataTypeId typeId>
std::unique_ptr<BaseVector> CreateDictionary(BaseVector *vector, int32_t *ids, int32_t size)
{
    using T = typename NativeType<typeId>::type;
    if constexpr (std::is_same_v<T, std::string_view> || std::is_same_v<T, uint8_t>) {
        return VectorHelper::CreateStringDictionary(ids, size,
            reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector));
    }
    return VectorHelper::CreateDictionary(ids, size, reinterpret_cast<Vector<T> *>(vector));
}

VectorBatch* CreateVectorBatch_1row_varchar_withPid(int pid, std::string inputChar);

VectorBatch* CreateVectorBatch_4col_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_1FixCol_withPid(int parNum, int rowNum, DataTypePtr fixColType);

VectorBatch* CreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar);

VectorBatch* CreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_4charCols_withPid(int parNum, int rowNum);

VectorBatch* CreateVectorBatch_5fixedCols_withPid(int parNum, int rowNum);

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

void Test_splitter_split(long splitter_id, VectorBatch* vb);

void Test_splitter_stop(long splitter_id);

void Test_splitter_close(long splitter_id);

void GetFilePath(const char *path, const char *filename, char *filepath);

void DeletePathAll(const char* path);

#endif //SPARK_THESTRAL_PLUGIN_TEST_UTILS_H