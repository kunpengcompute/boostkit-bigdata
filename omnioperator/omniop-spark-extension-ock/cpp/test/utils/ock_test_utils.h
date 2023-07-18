/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_TEST_UTILS_H
#define SPARK_THESTRAL_PLUGIN_TEST_UTILS_H

#include <vector>
#include <chrono>
#include <list>
#include <stdlib.h>
#include <stdio.h>
#include <dirent.h>
#include <type/data_types.h>
#include "../../src/jni/concurrent_map.h"
#define private public
static const int varcharType = 5;

#include "../../src/shuffle/ock_splitter.h"

static ock::dopspark::ConcurrentMap<std::shared_ptr<ock::dopspark::OckSplitter>> Ockshuffle_splitter_holder_;

static std::string Ocks_shuffle_tests_dir = "/tmp/OckshuffleTests";

std::unique_ptr<BaseVector> CreateVector(DataType &dataType, int32_t rowCount, va_list &args);

VectorBatch *OckCreateInputData(const DataTypes &types, int32_t rowCount, ...);

VectorBatch *OckCreateVectorBatch(const DataTypes &types, int32_t rowCount, ...);

BaseVector *OckNewbuildVector(const DataTypeId &typeId, int32_t rowNumber);

VectorBatch *OckCreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_1row_varchar_withPid(int pid, const std::string &inputChar);

VectorBatch *OckCreateVectorBatch_4col_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar);

VectorBatch *OckCreateVectorBatch_5fixedCols_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_1fixedCols_withPid(int parNum, int32_t rowNum, DataTypePtr fixColType);

VectorBatch *OckCreateVectorBatch_2dictionaryCols_withPid(int partitionNum);

VectorBatch *OckCreateVectorBatch_1decimal128Col_withPid(int partitionNum, int rowNum);

VectorBatch *OckCreateVectorBatch_1decimal64Col_withPid(int partitionNum, int rowNum);

VectorBatch *OckCreateVectorBatch_2decimalCol_withPid(int partitionNum, int rowNum);

VectorBatch *OckCreateVectorBatch_someNullRow_vectorBatch();

VectorBatch *OckCreateVectorBatch_someNullCol_vectorBatch();

void OckTest_Shuffle_Compression(std::string compStr, int32_t numPartition, int32_t numVb, int32_t numRow);

ock::dopspark::OckHashWriteBuffer *OckGetLocalBuffer(long splitter_id);

long OckTest_splitter_nativeMake(std::string partitionMethod, int partitionNum, const int32_t *colTypeIds, int colNum,
    bool isCompress, uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity);

int OckTest_splitter_split(long splitter_id, VectorBatch *vb);

void OckTest_splitter_stop(long splitter_id);

void OckTest_splitter_close(long splitter_id);

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



template <typename T, typename V> T *OckCreateVector(V *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    auto vector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

#endif // SPARK_THESTRAL_PLUGIN_TEST_UTILS_H