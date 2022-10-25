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

#include "../../src/jni/concurrent_map.h"
#define private public
static const int varcharType = 5;

#include "../../src/shuffle/ock_splitter.h"

static ock::dopspark::ConcurrentMap<std::shared_ptr<ock::dopspark::OckSplitter>> Ockshuffle_splitter_holder_;

static std::string Ocks_shuffle_tests_dir = "/tmp/OckshuffleTests";

VectorBatch *OckCreateInputData(const int32_t numRows, const int32_t numCols, int32_t *inputTypeIds, int64_t *allData);

Vector *OckbuildVector(const DataType &aggType, int32_t rowNumber);

Vector *OckNewbuildVector(const DataTypeId &typeId, int32_t rowNumber);

VectorBatch *OckCreateVectorBatch_1row_varchar_withPid(int pid, const std::string &inputChar);

VectorBatch *OckCreateVectorBatch_4col_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_1longCol_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_2column_1row_withPid(int pid, std::string strVar, int intVar);

VectorBatch *OckCreateVectorBatch_4varcharCols_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_3fixedCols_withPid(int parNum, int rowNum);

VectorBatch *OckCreateVectorBatch_1fixedCols_withPid(int parNum, int32_t rowNum);

VectorBatch *OckCreateVectorBatch_2dictionaryCols_withPid(int partitionNum);

VectorBatch *OckCreateVectorBatch_1decimal128Col_withPid(int partitionNum);

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

template <typename T, typename V> T *OckCreateVector(V *values, int32_t length)
{
    VectorAllocator *vecAllocator = VectorAllocator::GetGlobalAllocator();
    auto vector = new T(vecAllocator, length);
    vector->SetValues(0, values, length);
    return vector;
}

#endif // SPARK_THESTRAL_PLUGIN_TEST_UTILS_H