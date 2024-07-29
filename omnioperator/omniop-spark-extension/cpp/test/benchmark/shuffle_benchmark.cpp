/**
 * Copyright (C) 2020-2024. Huawei Technologies Co., Ltd. All rights reserved.
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

#include <cstdlib>
#include <ctime>
#include "gtest/gtest.h"
#include "../utils/test_utils.h"

using namespace omniruntime::type;
using namespace omniruntime;

static constexpr int ROWS = 300;
static constexpr int COLS = 300;
static constexpr int PARTITION_SIZE = 600;
static constexpr int BATCH_COUNT = 20;

static int generateRandomNumber() {
    return std::rand() % PARTITION_SIZE;
}

// construct data
static std::vector<BaseVector *> constructVecs(int rows, int cols, int* inputTypeIds, double nullProbability) {
    std::srand(time(nullptr));
    std::vector<BaseVector *> vecs;
    vecs.resize(cols);

    for (int i = 0; i < cols; ++i) {
        BaseVector *vector = VectorHelper::CreateFlatVector(inputTypeIds[i], rows);
        if (inputTypeIds[i] == OMNI_VARCHAR) {
            auto strVec = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
            for(int j = 0; j < rows; ++j) {
                auto randNum = static_cast<double>(std::rand()) / RAND_MAX;
                if (randNum < nullProbability) {
                    strVec->SetNull(j);
                } else {
                    std::string_view str("hello world");
                    strVec->SetValue(j, str);
                }
            }
        } else if (inputTypeIds[i] == OMNI_LONG) {
            auto longVec = reinterpret_cast<Vector<long> *>(vector);
            for (int j = 0; j < rows; ++j) {
                auto randNum = static_cast<double>(std::rand()) / RAND_MAX;
                if (randNum < nullProbability) {
                    longVec->SetNull(j);
                } else {
                    long value = generateRandomNumber();
                    longVec->SetValue(j, value);
                }
            }
        }
        vecs[i] = vector;
    }
    return vecs;
}

// generate partitionId
static Vector<int32_t>* constructPidVec(int rows) {
    srand(time(nullptr));
    auto pidVec = new Vector<int32_t>(rows);
    for (int j = 0; j < rows; ++j) {
        int pid = generateRandomNumber();
        pidVec->SetValue(j, pid);
    }
    return pidVec;
}

static std::vector<VectorBatch *> generateData(int rows, int cols, int* inputTypeIds, double nullProbability) {
    std::vector<VectorBatch *> vecBatches;
    vecBatches.resize(BATCH_COUNT);
    for (int i = 0; i < BATCH_COUNT; ++i) {
        auto vecBatch = new VectorBatch(rows);
        auto pidVec = constructPidVec(rows);
        vecBatch->Append(pidVec);
        auto vecs = constructVecs(rows, cols, inputTypeIds, nullProbability);
        for (uint32_t j = 0; j < vecs.size(); ++j) {
            vecBatch->Append(vecs[j]);
        }
        vecBatches[i] = vecBatch;
    }
    return vecBatches;
}

static std::vector<VectorBatch *> copyData(const std::vector<VectorBatch *>& origin) {
    std::vector<VectorBatch *> vecBatches;
    vecBatches.resize(origin.size());
    for (int i = 0; i < origin.size(); ++i) {
        auto originBatch = origin[i];
        auto vecBatch = new VectorBatch(originBatch->GetRowCount());

        for (uint32_t j = 0; j < originBatch->GetVectorCount(); ++j) {
            BaseVector *vec = originBatch->Get(j);
            BaseVector *sliceVec = VectorHelper::SliceVector(vec, 0, originBatch->GetRowCount());
            vecBatch->Append(sliceVec);
        }
        vecBatches[i] = vecBatch;
    }
    return vecBatches;
}

static void bm_row_handle(const std::vector<VectorBatch *>& vecBatches, int *inputTypeIds, int cols) {
    Timer timer;
    timer.SetStart();

    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputTypeIds;

    auto splitOptions = SplitOptions::Defaults();
    splitOptions.buffer_size = 4096;

    auto compression_type_result = GetCompressionType("lz4");
    splitOptions.compression_type = compression_type_result;
    auto splitter = Splitter::Make("hash", inputDataTypes, cols, PARTITION_SIZE, std::move(splitOptions));

    for (uint32_t i = 0; i < vecBatches.size(); ++i) {
        VectorBatch *vb = vecBatches[i];
        splitter->SplitByRow(vb);
    }
    splitter->StopByRow();

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "row time, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    delete splitter;
}

static void bm_col_handle(const std::vector<VectorBatch *>& vecBatches, int *inputTypeIds, int cols) {
    Timer timer;
    timer.SetStart();

    InputDataTypes inputDataTypes;
    inputDataTypes.inputVecTypeIds = inputTypeIds;

    auto splitOptions = SplitOptions::Defaults();
    splitOptions.buffer_size = 4096;

    auto compression_type_result = GetCompressionType("lz4");
    splitOptions.compression_type = compression_type_result;
    auto splitter = Splitter::Make("hash", inputDataTypes, cols, PARTITION_SIZE, std::move(splitOptions));

    for (uint32_t i = 0; i < vecBatches.size(); ++i) {
        VectorBatch *vb = vecBatches[i];
        splitter->Split(*vb);
    }
    splitter->Stop();

    timer.CalculateElapse();
    double wallElapsed = timer.GetWallElapse();
    double cpuElapsed = timer.GetCpuElapse();
    std::cout << "col time, wall " << wallElapsed << " cpu " << cpuElapsed << std::endl;

    delete splitter;
}

TEST(shuffle_benchmark, null_0) {
    double strProbability = 0.25;
    double nullProbability = 0;

    int *inputTypeIds = new int32_t[COLS];
    for (int i = 0; i < COLS; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(ROWS, COLS, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << ROWS << ", cols: " << COLS << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, COLS);
    bm_col_handle(vecBatches2, inputTypeIds, COLS);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_25) {
    double strProbability = 0.25;
    double nullProbability = 0.25;

    int *inputTypeIds = new int32_t[COLS];
    for (int i = 0; i < COLS; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(ROWS, COLS, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << ROWS << ", cols: " << COLS << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, COLS);
    bm_col_handle(vecBatches2, inputTypeIds, COLS);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_50) {
    double strProbability = 0.25;
    double nullProbability = 0.5;

    int *inputTypeIds = new int32_t[COLS];
    for (int i = 0; i < COLS; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(ROWS, COLS, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << ROWS << ", cols: " << COLS << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, COLS);
    bm_col_handle(vecBatches2, inputTypeIds, COLS);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_75) {
    double strProbability = 0.25;
    double nullProbability = 0.75;

    int *inputTypeIds = new int32_t[COLS];
    for (int i = 0; i < COLS; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(ROWS, COLS, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << ROWS << ", cols: " << COLS << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, COLS);
    bm_col_handle(vecBatches2, inputTypeIds, COLS);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_100) {
    double strProbability = 0.25;
    double nullProbability = 1;

    int *inputTypeIds = new int32_t[COLS];
    for (int i = 0; i < COLS; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(ROWS, COLS, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << ROWS << ", cols: " << COLS << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, COLS);
    bm_col_handle(vecBatches2, inputTypeIds, COLS);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_25_row_900_col_100) {
    double strProbability = 0.25;
    double nullProbability = 0.25;
    int rows = 900;
    int cols = 100;

    int *inputTypeIds = new int32_t[cols];
    for (int i = 0; i < cols; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(rows, cols, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << rows << ", cols: " << cols << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, cols);
    bm_col_handle(vecBatches2, inputTypeIds, cols);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_25_row_1800_col_50) {
    double strProbability = 0.25;
    double nullProbability = 0.25;
    int rows = 1800;
    int cols = 50;

    int *inputTypeIds = new int32_t[cols];
    for (int i = 0; i < cols; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(rows, cols, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << rows << ", cols: " << cols << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, cols);
    bm_col_handle(vecBatches2, inputTypeIds, cols);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_25_row_9000_col_10) {
    double strProbability = 0.25;
    double nullProbability = 0.25;
    int rows = 9000;
    int cols = 10;

    int *inputTypeIds = new int32_t[cols];
    for (int i = 0; i < cols; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(rows, cols, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << rows << ", cols: " << cols << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, cols);
    bm_col_handle(vecBatches2, inputTypeIds, cols);
    delete[] inputTypeIds;
}

TEST(shuffle_benchmark, null_25_row_18000_col_5) {
    double strProbability = 0.25;
    double nullProbability = 0.25;
    int rows = 18000;
    int cols = 5;

    int *inputTypeIds = new int32_t[cols];
    for (int i = 0; i < cols; ++i) {
        double randNum = static_cast<double>(std::rand()) / RAND_MAX;
        if (randNum < strProbability) {
            inputTypeIds[i] = OMNI_VARCHAR;
        } else {
            inputTypeIds[i] = OMNI_LONG;
        }
    }

    auto vecBatches1 = generateData(rows, cols, inputTypeIds, nullProbability);
    auto vecBatches2 = copyData(vecBatches1);

    std::cout << "rows: " << rows << ", cols: " << cols << ", null probability: " << nullProbability << std::endl;
    bm_row_handle(vecBatches1, inputTypeIds, cols);
    bm_col_handle(vecBatches2, inputTypeIds, cols);
    delete[] inputTypeIds;
}