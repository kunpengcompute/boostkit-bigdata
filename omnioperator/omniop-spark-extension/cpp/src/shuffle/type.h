/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2021. All rights reserved.
 */

#ifndef CPP_TYPE_H
#define CPP_TYPE_H
#include <vector/vector_common.h>
#include "../io/SparkFile.hh"
#include "../io/ColumnWriter.hh"

using namespace spark;
using namespace omniruntime::mem;

static constexpr int32_t kDefaultSplitterBufferSize = 4096;
static constexpr int32_t kDefaultNumSubDirs = 64;

struct SplitOptions {
    int32_t buffer_size = kDefaultSplitterBufferSize;
    int32_t num_sub_dirs = kDefaultNumSubDirs;
    CompressionKind compression_type = CompressionKind_NONE;
    std::string next_spilled_file_dir = "";

    std::string data_file;

    int64_t thread_id = -1;
    int64_t task_attempt_id = -1;

    BaseAllocator *allocator = omniruntime::mem::GetProcessRootAllocator();

    uint64_t spill_batch_row_num = 4096; // default value
    uint64_t spill_mem_threshold = 1024 * 1024 * 1024; // default value
    uint64_t compress_block_size = 64 * 1024; // default value

    static SplitOptions Defaults();
};

enum SHuffleTypeId : int {
    SHUFFLE_1BYTE = 0,
    SHUFFLE_2BYTE = 1,
    SHUFFLE_4BYTE = 2,
    SHUFFLE_8BYTE = 3,
    SHUFFLE_DECIMAL128 = 4,
    SHUFFLE_BIT = 5,
    SHUFFLE_BINARY = 6,
    SHUFFLE_LARGE_BINARY = 7,
    SHUFFLE_NULL = 8,
    NUM_TYPES = 9,
    SHUFFLE_NOT_IMPLEMENTED = 10
};

struct InputDataTypes {
    int32_t *inputVecTypeIds = nullptr;
    uint32_t *inputDataPrecisions = nullptr;
    uint32_t *inputDataScales = nullptr;
};

#endif //CPP_TYPE_H