/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_TYPE_H
#define SPARK_THESTRAL_PLUGIN_OCK_TYPE_H

#include "ock_vector.h"
#include "common/debug.h"

namespace ock {
namespace dopspark {
enum class ShuffleTypeId : int {
    SHUFFLE_1BYTE,
    SHUFFLE_2BYTE,
    SHUFFLE_4BYTE,
    SHUFFLE_8BYTE,
    SHUFFLE_DECIMAL128,
    SHUFFLE_BIT,
    SHUFFLE_BINARY,
    SHUFFLE_LARGE_BINARY,
    SHUFFLE_NULL,
    NUM_TYPES,
    SHUFFLE_NOT_IMPLEMENTED
};

/*
 * read_blob memory layout as  |vb_data_batch1|vb_data_batch2|vb_data_batch3|vb_data_batch4|..........|
 *
 * vb_data_batch memory layout as
 * |length(uint32_t)|row_num(uint32_t)|col_num(uint32_t)|vector1|vector2|vector3|............|
 */
using VBHeaderPtr = struct VBDataHeaderDesc {
    uint32_t length = 0; // 4Byte
    uint32_t rowNum = 0; // 4Byte
} __attribute__((packed)) * ;

using VBDataDescPtr = struct VBDataDesc {
    explicit VBDataDesc(uint32_t colNum)
    {
        mHeader.rowNum = 0;
        mHeader.length = 0;
        mColumnsHead.reserve(colNum);
        mColumnsHead.resize(colNum);
        mColumnsCur.reserve(colNum);
        mColumnsCur.resize(colNum);
        mVectorValueLength.reserve(colNum);
        mVectorValueLength.resize(colNum);

        for (auto &index : mColumnsHead) {
            index = new (std::nothrow) OckVector();
        }
    }

    inline void Reset()
    {
        mHeader.rowNum = 0;
        mHeader.length = 0;
        std::fill(mVectorValueLength.begin(), mVectorValueLength.end(), 0);
        for (uint32_t index = 0; index < mColumnsCur.size(); ++index) {
            mColumnsCur[index] = mColumnsHead[index];
        }
    }

    VBDataHeaderDesc mHeader;
    std::vector<uint32_t> mVectorValueLength;
    std::vector<OckVector *> mColumnsCur;
    std::vector<OckVector *> mColumnsHead; // Array[List[OckVector *]]
} * ;
}
}
#define PROFILE_START_L1(name)                \
    long tcDiff##name = 0;                    \
    struct timespec tcStart##name = { 0, 0 }; \
    clock_gettime(CLOCK_MONOTONIC, &tcStart##name);

#define PROFILE_END_L1(name)                                                                     \
    struct timespec tcEnd##name = { 0, 0 };                                                      \
    clock_gettime(CLOCK_MONOTONIC, &tcEnd##name);                                                \
                                                                                                 \
    long diffSec##name = tcEnd##name.tv_sec - tcStart##name.tv_sec;                              \
    if (diffSec##name == 0) {                                                                    \
        tcDiff##name = tcEnd##name.tv_nsec - tcStart##name.tv_nsec;                              \
    } else {                                                                                     \
        tcDiff##name = diffSec##name * 1000000000 + tcEnd##name.tv_nsec - tcStart##name.tv_nsec; \
    }

#define PROFILE_VALUE(name) tcDiff##name

#endif // SPARK_THESTRAL_PLUGIN_OCK_TYPE_H