/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_TYPE_H
#define SPARK_THESTRAL_PLUGIN_OCK_TYPE_H

#include "ock_vector.h"
#include "common/common.h"

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
} __attribute__((packed)) *;

class VBDataDesc {
public:
    VBDataDesc() = default;
    ~VBDataDesc()
    {
        for (auto &vector : mColumnsHead) {
            if (vector == nullptr) {
                continue;
            }
            auto currVector = vector;
            while (currVector->GetNextVector() != nullptr) {
                auto nextVector = currVector->GetNextVector();
                currVector->SetNextVector(nullptr);
                currVector = nextVector;
            }
        }
    }

    bool Initialize(uint32_t colNum)
    {
        this->colNum = colNum;
        mHeader.rowNum = 0;
        mHeader.length = 0;
        mColumnsHead.resize(colNum);
        mColumnsCur.resize(colNum);
        mColumnsCapacity.resize(colNum);

        for (auto &vector : mColumnsHead) {
            vector = std::make_shared<OckVector>();
            if (vector == nullptr) {
                mColumnsHead.clear();
                return false;
            }
        }
        return true;
    }

    inline void Reset()
    {
        mHeader.rowNum = 0;
        mHeader.length = 0;
        std::fill(mColumnsCapacity.begin(), mColumnsCapacity.end(), 0);
        for (uint32_t index = 0; index < mColumnsCur.size(); ++index) {
            mColumnsCur[index] = mColumnsHead[index];
        }
    }

    std::shared_ptr<OckVector> GetColumnHead(uint32_t colIndex) {
        if (colIndex >= colNum) {
            return nullptr;
        }
        return mColumnsHead[colIndex];
    }

    void SetColumnCapacity(uint32_t colIndex, uint32_t length) {
        mColumnsCapacity[colIndex] = length;
    }

    uint32_t GetColumnCapacity(uint32_t colIndex) {
        return mColumnsCapacity[colIndex];
    }

    std::shared_ptr<OckVector> GetCurColumn(uint32_t colIndex)
    {
        if (colIndex >= colNum) {
            return nullptr;
        }
        auto currVector = mColumnsCur[colIndex];
        if (currVector->GetNextVector() == nullptr) {
            auto newCurVector = std::make_shared<OckVector>();
            if (UNLIKELY(newCurVector == nullptr)) {
                LOG_ERROR("Failed to new instance for ock vector");
                return nullptr;
            }
            currVector->SetNextVector(newCurVector);
            mColumnsCur[colIndex] = newCurVector;
        } else {
            mColumnsCur[colIndex] = currVector->GetNextVector();
        }
        return currVector;
    }

    uint32_t GetTotalCapacity()
    {
        return mHeader.length;
    }

    uint32_t GetTotalRowNum()
    {
        return mHeader.rowNum;
    }

    void AddTotalCapacity(uint32_t length) {
        mHeader.length += length;
    }

    void AddTotalRowNum(uint32_t rowNum)
    {
        mHeader.rowNum +=rowNum;
    }

private:
    uint32_t colNum = 0;
    VBDataHeaderDesc mHeader;
    std::vector<uint32_t> mColumnsCapacity;
    std::vector<OckVectorPtr> mColumnsCur;
    std::vector<OckVectorPtr> mColumnsHead; // Array[List[OckVector *]]
};
using VBDataDescPtr = std::shared_ptr<VBDataDesc>;
}
}

#endif // SPARK_THESTRAL_PLUGIN_OCK_TYPE_H