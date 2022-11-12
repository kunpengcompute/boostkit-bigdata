/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_MERGE_READER_H
#define SPARK_THESTRAL_PLUGIN_OCK_MERGE_READER_H

#include "common/common.h"
#include "ock_type.h"

namespace ock {
namespace dopspark {
class OckMergeReader {
public:
    bool Initialize(const int32_t *typeIds, uint32_t colNum);
    bool GetMergeVectorBatch(uint8_t *&address, uint32_t remain, uint32_t maxRowNum, uint32_t maxSize);

    bool CopyPartDataToVector(uint8_t *&nulls, uint8_t *&values, uint32_t &remainingSize, uint32_t &remainingCapacity,
         OckVectorPtr &srcVector);
    bool CopyDataToVector(omniruntime::vec::Vector *dstVector, uint32_t colIndex);

    [[nodiscard]] inline uint32_t GetVectorBatchLength() const
    {
        return mVectorBatch->GetTotalCapacity();
    }

    [[nodiscard]] inline uint32_t GetRowNumAfterMerge() const
    {
        return mVectorBatch->GetTotalRowNum();
    }

    bool CalVectorValueLength(uint32_t colIndex, uint32_t &length);

private:
    static bool GenerateVector(OckVectorPtr &vector, uint32_t rowNum, int32_t typeId, uint8_t *&startAddress);
    bool ScanOneVectorBatch(uint8_t *&startAddress);
    static constexpr int capacityOffset = 4;
    static constexpr int decimal128Size = 16;
    static constexpr int maxCapacityInBytes = 1073741824;

private:
    // point to shuffle blob current vector batch data header
    uint32_t mColNum = 0;
    uint32_t mMergeCnt = 0;
    std::vector<int32_t> mColTypeIds {};
    VBDataDescPtr mVectorBatch = nullptr;
};
}
}
#endif // SPARK_THESTRAL_PLUGIN_OCK_MERGE_READER_H