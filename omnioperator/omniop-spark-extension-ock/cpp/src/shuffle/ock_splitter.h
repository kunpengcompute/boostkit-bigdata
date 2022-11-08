/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_SPLITTER_H
#define SPARK_THESTRAL_PLUGIN_OCK_SPLITTER_H

#include <cstring>
#include <vector>
#include <chrono>
#include <memory>
#include <list>
#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <unordered_map>

#include "ock_type.h"
#include "common/common.h"
#include "vec_data.pb.h"
#include "ock_hash_write_buffer.h"

#include "memory/base_allocator.h"

using namespace spark;
using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::mem;

namespace ock {
namespace dopspark {
class OckSplitter {
    // VectorBatchRegion record those row in one partitionId which belong to current vector batch
    using VBRegion = struct VectorBatchRegion {
        std::vector<uint32_t> mRowIndexes {}; // cache the index of rows in preoccupied state
        uint32_t mRowNum = 0;
        uint32_t mLength = 0; // the length of cached rows in bytes
    };

public:
    OckSplitter() = default;
    ~OckSplitter() = default;

    OckSplitter(int32_t colNum, int32_t partitionNum, bool isSinglePt, uint64_t threadId);

    static std::shared_ptr<OckSplitter> Make(const std::string &partitionMethod, int partitionNum,
        const int32_t *colTypeIds, int32_t colNum, uint64_t threadId);
    bool Initialize(const int32_t *colTypeIds);
    bool Split(VectorBatch &vb);
    void Stop();

    inline bool SetShuffleInfo(const std::string &appId, uint32_t shuffleId, uint32_t stageId, uint32_t stageAttemptNum,
        uint32_t mapId, uint32_t taskAttemptId)
    {
        mOckBuffer = new (std::nothrow)
            OckHashWriteBuffer(appId, shuffleId, stageId, stageAttemptNum, mapId, taskAttemptId, mPartitionNum);
        if (UNLIKELY(mOckBuffer == nullptr)) {
            LogError("Failed to new instance for ock hash write buffer.");
            return false;
        }

        return true;
    }

    inline bool InitLocalBuffer(uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity, bool isCompress)
    {
        if (UNLIKELY(!mOckBuffer->Initialize(regionSize, minCapacity, maxCapacity, isCompress))) {
            LOG_ERROR("Failed to initialize ock local buffer, region size %d, capacity[%d, %d], compress %d",
                      regionSize, minCapacity, maxCapacity, isCompress);
            return false;
        }

        if (UNLIKELY(!InitCacheRegion())) {
            LOG_ERROR("Failed to initialize CacheRegion");
            return false;
        }
        return true;
    }

    [[nodiscard]] inline const std::vector<int64_t> &PartitionLengths() const
    {
        return mPartitionLengths;
    }

    [[nodiscard]] inline uint64_t GetTotalWriteBytes() const
    {
        return mTotalWriteBytes;
    }

private:
    static std::shared_ptr<OckSplitter> Create(const int32_t *colTypeIds, int32_t colNum, int32_t partitionNum,
        bool isSinglePt, uint64_t threadId);
    bool ToSplitterTypeId(const int32_t *vBColTypes);

    uint32_t GetVarVecValue(VectorBatch &vb, uint32_t rowIndex, uint32_t colIndex, uint8_t **address) const;
    uint32_t GetRowLengthInBytes(VectorBatch &vb, uint32_t rowIndex) const;

    inline uint32_t GetPartitionIdOfRow(uint32_t rowIndex)
    {
        // all row in the vector batch belong to partition 0 when the vector batch is single partition mode
        return mIsSinglePt ? 0 : mPtViewInCurVB->GetValue(rowIndex);
    }

    bool InitCacheRegion();

    inline void ResetCacheRegion()
    {
        for (auto &region : mCacheRegion) {
            region.mLength = 0;
            region.mRowNum = 0;
        }
    }

    inline void ResetCacheRegion(uint32_t partitionId)
    {
        VBRegion &vbRegion = mCacheRegion[partitionId];
        vbRegion.mRowNum = 0;
        vbRegion.mLength = 0;
    }

    inline VBRegion *GetCacheRegion(uint32_t partitionId)
    {
        return &mCacheRegion[partitionId];
    }

    inline void UpdateCacheRegion(uint32_t partitionId, uint32_t rowIndex, uint32_t length)
    {
        VBRegion &vbRegion = mCacheRegion[partitionId];
        if (vbRegion.mRowNum == 0) {
            vbRegion.mRowIndexes[vbRegion.mRowNum++] = rowIndex;
            vbRegion.mLength = length;
            return;
        }
        vbRegion.mRowIndexes[vbRegion.mRowNum++] = rowIndex;
        vbRegion.mLength += length;
    }

    bool FlushAllRegionAndGetNewBlob(VectorBatch &vb);
    bool PreoccupiedBufferSpace(VectorBatch &vb, uint32_t partitionId, uint32_t rowIndex, uint32_t rowLength,
        bool newRegion);
    bool WritePartVectorBatch(VectorBatch &vb, uint32_t partitionId);

    static bool WriteNullValues(Vector *vector, std::vector<uint32_t> &rowIndexes, uint32_t rowNum, uint8_t *&address);
    template <typename T>
    bool WriteFixedWidthValueTemple(Vector *vector, bool isDict, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
        T *&address);
    bool WriteDecimal128(Vector *vector, bool isDict, std::vector<uint32_t> &rowIndexes, uint32_t rowNum, uint64_t *&address);
    bool WriteFixedWidthValue(Vector *vector, ShuffleTypeId typeId, std::vector<uint32_t> &rowIndexes,
        uint32_t rowNum, uint8_t *&address);
    static bool WriteVariableWidthValue(Vector *vector, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
        uint8_t *&address);
    bool WriteOneVector(VectorBatch &vb, uint32_t colIndex, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
        uint8_t **address);

private:
    BaseAllocator *mAllocator = omniruntime::mem::GetProcessRootAllocator();

    static constexpr uint32_t vbDataHeadLen = 8; // Byte
    static constexpr uint32_t uint8Size = 1;
    static constexpr uint32_t uint16Size = 2;
    static constexpr uint32_t uint32Size = 4;
    static constexpr uint32_t uint64Size = 8;
    static constexpr uint32_t decimal128Size = 16;
    static constexpr uint32_t vbHeaderSize = 8;
    static constexpr uint32_t doubleNum = 2;
    /* the region use for all vector batch ---------------------------------------------------------------- */
    // this splitter which corresponding to one map task in one shuffle, so some params is same
    uint32_t mPartitionNum = 0;
    uint32_t mColNum = 0;
    uint64_t mThreadId = 0;
    bool mIsSinglePt = false;
    uint32_t mTotalWriteBytes = 0;
    std::vector<int64_t> mPartitionLengths {};

    // sum fixed columns length in byte which consist of null(1Byte) + value(1 ~ 8Byte)
    //     and fixed length in variable columns as null (1Byte) + offset(4Byte, more 1Byte)
    uint32_t mMinDataLenInVBByRow = 0;
    uint32_t mMinDataLenInVB = 0; // contains vb header and length of those var vector

    std::vector<DataTypeId> mVBColDataTypes {};
    std::vector<ShuffleTypeId> mVBColShuffleTypes {};
    std::vector<uint32_t> mColIndexOfVarVec {};

    /* the region use for current vector batch ------------------------------------------------------------ */
    // this splitter which handle some vector batch by split, will exist variable param in differ vector batch which
    // will reset at split function
    VectorBatch *mCurrentVB = nullptr;

    // MAP<partitionId, vbRegion> => vbRegion describe one vector batch with one partitionId will write to one region
    // in ock local blob
    std::vector<VBRegion> mCacheRegion {};

    // the vector point to vector0 in current vb which record rowIndex -> ptId
    IntVector *mPtViewInCurVB = nullptr;

    /* ock shuffle resource -------------------------------------------------------------------------------- */
    OckHashWriteBuffer *mOckBuffer = nullptr;

    uint64_t mPreoccupiedTime = 0;
    uint64_t mWriteVBTime = 0;
    uint64_t mReleaseResource = 0;
};
}
}

#endif // SPARK_THESTRAL_PLUGIN_OCK_SPLITTER_H