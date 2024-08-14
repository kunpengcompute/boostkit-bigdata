/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef SPARK_THESTRAL_PLUGIN_OCK_HASH_WRITE_BUFFER_H
#define SPARK_THESTRAL_PLUGIN_OCK_HASH_WRITE_BUFFER_H

#include <cstdint>
#include <vector>
#include <string>
#include <iostream>
#include <queue>
#include "common/debug.h"
#include "common/common.h"
#include "sdk/ock_shuffle_sdk.h"

namespace ock {
namespace dopspark {
const int H_OK = 0;
const int H_FAIL = -1;

class OckHashWriteBuffer {
public:
    OckHashWriteBuffer() = default;
    OckHashWriteBuffer(const std::string &appId, uint32_t shuffleId, uint32_t stageId, uint32_t stageAttemptNum,
        uint32_t mapId, uint32_t taskAttemptId, uint32_t partitionNum)
        : mAppId(appId),
          mShuffleId(shuffleId),
          mStageId(stageId),
          mStageAttemptNum(stageAttemptNum),
          mMapId(mapId),
          mTaskAttemptId(taskAttemptId),
          mPartitionNum(partitionNum)
    {
        mTaskId = mAppId + "_" + std::to_string(shuffleId) + "_" + std::to_string(mTaskAttemptId);
    }
    ~OckHashWriteBuffer() = default;

    bool Initialize(uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity, bool isCompress, bool isRss,
        uint32_t &writerId);
    bool GetNewBuffer();
    bool InitRssBuffer(uint32_t &writerId);

    enum class ResultFlag {
        ENOUGH,
        NEW_REGION,
        LACK,
        UNEXPECTED
    };

    ResultFlag PreoccupiedDataSpace(uint32_t partitionId, uint32_t length, bool newRegion);
    uint8_t *GetEndAddressOfRegion(uint32_t partitionId, uint32_t &regionId, uint32_t length);
    bool Flush(bool isFinished, uint64_t &length, uint32_t writerId);
    bool CommitFullRegion4Rss(uint32_t writerId);

    [[nodiscard]] inline bool IsCompress() const
    {
        return mIsCompress;
    }

    [[maybe_unused]] inline uint8_t *GetBaseAddress()
    {
        return mBaseAddress;
    }

    [[maybe_unused]] [[nodiscard]] inline uint32_t DataSize() const
    {
        return mDataCapacity;
    }

    [[nodiscard]] inline uint32_t GetRegionSize() const
    {
        return mEachPartitionSize;
    }

    [[nodiscard]] inline std::vector<uint32_t> GetToCommitPartition() const
    {
        std::vector<uint32_t> partitionIds;
        for (uint32_t regionId : mToCommitRegions) {
            partitionIds.push_back(mRegionToPartition[regionId]);
        }
        return partitionIds;
    }

private:
    bool FlushAllRegion4Rss(uint32_t writerId, uint64_t &length);
    bool ResetRegionBuffer4Rss(uint32_t regionNum, uint32_t *regionIds);
    bool FlushFullRegion4Rss(uint32_t writerId, uint64_t &length, uint32_t *regionPtRecord, uint32_t *regionUsedRecord);

    inline int CommitRegionInternal(uint32_t region, uint32_t writerId, uint32_t &partitionId)
    {
        uint32_t regionSize = mRegionUsedSize[region];
        if (regionSize > 0) {
            partitionId = mRegionToPartition[region];
            uint8_t *address = nullptr;
            address = mBaseAddress + region * mEachPartitionSize;
            return OckShuffleSdk::mRssCommitRegionFun(writerId, region, partitionId, address, regionSize);
        }
        return H_OK;
    }

    inline bool GetNewRegion(uint32_t partitionId, uint32_t &regionId)
    {
        regionId = mUsedPartitionRegion++;
        if (regionId >= mPartitionNum) {
            if (!mReleasedPartitions.empty()) {
                regionId = mReleasedPartitions.front();
                mReleasedPartitions.pop();
            } else {
                return false; // There is no data region to write shuffle data
            }
        }

        mPtCurrentRegionId[partitionId] = regionId;
        mRegionToPartition[regionId] = partitionId;
        return true;
    }

    [[nodiscard]] inline bool LowBufferUsedRatio() const
    {
        return mTotalSize <= (mDataCapacity * 0.05);
    }

    static inline void EncodeBigEndian(uint8_t *buf, uint32_t value)
    {
        int loopNum = sizeof(uint32_t);
        for (int index = 0; index < loopNum; index++) {
            buf[index] = (value >> (24 - index * 8)) & 0xFF;
        }
    }

private:
    static constexpr int groupSize = 2;
    static constexpr int reserveSize = 2;
    static constexpr int mSinglePartitionAndRegionUsedSize = 8;
    static constexpr int mSingleRegionUsedSize = 4;
    /* the region define for total lifetime, init at new instance */
    std::string mAppId;
    std::string mTaskId;
    uint32_t mShuffleId = 0;
    uint32_t mStageId = 0;
    uint32_t mStageAttemptNum = 0;
    uint32_t mMapId = 0;
    uint32_t mTaskAttemptId = 0;
    uint32_t mDataCapacity = 0;
    uint32_t mRealCapacity = 0;
    uint32_t mRegionSize = 0; // single region size
    uint32_t mRegionUsedRecordOffset = 0;
    uint32_t mRegionPtRecordOffset = 0;
    bool mIsCompress = true;
    bool mIsRss = false;
    uint32_t mTypeFlag = 0; // 0 means ock local blob used as hash write mode

    uint32_t mEachPartitionSize = 0; // Size of each partition
    uint32_t mDoublePartitionSize = 0;
    uint32_t mPartitionNum = 0;

    /* the region define for one local blob lifetime, will reset at init */
    uint64_t mBlobId = 0;
    uint8_t *mBaseAddress = nullptr;
    uint32_t mTotalSize = 0;
    uint32_t mUsedPartitionRegion = 0;
    std::queue<uint32_t> mReleasedPartitions{};  // used for rss, which region is full and commit
    std::vector<uint32_t> mToCommitRegions{};    // used for rss, region wait to commit

    std::vector<uint32_t> mPtCurrentRegionId{};
    std::vector<uint32_t> mRegionToPartition{};
    std::vector<uint32_t> mRegionUsedSize{};
};
}
}
#endif // SPARK_THESTRAL_PLUGIN_OCK_HASH_WRITE_BUFFER_H