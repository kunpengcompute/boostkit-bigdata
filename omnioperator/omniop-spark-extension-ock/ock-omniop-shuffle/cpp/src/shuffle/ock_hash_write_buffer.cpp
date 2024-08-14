/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ock_hash_write_buffer.h"

using namespace ock::dopspark;

void *OckShuffleSdk::mHandle = nullptr;
FUNC_GET_LOCAL_BLOB OckShuffleSdk::mGetLocalBlobFun = nullptr;
FUNC_COMMIT_LOCAL_BLOB OckShuffleSdk::mCommitLocalBlobFun = nullptr;
FUNC_MAP_BLOB OckShuffleSdk::mMapBlobFun = nullptr;
FUNC_UNMAP_BLOB OckShuffleSdk::mUnmapBlobFun = nullptr;
FUNC_RSS_CREATE_WRITER OckShuffleSdk::mRssCreateWriterFun = nullptr;
FUNC_RSS_COMMIT_REGION OckShuffleSdk::mRssCommitRegionFun = nullptr;
FUNC_RSS_FLUSH_REGION OckShuffleSdk::mRssFlushRegionFun = nullptr;
FUNC_RSS_GET_RELEASE_REGION OckShuffleSdk::mRssGetReleaseRegionFun = nullptr;

bool OckHashWriteBuffer::Initialize(uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity, bool isCompress,
    bool isRss, uint32_t &writerId)
{
    if (UNLIKELY(mPartitionNum == 0)) {
        LogError("Partition number can't be zero.");
        return false;
    }

    mIsRss = isRss;
    mIsCompress = isCompress;
    mRegionSize = regionSize;
    uint32_t bufferNeed = regionSize * mPartitionNum;
    if (isRss) {
        mDataCapacity = bufferNeed;
    } else {
        mDataCapacity = std::min(std::max(bufferNeed, minCapacity), maxCapacity);
    }
    if (UNLIKELY(mDataCapacity < mSinglePartitionAndRegionUsedSize * mPartitionNum)) {
        LogError("mDataCapacity should be bigger than mSinglePartitionAndRegionUsedSize * mPartitionNum");
        return false;
    }
    mRegionPtRecordOffset = mDataCapacity - mSinglePartitionAndRegionUsedSize * mPartitionNum;
    if (UNLIKELY(mDataCapacity < mSingleRegionUsedSize * mPartitionNum)) {
        LogError("mDataCapacity should be bigger than mSingleRegionUsedSize * mPartitionNum");
        return false;
    }
    mRegionUsedRecordOffset = mDataCapacity - mSingleRegionUsedSize * mPartitionNum;

    if (UNLIKELY(mDataCapacity / mPartitionNum < mSinglePartitionAndRegionUsedSize)) {
        LogError("mDataCapacity / mPartitionNum should be bigger than mSinglePartitionAndRegionUsedSize");
        return false;
    }
    mEachPartitionSize = mDataCapacity / mPartitionNum - mSinglePartitionAndRegionUsedSize;
    mDoublePartitionSize = reserveSize * mEachPartitionSize;

    // used for ess
    mRealCapacity = mIsCompress ? mDataCapacity + mDoublePartitionSize : mDataCapacity;

    // init meta information for local blob
    mPtCurrentRegionId.resize(mPartitionNum);
    mRegionToPartition.resize(mPartitionNum);
    mRegionUsedSize.resize(mPartitionNum);

    return isRss ? InitRssBuffer(writerId) : GetNewBuffer();
}

bool OckHashWriteBuffer::GetNewBuffer()
{
    int ret = OckShuffleSdk::mGetLocalBlobFun(mAppId.c_str(), mTaskId.c_str(), mRealCapacity, mPartitionNum, mTypeFlag,
        &mBlobId);
    if (ret != 0) {
        LogError("Failed to get local blob for size %d , blob id %ld", mRealCapacity, mBlobId);
        return false;
    }

    void *address = nullptr;
    ret = OckShuffleSdk::mMapBlobFun(mBlobId, &address, mAppId.c_str());
    if (ret != 0) {
        LogError("Failed to map local blob id %ld", mBlobId);
        return false;
    }
    mBaseAddress = mIsCompress ? reinterpret_cast<uint8_t *>(address) + mDoublePartitionSize :
                                 reinterpret_cast<uint8_t *>(address);

    // reset data struct for new buffer
    mTotalSize = 0;
    mUsedPartitionRegion = 0;

    std::fill(mPtCurrentRegionId.begin(), mPtCurrentRegionId.end(), UINT32_MAX);
    std::fill(mRegionToPartition.begin(), mRegionToPartition.end(), UINT32_MAX);
    std::fill(mRegionUsedSize.begin(), mRegionUsedSize.end(), 0);

    return true;
}

bool OckHashWriteBuffer::InitRssBuffer(uint32_t &writerId)
{
    uint8_t *address = nullptr;
    int ret = OckShuffleSdk::mRssCreateWriterFun(mRegionSize, mPartitionNum, &writerId, &address);
    if (ret != 0) {
        LogError("Failed to create rss writer");
        return false;
    }
    mBaseAddress = address;

    mTotalSize = 0;
    mUsedPartitionRegion = 0;

    std::fill(mPtCurrentRegionId.begin(), mPtCurrentRegionId.end(), UINT32_MAX);
    std::fill(mRegionToPartition.begin(), mRegionToPartition.end(), UINT32_MAX);
    std::fill(mRegionUsedSize.begin(), mRegionUsedSize.end(), 0);

    return true;
}

OckHashWriteBuffer::ResultFlag OckHashWriteBuffer::PreoccupiedDataSpace(uint32_t partitionId, uint32_t length,
    bool newRegion)
{
    if (UNLIKELY(length > mEachPartitionSize)) {
        LogError("The row size is %d exceed region size %d.", length, mEachPartitionSize);
        return ResultFlag::UNEXPECTED;
    }

    if (UNLIKELY(mTotalSize > UINT32_MAX - length)) {
        LogError("mTotalSize + length exceed UINT32_MAX");
        return ResultFlag::UNEXPECTED;
    }
    // 1. get the new region id for partitionId
    uint32_t regionId = UINT32_MAX;
    if (newRegion && !GetNewRegion(partitionId, regionId)) {
        return ResultFlag::UNEXPECTED;
    }

    // 2. get current region id for partitionId
    regionId = mPtCurrentRegionId[partitionId];
    // -1 means the first time to get new data region
    if ((regionId == UINT32_MAX && !GetNewRegion(partitionId, regionId))) {
        ASSERT(newRgion);
        return ResultFlag::LACK;
    }

    uint32_t remainBufLength = 0;
    if (mIsRss) {
        remainBufLength = mEachPartitionSize - mRegionUsedSize[regionId];
    } else {
        // 3. get the near region
        uint32_t nearRegionId = ((regionId % 2) == 0) ? (regionId + 1) : (regionId - 1);
        // 4. compute remaining size of current region. Consider the used size of near region
        remainBufLength = ((regionId == (mPartitionNum - 1)) && ((regionId % 2) == 0)) ?
            (mEachPartitionSize - mRegionUsedSize[regionId]) :
            (mDoublePartitionSize - mRegionUsedSize[regionId] - mRegionUsedSize[nearRegionId]);
    }
    if (remainBufLength >= length) {
        mRegionUsedSize[regionId] += length;
        mTotalSize += length;
        return ResultFlag::ENOUGH;
    } else {
        if (mIsRss) {
            mToCommitRegions.push_back(regionId);
            return ResultFlag::NEW_REGION;
        }
    }

    return ((mUsedPartitionRegion + 1 >= mPartitionNum) && mReleasedPartitions.empty()) ? ResultFlag::LACK :
        ResultFlag::NEW_REGION;
}

uint8_t *OckHashWriteBuffer::GetEndAddressOfRegion(uint32_t partitionId, uint32_t &regionId, uint32_t length)
{
    uint32_t offset;
    regionId = mPtCurrentRegionId[partitionId];

    if (mIsRss) {
        offset = regionId * mEachPartitionSize + mRegionUsedSize[regionId] - length;
    } else {
        if ((regionId % groupSize) == 0) {
            if (UNLIKELY(regionId * mEachPartitionSize + mRegionUsedSize[regionId] < length)) {
                LogError("regionId * mEachPartitionSize + mRegionUsedSize[regionId] shoulld be bigger than length");
                return nullptr;
            }
            offset = regionId * mEachPartitionSize + mRegionUsedSize[regionId] - length;
        } else {
            if (UNLIKELY((regionId + 1) * mEachPartitionSize < mRegionUsedSize[regionId])) {
                LogError("(regionId + 1) * mEachPartitionSize  shoulld be bigger than mRegionUsedSize[regionId]");
                return nullptr;
            }
            offset = (regionId + 1) * mEachPartitionSize - mRegionUsedSize[regionId];
        }
    }

    return mBaseAddress + offset;
}

bool OckHashWriteBuffer::Flush(bool isFinished, uint64_t &length, uint32_t writerId)
{
    // point to the those region(pt -> regionId, region size -> regionId) the local blob
    auto regionPtRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionPtRecordOffset);
    auto regionUsedRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionUsedRecordOffset);

    // write meta information for those partition regions in the local blob
    if (!mIsRss || isFinished) {
        for (uint32_t index = 0; index < mPartitionNum; index++) {
            EncodeBigEndian((uint8_t *)(&regionPtRecord[index]), mRegionToPartition[index]);
            EncodeBigEndian((uint8_t *)(&regionUsedRecord[index]), mRegionUsedSize[index]);
        }
    }

    if (mIsRss) {
        if (isFinished) {
            return FlushAllRegion4Rss(writerId, length);
        } else {
            return FlushFullRegion4Rss(writerId, length, regionPtRecord, regionUsedRecord);
        }
    } else {
        uint32_t flags = LowBufferUsedRatio() ? (1 << 1) : 0;
        flags |= isFinished ? 0x01 : 0x00;

        uint32_t totalSize;
        int ret = OckShuffleSdk::mCommitLocalBlobFun(mAppId.c_str(), mBlobId, flags, mMapId, mTaskAttemptId, mPartitionNum,
            mStageId, mStageAttemptNum, mDoublePartitionSize, &totalSize);
        length = totalSize;

        void *address = reinterpret_cast<void *>(mIsCompress ? mBaseAddress - mDoublePartitionSize : mBaseAddress);
        OckShuffleSdk::mUnmapBlobFun(mBlobId, address);
        return (ret == H_OK);
    }
}

bool OckHashWriteBuffer::FlushAllRegion4Rss(uint32_t writerId, uint64_t &length)
{
    int ret = H_FAIL;
    for (uint32_t regionId = 0; regionId < mPartitionNum; ++regionId) {
        uint32_t partitionId = 0;
        ret = CommitRegionInternal(regionId, writerId, partitionId);
        if (ret != H_OK) {
            LogError("Commit regionId %d failed of partition %d", regionId, partitionId);
            return false;
        }
    }

    ret = OckShuffleSdk::mRssFlushRegionFun(writerId, true, &length);
    if (ret != H_OK) {
        LogError("Flush all region failed.");
        return false;
    }
    return true;
}

bool OckHashWriteBuffer::FlushFullRegion4Rss(uint32_t writerId, uint64_t &length, uint32_t *regionPtRecord,
    uint32_t *regionUsedRecord)
{
    int ret = H_FAIL;
    for (uint32_t regionId : mToCommitRegions) {
        EncodeBigEndian((uint8_t *)(&regionPtRecord[regionId]), mRegionToPartition[regionId]);
        EncodeBigEndian((uint8_t *)(&regionUsedRecord[regionId]), mRegionUsedSize[regionId]);

        uint32_t partitionId = 0;
        ret = CommitRegionInternal(regionId, writerId, partitionId);
        if (ret != H_OK) {
            LogError("Commit regionId %d failed of partition %d", regionId, partitionId);
            return false;
        }
    }

    ret = OckShuffleSdk::mRssFlushRegionFun(writerId, false, &length);
    if (ret != H_OK) {
        LogError("Flush full region failed.");
        return false;
    }

    uint32_t *regionIds = nullptr;
    uint32_t regionNum = 0;
    ret = OckShuffleSdk::mRssGetReleaseRegionFun(writerId, &regionIds, &regionNum);
    if (ret != H_OK) {
        LogError("Get released regionId failed.");
        return false;
    }

    return ResetRegionBuffer4Rss(regionNum, regionIds);
}

bool OckHashWriteBuffer::CommitFullRegion4Rss(uint32_t writerId)
{
    auto regionPtRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionPtRecordOffset);
    auto regionUsedRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionUsedRecordOffset);

    int ret = H_FAIL;
    for (uint32_t regionId : mToCommitRegions) {
        EncodeBigEndian((uint8_t *)(&regionPtRecord[regionId]), mRegionToPartition[regionId]);
        EncodeBigEndian((uint8_t *)(&regionUsedRecord[regionId]), mRegionUsedSize[regionId]);

        uint32_t partitionId = 0;
        ret = CommitRegionInternal(regionId, writerId, partitionId);
        if (ret != H_OK) {
            LogError("Commit regionId %d failed of partition %d", regionId, partitionId);
            return false;
        }
    }

    uint32_t *regionIds = nullptr;
    uint32_t regionNum = 0;
    ret = OckShuffleSdk::mRssGetReleaseRegionFun(writerId, &regionIds, &regionNum);
    if (ret != H_OK) {
        LogError("Get released regionId failed.");
        return false;
    }

    return ResetRegionBuffer4Rss(regionNum, regionIds);
}

bool OckHashWriteBuffer::ResetRegionBuffer4Rss(uint32_t regionNum, uint32_t *regionIds)
{
    for (uint32_t i = 0; i < regionNum; ++i) {
        uint32_t region = *regionIds++;
        uint32_t regionSize = mRegionUsedSize[region];
        if (regionSize > 0) {
            uint8_t *address = nullptr;
            address = mBaseAddress + region * mEachPartitionSize;
            if (EOK != memset_s(address, regionSize, 0, regionSize)) {
                LogError("Failed to reset rss region buffer.");
                return false;
            }

            mPtCurrentRegionId[mRegionToPartition[region]] = UINT32_MAX;
            mRegionToPartition[region] = UINT32_MAX;
            mRegionUsedSize[region] = 0;

            mReleasedPartitions.push(region);
        }
    }

    mToCommitRegions.clear();

    return true;
}