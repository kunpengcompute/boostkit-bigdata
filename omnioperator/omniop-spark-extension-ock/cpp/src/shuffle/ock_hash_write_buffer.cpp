/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ock_hash_write_buffer.h"
#include "sdk/ock_shuffle_sdk.h"

using namespace ock::dopspark;

void *OckShuffleSdk::mHandle = nullptr;
FUNC_GET_LOCAL_BLOB OckShuffleSdk::mGetLocalBlobFun = nullptr;
FUNC_COMMIT_LOCAL_BLOB OckShuffleSdk::mCommitLocalBlobFun = nullptr;
FUNC_MAP_BLOB OckShuffleSdk::mMapBlobFun = nullptr;
FUNC_UNMAP_BLOB OckShuffleSdk::mUnmapBlobFun = nullptr;

bool OckHashWriteBuffer::Initialize(uint32_t regionSize, uint32_t minCapacity, uint32_t maxCapacity, bool isCompress)
{
    if (UNLIKELY(mPartitionNum == 0)) {
        LogError("Partition number can't be zero.");
        return false;
    }

    mIsCompress = isCompress;
    uint32_t bufferNeed = regionSize * mPartitionNum;
    mDataCapacity = std::min(std::max(bufferNeed, minCapacity), maxCapacity);
    mRegionPtRecordOffset = mDataCapacity - mSinglePartitionAndRegionUsedSize * mPartitionNum;
    mRegionUsedRecordOffset = mDataCapacity - mSingleRegionUsedSize * mPartitionNum;

    mEachPartitionSize = mDataCapacity / mPartitionNum - mSinglePartitionAndRegionUsedSize;
    mDoublePartitionSize = reserveSize * mEachPartitionSize;

    mRealCapacity = mIsCompress ? mDataCapacity + mDoublePartitionSize : mDataCapacity;

    // init meta information for local blob
    mPtCurrentRegionId.resize(mPartitionNum);
    mRegionToPartition.resize(mPartitionNum);
    mRegionUsedSize.resize(mPartitionNum);

    return GetNewBuffer();
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

OckHashWriteBuffer::ResultFlag OckHashWriteBuffer::PreoccupiedDataSpace(uint32_t partitionId, uint32_t length,
    bool newRegion)
{
    if (UNLIKELY(length > mEachPartitionSize)) {
        LogError("The row size is %d exceed region size %d.", length, mEachPartitionSize);
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

    // 3. get the near region
    uint32_t nearRegionId = ((regionId % 2) == 0) ? (regionId + 1) : (regionId - 1);
    // 4. compute remaining size of current region. Consider the used size of near region
    uint32_t remainBufLength = ((regionId == (mPartitionNum - 1)) && ((regionId % 2) == 0)) ?
        (mEachPartitionSize - mRegionUsedSize[regionId]) :
        (mDoublePartitionSize - mRegionUsedSize[regionId] - mRegionUsedSize[nearRegionId]);
    if (remainBufLength >= length) {
        mRegionUsedSize[regionId] += length;
        mTotalSize += length; // todo check
        return ResultFlag::ENOUGH;
    }

    return (mUsedPartitionRegion + 1 >= mPartitionNum) ? ResultFlag::LACK : ResultFlag::NEW_REGION;
}

uint8_t *OckHashWriteBuffer::GetEndAddressOfRegion(uint32_t partitionId, uint32_t &regionId, uint32_t length)
{
    uint32_t offset;
    regionId = mPtCurrentRegionId[partitionId];

    if ((regionId % groupSize) == 0) {
        offset = regionId * mEachPartitionSize + mRegionUsedSize[regionId] - length;
    } else {
        offset = (regionId + 1) * mEachPartitionSize - mRegionUsedSize[regionId];
    }

    return mBaseAddress + offset;
}

bool OckHashWriteBuffer::Flush(bool isFinished, uint32_t &length)
{
    // point to the those region(pt -> regionId, region size -> regionId) the local blob
    auto regionPtRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionPtRecordOffset);
    auto regionUsedRecord = reinterpret_cast<uint32_t *>(mBaseAddress + mRegionUsedRecordOffset);

    // write meta information for those partition regions in the local blob
    for (uint32_t index = 0; index < mPartitionNum; index++) {
        EncodeBigEndian((uint8_t *)(&regionPtRecord[index]), mRegionToPartition[index]);
        EncodeBigEndian((uint8_t *)(&regionUsedRecord[index]), mRegionUsedSize[index]);
    }

    uint32_t flags = LowBufferUsedRatio() ? (1 << 1) : 0;
    flags |= isFinished ? 0x01 : 0x00;

    int ret = OckShuffleSdk::mCommitLocalBlobFun(mAppId.c_str(), mBlobId, flags, mMapId, mTaskAttemptId, mPartitionNum,
        mStageId, mStageAttemptNum, mDoublePartitionSize, &length);

    void *address = reinterpret_cast<void *>(mIsCompress ? mBaseAddress - mDoublePartitionSize : mBaseAddress);
    OckShuffleSdk::mUnmapBlobFun(mBlobId, address);

    return (ret == 0);
}