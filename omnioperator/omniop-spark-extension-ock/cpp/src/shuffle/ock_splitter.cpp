/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ock_splitter.h"

#include <sstream>

using namespace omniruntime::type;
using namespace ock::dopspark;

OckSplitter::OckSplitter(int32_t colNum, int32_t partitionNum, bool isSinglePt, uint64_t threadId)
    : mColNum(colNum), mPartitionNum(partitionNum), mIsSinglePt(isSinglePt), mThreadId(threadId)
{
    LOG_DEBUG("Input schema columns number: %d", colNum);
}

bool OckSplitter::ToSplitterTypeId(const int32_t *vBColTypes)
{
    // each vector inside exist one null vector which cost 1Byte
    mMinDataLenInVBByRow = mColNum;

    for (uint32_t colIndex = 0; colIndex < mColNum; ++colIndex) {
        switch (vBColTypes[colIndex]) {
            case OMNI_BOOLEAN: {
                CasOmniToShuffleType(OMNI_BOOLEAN, ShuffleTypeId::SHUFFLE_1BYTE, uint8Size);
                break;
            }
            case OMNI_SHORT: {
                CasOmniToShuffleType(OMNI_SHORT, ShuffleTypeId::SHUFFLE_2BYTE, uint16Size);
                break;
            }
            case OMNI_DATE32: {
                CasOmniToShuffleType(OMNI_DATE32, ShuffleTypeId::SHUFFLE_4BYTE, uint32Size);
                break;
            }
            case OMNI_INT: {
                CasOmniToShuffleType(OMNI_INT, ShuffleTypeId::SHUFFLE_4BYTE, uint32Size);
                break;
            }
            case OMNI_DATE64: {
                CasOmniToShuffleType(OMNI_DATE64, ShuffleTypeId::SHUFFLE_8BYTE, uint64Size);
                break;
            }
            case OMNI_DOUBLE: {
                CasOmniToShuffleType(OMNI_DOUBLE, ShuffleTypeId::SHUFFLE_8BYTE, uint64Size);
                break;
            }
            case OMNI_DECIMAL64: {
                CasOmniToShuffleType(OMNI_DECIMAL64, ShuffleTypeId::SHUFFLE_8BYTE, uint64Size);
                break;
            }
            case OMNI_LONG: {
                CasOmniToShuffleType(OMNI_LONG, ShuffleTypeId::SHUFFLE_8BYTE, uint64Size);
                break;
            }
            case OMNI_CHAR: {
                CasOmniToShuffleType(OMNI_CHAR, ShuffleTypeId::SHUFFLE_BINARY, uint32Size);
                mColIndexOfVarVec.emplace_back(colIndex);
                break;
            }
            case OMNI_VARCHAR: {        // unknown length for value vector, calculate later
                CasOmniToShuffleType(OMNI_VARCHAR, ShuffleTypeId::SHUFFLE_BINARY, uint32Size);
                mColIndexOfVarVec.emplace_back(colIndex);
                break;
            }
            case OMNI_DECIMAL128: {
                CasOmniToShuffleType(OMNI_DECIMAL128, ShuffleTypeId::SHUFFLE_DECIMAL128, decima128Size);
                break;
            }
            default: {
                LOG_ERROR("Unsupported data type id %d", vBColTypes[colIndex]);
                return false;
            }
        }
    }

    mMinDataLenInVB = vbDataHeadLen + uint32Size * mColIndexOfVarVec.size(); // 4 * mVarVecNum used for offset last

    return true;
}

bool OckSplitter::InitCacheRegion()
{
    mCacheRegion.reserve(mPartitionNum);
    mCacheRegion.resize(mPartitionNum);

    if (UNLIKELY(mOckBuffer->GetRegionSize() * 2 < mMinDataLenInVB || mMinDataLenInVBByRow == 0)) {
        LOG_DEBUG("regionSize * doubleNum should be bigger than mMinDataLenInVB %d", mMinDataLenInVBByRow);
        return false;
    }
    uint32_t rowNum = (mOckBuffer->GetRegionSize() * 2 - mMinDataLenInVB) / mMinDataLenInVBByRow;
    LOG_INFO("Each region can cache row number is %d", rowNum);

    for (auto &region : mCacheRegion) {
        region.mRowIndexes.reserve(rowNum);
        region.mRowIndexes.resize(rowNum);
        region.mLength = 0;
        region.mRowNum = 0;
    }
    return true;
}

bool OckSplitter::Initialize(const int32_t *colTypeIds)
{
    mVBColShuffleTypes.reserve(mColNum);
    mColIndexOfVarVec.reserve(mColNum);

    if (UNLIKELY(!ToSplitterTypeId(colTypeIds))) {
        LOG_ERROR("Failed to initialize ock splitter");
        return false;
    }

    mColIndexOfVarVec.reserve(mColIndexOfVarVec.size());
    mPartitionLengths.resize(mPartitionNum);
    std::fill(mPartitionLengths.begin(), mPartitionLengths.end(), 0);
    return true;
}

std::shared_ptr<OckSplitter> OckSplitter::Create(const int32_t *colTypeIds, int32_t colNum, int32_t partitionNum,
    bool isSinglePt, uint64_t threadId)
{
    std::shared_ptr<OckSplitter> instance = std::make_shared<OckSplitter>(colNum, partitionNum, isSinglePt, threadId);
    if (UNLIKELY(instance == nullptr)) {
        LOG_ERROR("Failed to new ock splitter instance.");
        return nullptr;
    }

    if (UNLIKELY(!instance->Initialize(colTypeIds))) {
        LOG_ERROR("Failed to initialize ock splitter");
        instance = nullptr;
    }

    return instance;
}

std::shared_ptr<OckSplitter> OckSplitter::Make(const std::string &partitionMethod, int partitionNum,
    const int32_t *colTypeIds, int32_t colNum, uint64_t threadId)
{
    if (UNLIKELY(colTypeIds == nullptr || colNum == 0)) {
        LOG_ERROR("colTypeIds is null or colNum is 0, colNum %d", colNum);
        return nullptr;
    }
    if (partitionMethod == "hash" || partitionMethod == "rr" || partitionMethod == "range") {
        return Create(colTypeIds, colNum, partitionNum, false, threadId);
    } else if (UNLIKELY(partitionMethod == "single")) {
        return Create(colTypeIds, colNum, partitionNum, true, threadId);
    } else {
        LOG_ERROR("Unsupported partition method %s", partitionMethod.c_str());
        return nullptr;
    }
}

uint32_t OckSplitter::GetVarVecValue(VectorBatch &vb, uint32_t rowIndex, uint32_t colIndex) const
{
    auto vector = mIsSinglePt ? vb.Get(colIndex) : vb.Get(static_cast<int>(colIndex + 1));
    if (vector->GetEncoding() == OMNI_DICTIONARY) {
        auto vc = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector);
        std::string_view value = vc->GetValue(rowIndex);
        return static_cast<uint32_t>(value.length());
    } else {
        auto vc = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
        std::string_view value = vc->GetValue(rowIndex);
        return static_cast<uint32_t>(value.length());
    }
}

uint32_t OckSplitter::GetRowLengthInBytes(VectorBatch &vb, uint32_t rowIndex) const
{
    uint32_t length = mMinDataLenInVBByRow;

    // calculate variable width value
    for (auto &colIndex : mColIndexOfVarVec) {
        length += GetVarVecValue(vb, rowIndex, colIndex);
    }

    return length;
}

bool OckSplitter::WriteNullValues(BaseVector *vector, std::vector<uint32_t> &rowIndexes, uint32_t rowNum, uint8_t *&address)
{
    uint8_t *nullAddress = address;

    for (uint32_t index = 0; index < rowNum; ++index) {
        *nullAddress = const_cast<uint8_t *>((uint8_t *)(unsafe::UnsafeBaseVector::GetNulls(vector)))[rowIndexes[index]];
        nullAddress++;
    }

    address = nullAddress;
    return true;
}

template <typename T>
bool OckSplitter::WriteFixedWidthValueTemple(BaseVector *vector, bool isDict, std::vector<uint32_t> &rowIndexes,
    uint32_t rowNum, T *&address, DataTypeId dataTypeId)
{
    T *dstValues = address;
    T *srcValues = nullptr;

    if (isDict) {
        int32_t idsNum = mCurrentVB->GetRowCount();
        int64_t idsSizeInBytes = idsNum * sizeof(int32_t);
        auto ids = VectorHelper::UnsafeGetValues(vector, dataTypeId);
        srcValues = reinterpret_cast<T *>(VectorHelper::UnsafeGetDictionary(vector, dataTypeId));
        if (UNLIKELY(srcValues == nullptr)) {
            LOG_ERROR("Source values address is null.");
            return false;
        }

        for (uint32_t index = 0; index < rowNum; ++index) {
            uint32_t idIndex = rowIndexes[index];
            if (UNLIKELY(idIndex >= idsNum)) {
                LOG_ERROR("Invalid idIndex %d, idsNum.", idIndex, idsNum);
                return false;
            }
            uint32_t rowIndex = reinterpret_cast<int32_t *>(ids)[idIndex];
            *dstValues++ = srcValues[rowIndex]; // write value to local blob
        }
    } else {
        srcValues = reinterpret_cast<T *>(VectorHelper::UnsafeGetValues(vector, dataTypeId));
        if (UNLIKELY(srcValues == nullptr)) {
            LOG_ERROR("Source values address is null.");
            return false;
        }
        int32_t srcRowCount = vector->GetSize();
        for (uint32_t index = 0; index < rowNum; ++index) {
            uint32_t rowIndex = rowIndexes[index];
            if (UNLIKELY(rowIndex >= srcRowCount)) {
                LOG_ERROR("Invalid rowIndex %d, srcRowCount %d.", rowIndex, srcRowCount);
                return false;
            }
            *dstValues++ = srcValues[rowIndex]; // write value to local blob
        }
    }

    address = dstValues;

    return true;
}

bool OckSplitter::WriteDecimal128(BaseVector *vector, bool isDict, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
    uint64_t *&address, DataTypeId dataTypeId)
{
    uint64_t *dstValues = address;
    uint64_t *srcValues = nullptr;

    if (isDict) {
        uint32_t idsNum = mCurrentVB->GetRowCount();
        auto ids = VectorHelper::UnsafeGetValues(vector, dataTypeId);
        srcValues = reinterpret_cast<uint64_t *>(VectorHelper::UnsafeGetDictionary(vector, dataTypeId));
        if (UNLIKELY(srcValues == nullptr)) {
            LOG_ERROR("Source values address is null.");
            return false;
        }
        for (uint32_t index = 0; index < rowNum; ++index) {
            uint32_t idIndex = rowIndexes[index];
            if (UNLIKELY(idIndex >= idsNum)) {
                LOG_ERROR("Invalid idIndex %d, idsNum.", idIndex, idsNum);
                return false;
            }
            uint32_t rowIndex = reinterpret_cast<int32_t *>(ids)[idIndex];
            if (UNLIKELY(rowIndex >= srcRowCount)) {
                LOG_ERROR("Invalid rowIndex %d, srcRowCount %d.", rowIndex, srcRowCount);
                return false;
            }
            *dstValues++ = srcValues[rowIndex << 1];
            *dstValues++ = srcValues[rowIndex << 1 | 1];
        }
    } else {
        srcValues = reinterpret_cast<uint64_t *>(VectorHelper::UnsafeGetValues(vector, dataTypeId));
        if (UNLIKELY(srcValues == nullptr)) {
            LOG_ERROR("Source values address is null.");
            return false;
        }
        int32_t srcRowCount = vector->GetSize();
        for (uint32_t index = 0; index < rowNum; ++index) {
            uint32_t rowIndex = rowIndexes[index];
            if (UNLIKELY(rowIndex >= srcRowCount)) {
                LOG_ERROR("Invalid rowIndex %d, srcRowCount %d.", rowIndex, srcRowCount);
                return false;
            }
            *dstValues++ = srcValues[rowIndexes[index] << 1]; // write value to local blob
            *dstValues++ = srcValues[rowIndexes[index] << 1 | 1]; // write value to local blob
        }
    }

    address = dstValues;
    return true;
}

bool OckSplitter::WriteFixedWidthValue(BaseVector *vector, ShuffleTypeId typeId, std::vector<uint32_t> &rowIndexes, 
    uint32_t rowNum, uint8_t *&address, DataTypeId dataTypeId)
{
    bool isDict = (vector->GetEncoding() == OMNI_DICTIONARY);
    switch (typeId) {
        case ShuffleTypeId::SHUFFLE_1BYTE: {
            WriteFixedWidthValueTemple<uint8_t>(vector, isDict, rowIndexes, rowNum, address, dataTypeId);
            break;
        }
        case ShuffleTypeId::SHUFFLE_2BYTE: {
            auto *addressFormat = reinterpret_cast<uint16_t *>(address);
            WriteFixedWidthValueTemple<uint16_t>(vector, isDict, rowIndexes, rowNum, addressFormat, dataTypeId);
            address = reinterpret_cast<uint8_t *>(addressFormat);
            break;
        }
        case ShuffleTypeId::SHUFFLE_4BYTE: {
            auto *addressFormat = reinterpret_cast<uint32_t *>(address);
            WriteFixedWidthValueTemple<uint32_t>(vector, isDict, rowIndexes, rowNum, addressFormat, dataTypeId);
            address = reinterpret_cast<uint8_t *>(addressFormat);
            break;
        }
        case ShuffleTypeId::SHUFFLE_8BYTE: {
            auto *addressFormat = reinterpret_cast<uint64_t *>(address);
            WriteFixedWidthValueTemple<uint64_t>(vector, isDict, rowIndexes, rowNum, addressFormat, dataTypeId);
            address = reinterpret_cast<uint8_t *>(addressFormat);
            break;
        }
        case ShuffleTypeId::SHUFFLE_DECIMAL128: {
            auto *addressFormat = reinterpret_cast<uint64_t *>(address);
            WriteDecimal128(vector, isDict, rowIndexes, rowNum, addressFormat, dataTypeId);
            address = reinterpret_cast<uint8_t *>(addressFormat);
            break;
        }
        default: {
            LogError("Unexpected shuffle type id %d", typeId);
            return false;
        }
    }

    return true;
}

bool OckSplitter::WriteVariableWidthValue(BaseVector *vector, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
    uint8_t *&address)
{
    bool isDict = (vector->GetEncoding() == OMNI_DICTIONARY);
    auto *offsetAddress = reinterpret_cast<int32_t *>(address);            // point the offset space base address
    uint8_t *valueStartAddress = address + (rowNum + 1) * sizeof(int32_t); // skip the offsets space
    uint8_t *valueAddress = valueStartAddress;

    uint32_t length = 0;
    uint8_t *srcValues = nullptr;
    int32_t vectorSize = vector->GetSize();
    for (uint32_t rowCnt = 0; rowCnt < rowNum; rowCnt++) {
        uint32_t rowIndex = rowIndexes[rowCnt];
        if (UNLIKELY(rowIndex >= vectorSize)) {
            LOG_ERROR("Invalid rowIndex %d, vectorSize %d.", rowIndex, vectorSize);
            return false;
        }
        if (isDict) {
            auto vc = reinterpret_cast<Vector<DictionaryContainer<std::string_view, LargeStringContainer>> *>(vector);
            std::string_view value = vc->GetValue(rowIndex);
            srcValues = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
            length = static_cast<uint32_t>(value.length());
        } else {
            auto vc = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(vector);
            std::string_view value = vc->GetValue(rowIndex);
            srcValues = reinterpret_cast<uint8_t *>(reinterpret_cast<int64_t>(value.data()));
            length = static_cast<uint32_t>(value.length());
        }
        // write the null value in the vector with row index to local blob
        if (UNLIKELY(length > 0 && memcpy_s(valueAddress, length, srcValues, length) != EOK)) {
            LOG_ERROR("Failed to write variable value with length %d", length);
            return false;
        }

        offsetAddress[rowCnt] = length;
        valueAddress += length;
    }

    offsetAddress[rowNum] = valueAddress - valueStartAddress;
    address = valueAddress;

    return true;
}

bool OckSplitter::WriteOneVector(VectorBatch &vb, uint32_t colIndex, std::vector<uint32_t> &rowIndexes, uint32_t rowNum,
    uint8_t **address)
{
    BaseVector *vector = vb.Get(colIndex);
    if (UNLIKELY(vector == nullptr)) {
        LOG_ERROR("Failed to get vector with index %d in current vector batch", colIndex);
        return false;
    }

    // write null values
    if (UNLIKELY(!WriteNullValues(vector, rowIndexes, rowNum, *address))) {
        LOG_ERROR("Failed to write null values for vector index %d in current vector batch", colIndex);
        return false;
    }

    ShuffleTypeId typeId = mIsSinglePt ? mVBColShuffleTypes[colIndex] : mVBColShuffleTypes[colIndex - 1];
    DataTypeId dataTypeId = mIsSinglePt ? mVBColDataTypes[colIndex] : mVBColDataTypes[colIndex - 1];
    if (typeId == ShuffleTypeId::SHUFFLE_BINARY) {
        return WriteVariableWidthValue(vector, rowIndexes, rowNum, *address);
    } else {
        return WriteFixedWidthValue(vector, typeId, rowIndexes, rowNum, *address, dataTypeId);
    }
}

bool OckSplitter::WritePartVectorBatch(VectorBatch &vb, uint32_t partitionId)
{
    VBRegion *vbRegion = GetCacheRegion(partitionId);
    // check whether exist history vb data belong to the partitionId
    if (vbRegion->mRowNum == 0) {
        return true;
    }

    // get address of the partition region in local blob
    uint32_t regionId = 0;
    // backspace from local blob the region end address to remove preoccupied bytes for the vector batch region
    auto address = mOckBuffer->GetEndAddressOfRegion(partitionId, regionId, vbRegion->mLength);
    if (UNLIKELY(address == nullptr)) {
        LOG_ERROR("Failed to get address with partitionId %d", partitionId);
        return false;
    }
    // write the header information of the vector batch in local blob
    auto header = reinterpret_cast<VBDataHeaderDesc *>(address);
    header->length = vbRegion->mLength;
    header->rowNum = vbRegion->mRowNum;

    if (!mOckBuffer->IsCompress()) { // record write bytes when don't need compress
        mTotalWriteBytes += header->length;
    }
    if (UNLIKELY(partitionId > mPartitionLengths.size())) {
        LOG_ERROR("Illegal partitionId %d", partitionId);
        return false;
    }
    mPartitionLengths[partitionId] += header->length; // we can't get real length when compress

    address += vbHeaderSize; // 8 means header length so skip

    // remove pt view vector in vector batch when multiply partition
    int colIndex = mIsSinglePt ? 0 : 1;
    // for example: vector with 4 column, when single colIndex is col [0, 4), as multi partition colIndex is (0, 5)
    for (; colIndex < vb.GetVectorCount(); colIndex++) {
        if (UNLIKELY(!WriteOneVector(vb, colIndex, vbRegion->mRowIndexes, vbRegion->mRowNum, &address))) {
            LOG_ERROR("Failed to write vector with index %d in current vector batch", colIndex);
            return false;
        }
    }

    // reset vector batch region info
    ResetCacheRegion(partitionId);
    return true;
}

bool OckSplitter::FlushAllRegionAndGetNewBlob(VectorBatch &vb)
{
    if (UNLIKELY(mPartitionNum > mCacheRegion.size())) {
        LOG_ERROR("Illegal mPartitionNum  %d", mPartitionNum);
        return false;
    }
    for (uint32_t partitionId = 0; partitionId < mPartitionNum; ++partitionId) {
        if (mCacheRegion[partitionId].mRowNum == 0) {
            continue;
        }

        if (!WritePartVectorBatch(vb, partitionId)) {
            return false;
        }
    }

    ResetCacheRegion();

    uint32_t dataSize = 0;
    if (UNLIKELY(!mOckBuffer->Flush(false, dataSize))) {
        LogError("Failed to flush local blob.");
        return false;
    }

    if (mOckBuffer->IsCompress()) {
        mTotalWriteBytes += dataSize; // get compressed size from ock shuffle sdk
    }

    if (UNLIKELY(!mOckBuffer->GetNewBuffer())) {
        LogError("Failed to get new local blob.");
        return false;
    }

    return true;
}

/**
 * preoccupied one row data space in ock local buffer
 * @param partitionId
 * @param length
 * @return
 */
bool OckSplitter::PreoccupiedBufferSpace(VectorBatch &vb, uint32_t partitionId, uint32_t rowIndex, uint32_t rowLength,
    bool newRegion)
{
    if (UNLIKELY(partitionId > mCacheRegion.size())) {
        LOG_ERROR("Illegal partitionId  %d", partitionId);
        return false;
    }
    uint32_t preoccupiedSize = rowLength;
    if (mCacheRegion[partitionId].mRowNum == 0) {
        preoccupiedSize += mMinDataLenInVB; // means create a new vector batch, so will cost header
    }

    switch (mOckBuffer->PreoccupiedDataSpace(partitionId, preoccupiedSize, newRegion)) {
        case OckHashWriteBuffer::ResultFlag::ENOUGH: {
            UpdateCacheRegion(partitionId, rowIndex, preoccupiedSize);
            break;
        }
        case OckHashWriteBuffer::ResultFlag::NEW_REGION: {
            // write preoccupied region data to local blob when it exist
            if (UNLIKELY(!WritePartVectorBatch(vb, partitionId))) {
                LOG_ERROR("Failed to write part vector batch or get new region in local blob");
                return false;
            }

            // try to preoccupied new region in this local blob for this row
            return PreoccupiedBufferSpace(vb, partitionId, rowIndex, rowLength, true);
        }
        case OckHashWriteBuffer::ResultFlag::LACK: {
            // flush all partition preoccupied region data to local blob when it exist
            if (UNLIKELY(!FlushAllRegionAndGetNewBlob(vb))) {
                LOG_ERROR("Failed to write part vector batch or get new local blob");
                return false;
            }

            // try preoccupied new region in new local blob for this row
            return PreoccupiedBufferSpace(vb, partitionId, rowIndex, rowLength, false);
        }
        default: {
            LogError("Unexpected error happen.");
            return false;
        }
    }

    return true;
}

/**
 *
 * @param vb
 * @return
 */
bool OckSplitter::Split(VectorBatch &vb)
{
    LOG_TRACE("Split vb row number: %d ", vb.GetRowCount());

    ResetCacheRegion(); // clear the record about those partition regions in old vector batch
    mCurrentVB = &vb;   // point to current native vector batch address
    // the first vector in vector batch that record partitionId about same index row when exist multiple partition
    mPtViewInCurVB = mIsSinglePt ? nullptr : reinterpret_cast<Vector<int32_t> *>(vb.Get(0));

    //    PROFILE_START_L1(PREOCCUPIED_STAGE)
    for (int rowIndex = 0; rowIndex < vb.GetRowCount(); ++rowIndex) {
        uint32_t partitionId = GetPartitionIdOfRow(rowIndex);

        // calculate row length in the vb
        uint32_t oneRowLength = GetRowLengthInBytes(vb, rowIndex);
        if (!PreoccupiedBufferSpace(vb, partitionId, rowIndex, oneRowLength, false)) {
            LOG_ERROR("Failed to preoccupied local buffer space for row index %d", rowIndex);
            return false;
        }
    }

    // write all partition region data that already preoccupied to local blob
    for (uint32_t partitionId = 0; partitionId < mPartitionNum; ++partitionId) {
        if (mCacheRegion[partitionId].mRowNum == 0) {
            continue;
        }

        if (!WritePartVectorBatch(vb, partitionId)) {
            LOG_ERROR("Failed to write rows in partitionId %d in the vector batch to local blob", partitionId);
            return false;
        }
    }

    // release data belong to the vector batch in memory after write it to local blob
    vb.FreeAllVectors();
    // PROFILE_END_L1(RELEASE_VECTOR)
    mCurrentVB = nullptr;

    return true;
}

void OckSplitter::Stop()
{
    uint32_t dataSize = 0;
    if (UNLIKELY(!mOckBuffer->Flush(true, dataSize))) {
        LogError("Failed to flush local blob when stop.");
        return;
    }

    if (mOckBuffer->IsCompress()) {
        mTotalWriteBytes += dataSize;
    }

    LOG_INFO("Time cost preoccupied: %lu write_data: %lu release_resource: %lu", mPreoccupiedTime, mWriteVBTime,
             mReleaseResource);
}