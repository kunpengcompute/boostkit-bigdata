/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ock_merge_reader.h"

#include <sstream>

#include "common/common.h"

using namespace omniruntime::type;
using namespace omniruntime::vec;
using namespace ock::dopspark;

bool OckMergeReader::Initialize(const int32_t *typeIds, uint32_t colNum)
{
    mColNum = colNum;
    mVectorBatch = new (std::nothrow) VBDataDesc(colNum);
    if (UNLIKELY(mVectorBatch == nullptr)) {
        LOG_ERROR("Failed to new instance for vector batch description");
        return false;
    }

    mColTypeIds.reserve(colNum);
    for (uint32_t index = 0; index < colNum; ++index) {
        mColTypeIds.emplace_back(typeIds[index]);
    }

    return true;
}

bool OckMergeReader::GenerateVector(OckVector &vector, uint32_t rowNum, int32_t typeId, uint8_t *&startAddress)
{
    uint8_t *address = startAddress;
    vector.SetValueNulls(static_cast<void *>(address));
    vector.SetSize(rowNum);
    address += rowNum;

    switch (typeId) {
        case OMNI_BOOLEAN: {
            vector.SetCapacityInBytes(sizeof(uint8_t) * rowNum);
            break;
        }
        case OMNI_SHORT: {
            vector.SetCapacityInBytes(sizeof(uint16_t) * rowNum);
            break;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            vector.SetCapacityInBytes(sizeof(uint32_t) * rowNum);
            break;
        }
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_DECIMAL64:
        case OMNI_DATE64: {
            vector.SetCapacityInBytes(sizeof(uint64_t) * rowNum);
            break;
        }
        case OMNI_DECIMAL128: {
            vector.SetCapacityInBytes(decimal128Size * rowNum); // 16 means value cost 16Byte
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: { // unknown length for value vector, calculate later
            // will add offset_vector_len when the length of values_vector is variable
            vector.SetValueOffsets(static_cast<void *>(address));
            address += capacityOffset * (rowNum + 1); // 4 means value cost 4Byte
            vector.SetCapacityInBytes(*reinterpret_cast<int32_t *>(address - capacityOffset));
            break;
        }
        default: {
            LOG_ERROR("Unsupported data type id %d", typeId);
            return false;
        }
    }

    vector.SetValues(static_cast<void *>(address));
    address += vector.GetCapacityInBytes();
    startAddress = address;
    return true;
}

bool OckMergeReader::CalVectorValueLength(uint32_t colIndex, uint32_t &length)
{
    OckVector *vector = mVectorBatch->mColumnsHead[colIndex];
    for (uint32_t cnt = 0; cnt < mMergeCnt; ++cnt) {
        if (UNLIKELY(vector == nullptr)) {
            LOG_ERROR("Failed to calculate value length for column index %d", colIndex);
            return false;
        }

        mVectorBatch->mVectorValueLength[colIndex] += vector->GetCapacityInBytes();
        vector = vector->GetNextVector();
    }

    length = mVectorBatch->mVectorValueLength[colIndex];
    return true;
}

bool OckMergeReader::ScanOneVectorBatch(uint8_t *&startAddress)
{
    uint8_t *address = startAddress;
    // get vector batch msg as vb_data_batch memory layout (upper)
    mCurVBHeader = reinterpret_cast<VBHeaderPtr>(address);
    mVectorBatch->mHeader.rowNum += mCurVBHeader->rowNum;
    mVectorBatch->mHeader.length += mCurVBHeader->length;
    address += sizeof(struct VBDataHeaderDesc);

    OckVector *curVector = nullptr;
    for (uint32_t colIndex = 0; colIndex < mColNum; colIndex++) {
        curVector = mVectorBatch->mColumnsCur[colIndex];
        if (UNLIKELY(!GenerateVector(*curVector, mCurVBHeader->rowNum, mColTypeIds[colIndex], address))) {
            LOG_ERROR("Failed to generate vector");
            return false;
        }

        if (curVector->GetNextVector() == nullptr) {
            curVector = new (std::nothrow) OckVector();
            if (UNLIKELY(curVector == nullptr)) {
                LOG_ERROR("Failed to new instance for ock vector");
                return false;
            }

            // set next vector in the column merge list, and current column vector point to it
            mVectorBatch->mColumnsCur[colIndex]->SetNextVector(curVector);
            mVectorBatch->mColumnsCur[colIndex] = curVector;
        } else {
            mVectorBatch->mColumnsCur[colIndex] = curVector->GetNextVector();
        }
    }

    if (UNLIKELY((uint32_t)(address - startAddress) != mCurVBHeader->length)) {
        LOG_ERROR("Failed to scan one vector batch as invalid date setting %d vs %d",
                  (uint32_t)(address - startAddress), mCurVBHeader->length);
        return false;
    }

    startAddress = address;
    return true;
}

bool OckMergeReader::GetMergeVectorBatch(uint8_t *&startAddress, uint32_t remain, uint32_t maxRowNum, uint32_t maxSize)
{
    mVectorBatch->Reset(); // clean data struct for vector batch
    mMergeCnt = 0;

    uint8_t *address = startAddress;
    if (UNLIKELY(address == nullptr)) {
        LOG_ERROR("Invalid address as nullptr");
        return false;
    }

    auto *endAddress = address + remain;
    for (; address < endAddress;) {
        if (UNLIKELY(!ScanOneVectorBatch(address))) {
            LOG_ERROR("Failed to scan one vector batch data");
            return false;
        }

        mMergeCnt++;
        if (mVectorBatch->mHeader.rowNum >= maxRowNum || mVectorBatch->mHeader.length >= maxSize) {
            break;
        }
    }

    startAddress = address;

    return true;
}

bool OckMergeReader::CopyPartDataToVector(uint8_t *&nulls, uint8_t *&values,
    OckVector &srcVector, uint32_t colIndex)
{
    errno_t ret = memcpy_s(nulls, srcVector.GetSize(), srcVector.GetValueNulls(), srcVector.GetSize());
    if (UNLIKELY(ret != EOK)) {
        LOG_ERROR("Failed to copy null vector");
        return false;
    }
    nulls += srcVector.GetSize();

    if (srcVector.GetCapacityInBytes() > 0) {
        ret = memcpy_s(values, srcVector.GetCapacityInBytes(), srcVector.GetValues(),
            srcVector.GetCapacityInBytes());
        if (UNLIKELY(ret != EOK)) {
            LOG_ERROR("Failed to copy values vector");
            return false;
        }
        values += srcVector.GetCapacityInBytes();
    }

    return true;
}

bool OckMergeReader::CopyDataToVector(Vector *dstVector, uint32_t colIndex)
{
    // point to first src vector in list
    OckVector *srcVector = mVectorBatch->mColumnsHead[colIndex];

    auto *nullsAddress = (uint8_t *)dstVector->GetValueNulls();
    auto *valuesAddress = (uint8_t *)dstVector->GetValues();
    uint32_t *offsetsAddress = (uint32_t *)dstVector->GetValueOffsets();
    dstVector->SetValueNulls(true);
    uint32_t totalSize = 0;
    uint32_t currentSize = 0;

    for (uint32_t cnt = 0; cnt < mMergeCnt; ++cnt) {
        if (UNLIKELY(srcVector == nullptr)) {
            LOG_ERROR("Invalid src vector");
            return false;
        }

        if (UNLIKELY(!CopyPartDataToVector(nullsAddress, valuesAddress, *srcVector, colIndex))) {
            return false;
        }

        if (mColTypeIds[colIndex] == OMNI_CHAR || mColTypeIds[colIndex] == OMNI_VARCHAR) {
            for (uint32_t rowIndex = 0; rowIndex < srcVector->GetSize(); ++rowIndex, ++offsetsAddress) {
                currentSize = ((uint32_t *)srcVector->GetValueOffsets())[rowIndex];
                *offsetsAddress = totalSize;
                totalSize += currentSize;
            }
        }

        srcVector = srcVector->GetNextVector();
    }

    if (mColTypeIds[colIndex] == OMNI_CHAR || mColTypeIds[colIndex] == OMNI_VARCHAR) {
        *offsetsAddress = totalSize;
        if (UNLIKELY(totalSize != mVectorBatch->mVectorValueLength[colIndex])) {
            LOG_ERROR("Failed to calculate variable vector value length, %d to %d", totalSize,
                      mVectorBatch->mVectorValueLength[colIndex]);
            return false;
        }
    }

    return true;
}