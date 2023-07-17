/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "ock_merge_reader.h"

#include <sstream>

#include "common/common.h"

using namespace omniruntime::vec;
using namespace ock::dopspark;

bool OckMergeReader::Initialize(const int32_t *typeIds, uint32_t colNum)
{
    mColNum = colNum;
    mVectorBatch = std::make_shared<VBDataDesc>();
    if (UNLIKELY(mVectorBatch == nullptr)) {
        LOG_ERROR("Failed to new instance for vector batch description");
        return false;
    }

    if (UNLIKELY(!mVectorBatch->Initialize(colNum))) {
        LOG_ERROR("Failed to initialize vector batch.");
        return false;
    }

    mColTypeIds.reserve(colNum);
    for (uint32_t index = 0; index < colNum; ++index) {
        mColTypeIds.emplace_back(typeIds[index]);
    }

    return true;
}

bool OckMergeReader::GenerateVector(OckVectorPtr &vector, uint32_t rowNum, int32_t typeId, uint8_t *&startAddress)
{
    uint8_t *address = startAddress;
    vector->SetValueNulls(static_cast<void *>(address));
    vector->SetSize(rowNum);
    address += rowNum;

    switch (typeId) {
        case OMNI_BOOLEAN: {
            vector->SetCapacityInBytes(sizeof(uint8_t) * rowNum);
            break;
        }
        case OMNI_SHORT: {
            vector->SetCapacityInBytes(sizeof(uint16_t) * rowNum);
            break;
        }
        case OMNI_INT:
        case OMNI_DATE32: {
            vector->SetCapacityInBytes(sizeof(uint32_t) * rowNum);
            break;
        }
        case OMNI_LONG:
        case OMNI_DOUBLE:
        case OMNI_DECIMAL64:
        case OMNI_DATE64: {
            vector->SetCapacityInBytes(sizeof(uint64_t) * rowNum);
            break;
        }
        case OMNI_DECIMAL128: {
            vector->SetCapacityInBytes(decimal128Size * rowNum); // 16 means value cost 16Byte
            break;
        }
        case OMNI_CHAR:
        case OMNI_VARCHAR: { // unknown length for value vector, calculate later
            // will add offset_vector_len when the length of values_vector is variable
            vector->SetValueOffsets(static_cast<void *>(address));
            address += capacityOffset * (rowNum + 1); // 4 means value cost 4Byte
            vector->SetCapacityInBytes(*reinterpret_cast<int32_t *>(address - capacityOffset));
            if (UNLIKELY(vector->GetCapacityInBytes() > maxCapacityInBytes)) {
                LOG_ERROR("vector capacityInBytes exceed maxCapacityInBytes");
                return false;
            }
            break;
        }
        default: {
            LOG_ERROR("Unsupported data type id %d", typeId);
            return false;
        }
    }

    vector->SetValues(static_cast<void *>(address));
    address += vector->GetCapacityInBytes();
    startAddress = address;
    return true;
}

bool OckMergeReader::CalVectorValueLength(uint32_t colIndex, uint32_t &length)
{
    auto vector = mVectorBatch->GetColumnHead(colIndex);
    length = 0;
    for (uint32_t cnt = 0; cnt < mMergeCnt; ++cnt) {
        if (UNLIKELY(vector == nullptr)) {
            LOG_ERROR("Failed to calculate value length for column index %d", colIndex);
            return false;
        }
        length += vector->GetCapacityInBytes();
        vector = vector->GetNextVector();
    }

    mVectorBatch->SetColumnCapacity(colIndex, length);
    return true;
}

bool OckMergeReader::ScanOneVectorBatch(uint8_t *&startAddress)
{
    uint8_t *address = startAddress;
    // get vector batch msg as vb_data_batch memory layout (upper)
    auto curVBHeader = reinterpret_cast<VBHeaderPtr>(address);
    mVectorBatch->AddTotalCapacity(curVBHeader->length);
    mVectorBatch->AddTotalRowNum(curVBHeader->rowNum);
    address += sizeof(struct VBDataHeaderDesc);

    OckVector *curVector = nullptr;
    for (uint32_t colIndex = 0; colIndex < mColNum; colIndex++) {
        auto curVector = mVectorBatch->GetCurColumn(colIndex);
        if (UNLIKELY(curVector == nullptr)) {
            LOG_ERROR("curVector is null, index %d", colIndex);
            return false;
        }
        if (UNLIKELY(!GenerateVector(curVector, curVBHeader->rowNum, mColTypeIds[colIndex], address))) {
            LOG_ERROR("Failed to generate vector");
            return false;
        }
    }

    if (UNLIKELY((uint32_t)(address - startAddress) != curVBHeader->length)) {
        LOG_ERROR("Failed to scan one vector batch as invalid date setting %d vs %d",
                  (uint32_t)(address - startAddress), curVBHeader->length);
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
        if (mVectorBatch->GetTotalRowNum() >= maxRowNum || mVectorBatch->GetTotalCapacity() >= maxSize) {
            break;
        }
    }

    startAddress = address;
    return true;
}

bool OckMergeReader::CopyPartDataToVector(uint8_t *&nulls, uint8_t *&values, uint32_t &remainingSize, 
    uint32_t &remainingCapacity, OckVectorPtr &srcVector)
{
    uint32_t srcSize = srcVector->GetSize();
    if (UNLIKELY(remainingSize < srcSize)) {
        LOG_ERROR("Not eneough resource. remainingSize %d, srcSize %d.", remainingSize, srcSize);
        return false;
    }
    errno_t ret = memcpy_s(nulls, remainingSize, srcVector->GetValueNulls(), srcSize);
    if (UNLIKELY(ret != EOK)) {
        LOG_ERROR("Failed to copy null vector");
        return false;
    }
    nulls += srcSize;
    remainingSize -= srcSize;

    uint32_t srcCapacity = srcVector->GetCapacityInBytes();
    if (UNLIKELY(remainingCapacity < srcCapacity)) {
        LOG_ERROR("Not enough resource. remainingCapacity %d, srcCapacity %d", remainingCapacity, srcCapacity);
        return false;
    }
    if (srcCapacity > 0) {
        ret = memcpy_s(values, remainingCapacity, srcVector->GetValues(), srcCapacity);
        if (UNLIKELY(ret != EOK)) {
            LOG_ERROR("Failed to copy values vector");
            return false;
        }
        values += srcCapacity;
        remainingCapacity -=srcCapacity;
    }

    return true;
}

bool OckMergeReader::CopyDataToVector(BaseVector *dstVector, uint32_t colIndex)
{
    // point to first src vector in list
    auto srcVector = mVectorBatch->GetColumnHead(colIndex);

    auto *nullsAddress = (uint8_t *)omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(dstVector);
    auto *valuesAddress = (uint8_t *)VectorHelper::UnsafeGetValues(dstVector, mColTypeIds[colIndex]);
    uint32_t *offsetsAddress = (uint32_t *)VectorHelper::UnsafeGetOffsetsAddr(dstVector, mColTypeIds[colIndex]);
    dstVector->SetNullFlag(true);
    uint32_t totalSize = 0;
    uint32_t currentSize = 0;
    if (dstVector->GetSize() < 0) {
        LOG_ERROR("Invalid vector size %d", dstVector->GetSize());
        return false;
    }
    uint32_t remainingSize = (uint32_t)dstVector->GetSize();
    uint32_t remainingCapacity = 0;
    if (mColTypeIds[colIndex] == OMNI_CHAR || mColTypeIds[colIndex] == OMNI_VARCHAR) {
        auto *varCharVector = reinterpret_cast<Vector<LargeStringContainer<std::string_view>> *>(dstVector);
        remainingCapacity = omniruntime::vec::unsafe::UnsafeStringVector::GetContainer(varCharVector)->GetCapacityInBytes();
    } else {
        remainingCapacity = GetDataSize(colIndex) *remainingSize;
    }

    for (uint32_t cnt = 0; cnt < mMergeCnt; ++cnt) {
        if (UNLIKELY(srcVector == nullptr)) {
            LOG_ERROR("Invalid src vector");
            return false;
        }

        if (UNLIKELY(!CopyPartDataToVector(nullsAddress, valuesAddress, remainingSize, remainingCapacity, srcVector))) {
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
        if (UNLIKELY(totalSize != mVectorBatch->GetColumnCapacity(colIndex))) {
            LOG_ERROR("Failed to calculate variable vector value length, %d to %d", totalSize,
                      mVectorBatch->GetColumnCapacity(colIndex));
            return false;
        }
    }

    return true;
}